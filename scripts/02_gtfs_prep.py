"""Prepare per-month static tables from that month's archived GTFS feed.

Outputs in data_work/<YYYY-MM>/ :
  stations.parquet  parent_station, station_name, lat, lon
  segments.parquet  route_id, direction_id, from_parent, to_parent, dist_mi, straight_mi, n_patterns, geom (JSON [[lat,lon],..] along the shape)
  sched.parquet     trip_id, route_id, direction_id, from_parent, to_parent, sched_dep, sched_arr (sec)
  terminals.csv     route_id, parent_station, share_of_trips   (stations that start/end >=1% of trips)

Distance method: GTFS shape_dist_traveled is empty in this feed, so for each (shape, ordered
stop list) pattern we snap each platform stop to the nearest shape vertex (monotonically along
the shape) and measure haversine distance along the polyline between consecutive snapped stops.
Usage: python scripts/02_gtfs_prep.py 2026-01
"""
import sys, json
import numpy as np, pandas as pd
from common import GTFS_ARCHIVE, WORK, ROUTES_SQL, gtfs_reader, connect

ym = sys.argv[1]
feed = GTFS_ARCHIVE / ym
out = WORK / ym; out.mkdir(parents=True, exist_ok=True)
con = connect()
rd = lambda n: gtfs_reader(feed, n)

# --- stations (parents) and platform -> parent map
con.execute(f"""CREATE TABLE stops AS SELECT stop_id, stop_name, location_type,
    try_cast(stop_lat AS DOUBLE) lat, try_cast(stop_lon AS DOUBLE) lon,
    CASE WHEN location_type='1' THEN stop_id ELSE parent_station END AS parent FROM {rd('stops')}""")
con.execute("""COPY (SELECT stop_id AS parent_station, stop_name AS station_name, lat, lon FROM stops WHERE location_type='1')
               TO '%s/stations.parquet'""" % out)

# --- subway trips + stop_times (ordered)
con.execute(f"""CREATE TABLE trips AS SELECT trip_id, route_id, try_cast(direction_id AS INT) direction_id, shape_id
                FROM {rd('trips')} WHERE route_id IN ({ROUTES_SQL})""")
con.execute(f"""CREATE TABLE st AS
  SELECT t.trip_id, t.route_id, t.direction_id, t.shape_id, s.stop_id, p.parent, p.lat, p.lon,
         try_cast(s.stop_sequence AS INT) seq,
         CAST(split_part(s.arrival_time,':',1) AS INT)*3600 + CAST(split_part(s.arrival_time,':',2) AS INT)*60 + CAST(split_part(s.arrival_time,':',3) AS INT) arr,
         CAST(split_part(s.departure_time,':',1) AS INT)*3600 + CAST(split_part(s.departure_time,':',2) AS INT)*60 + CAST(split_part(s.departure_time,':',3) AS INT) dep
  FROM {rd('stop_times')} s JOIN trips t USING(trip_id) JOIN stops p ON p.stop_id = s.stop_id""")

# --- scheduled consecutive-stop pairs per trip
con.execute("""CREATE TABLE sched AS
  SELECT trip_id, route_id, direction_id, parent AS from_parent,
         lead(parent) OVER w AS to_parent, dep AS sched_dep, lead(arr) OVER w AS sched_arr
  FROM st WINDOW w AS (PARTITION BY trip_id ORDER BY seq)""")
con.execute(f"COPY (SELECT * FROM sched WHERE to_parent IS NOT NULL AND to_parent<>from_parent) TO '{out}/sched.parquet'")

# --- terminals: stations that are first/last stop for >=1% of a route's trips
con.execute(f"""COPY (
  WITH ends AS (SELECT trip_id, route_id, arg_min(parent,seq) o, arg_max(parent,seq) d FROM st GROUP BY ALL),
       n AS (SELECT route_id, count(*) tot FROM ends GROUP BY 1),
       u AS (SELECT route_id, o AS parent_station FROM ends UNION ALL SELECT route_id, d FROM ends)
  SELECT route_id, parent_station, count(*)/ (2.0*any_value(tot)) share_of_trips FROM u JOIN n USING(route_id)
  GROUP BY route_id, parent_station HAVING count(*)/(2.0*any_value(tot)) >= 0.01 ORDER BY 1,3 DESC) TO '{out}/terminals.csv' (HEADER)""")

# --- segment distances from shapes
patterns = con.execute("""SELECT t.route_id, t.direction_id, t.shape_id, list(st.parent ORDER BY st.seq) parents,
    list(st.lat ORDER BY st.seq) lats, list(st.lon ORDER BY st.seq) lons, count(DISTINCT t.trip_id) n
    FROM trips t JOIN st USING(trip_id) GROUP BY t.route_id, t.direction_id, t.shape_id, st.trip_id""").df()
patterns["key"] = patterns.parents.map(lambda x: tuple(x))
patterns = patterns.drop_duplicates(["route_id", "direction_id", "shape_id", "key"])
shapes = con.execute(f"""SELECT shape_id, try_cast(shape_pt_lat AS DOUBLE) lat, try_cast(shape_pt_lon AS DOUBLE) lon,
    try_cast(shape_pt_sequence AS INT) sq FROM {gtfs_reader(feed,'shapes')}
    WHERE shape_id IN (SELECT DISTINCT shape_id FROM trips) ORDER BY shape_id, sq""").df()
shp = {k: (g.lat.values, g.lon.values) for k, g in shapes.groupby("shape_id")}

def hav(lat1, lon1, lat2, lon2):  # miles
    p = np.pi / 180
    a = np.sin((lat2-lat1)*p/2)**2 + np.cos(lat1*p)*np.cos(lat2*p)*np.sin((lon2-lon1)*p/2)**2
    return 2 * 3958.8 * np.arcsin(np.sqrt(a))

rows = []
for r in patterns.itertuples():
    la, lo = shp[r.shape_id]
    cum = np.concatenate([[0], np.cumsum(hav(la[:-1], lo[:-1], la[1:], lo[1:]))])
    idx, prev = [], 0
    for y, x in zip(r.lats, r.lons):
        d = hav(y, x, la[prev:], lo[prev:])
        prev = prev + int(np.argmin(d)); idx.append(prev)
    for i in range(len(r.parents)-1):
        a, b = r.parents[i], r.parents[i+1]
        if a == b: continue
        geom = json.dumps([[round(float(y), 5), round(float(x), 5)] for y, x in zip(la[idx[i]:idx[i+1]+1], lo[idx[i]:idx[i+1]+1])])
        rows.append((r.route_id, r.direction_id, a, b, cum[idx[i+1]] - cum[idx[i]],
                     float(hav(r.lats[i], r.lons[i], r.lats[i+1], r.lons[i+1])), geom))
seg = pd.DataFrame(rows, columns=["route_id","direction_id","from_parent","to_parent","dist_mi","straight_mi","geom"])
seg = seg.groupby(["route_id","direction_id","from_parent","to_parent"], as_index=False).agg(
    dist_mi=("dist_mi","median"), straight_mi=("straight_mi","median"), n_patterns=("dist_mi","size"), geom=("geom","first"),
    dist_min=("dist_mi","min"), dist_max=("dist_mi","max"))
# Nearest-vertex snapping can make a path slightly shorter than the straight line between
# platforms (impossible), so floor at the straight-line distance.
seg["dist_mi"] = np.maximum(seg.dist_mi, seg.straight_mi)
seg.to_parquet(out / "segments.parquet")
bad = seg[seg.dist_mi > seg.straight_mi*2.0 + 0.05]
print(f"{ym}: {len(seg)} segments, {len(bad)} with suspicious shape distance; "
      f"total route-miles (sum of one dir) ~ {seg[seg.direction_id==0].dist_mi.sum():.1f}")
if len(bad): print(bad.head(10).to_string(index=False))
