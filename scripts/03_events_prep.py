"""Clean one month of events and turn them into dwell + between-stop movements.

Cleaning rules (counts go to data_work/<ym>/quality.json):
  - subway routes only (Mattapan dropped), NONREV trips dropped
  - rows with NA event_time / event_time_sec dropped (heavy-rail files contain literal 'NA')
  - duplicates removed on (service_date, trip_id, stop_id, event_type, vehicle_id); when the same
    physical event is logged under two Green branches, keep the row whose route matches GTFS trips.txt
  - timestamps: 'event_time' is labelled Z but is local clock time; use event_time_sec
    (seconds since local midnight of service_date, may exceed 86400 after midnight).
Per (service_date, vehicle_id, trip_id), events are ordered by time (ARR before DEP on ties):
  dwell   = ARR then DEP at the same platform stop
  segment = DEP at stop A then ARR at the next stop B  (running time, excludes dwell)
Outputs in data_work/<ym>/: dwell.parquet, segs.parquet, station_day.parquet, quality.json
Usage: python scripts/03_events_prep.py 2026-01
"""
import sys, json
from common import EVENTS, GTFS_ARCHIVE, WORK, ROUTES_SQL, gtfs_reader, connect

ym = sys.argv[1]
out = WORK / ym; out.mkdir(parents=True, exist_ok=True)
con = connect()
files = [str(p) for p in sorted(EVENTS.glob(f"{ym}_*.csv"))]
q = {}

con.execute(f"""CREATE TABLE raw AS SELECT service_date::DATE service_date, route_id, trip_id, try_cast(direction_id AS INT) direction_id,
   parent_station, stop_id, vehicle_id, event_type, try_cast(event_time_sec AS INT) sec
   FROM read_csv({files}, all_varchar=true, header=true, nullstr=['NA',''])""")
q["raw_rows"] = con.execute("SELECT count(*) FROM raw").fetchone()[0]
q["mattapan_rows"] = con.execute("SELECT count(*) FROM raw WHERE route_id='Mattapan'").fetchone()[0]
q["nonrev_rows"] = con.execute(f"SELECT count(*) FROM raw WHERE trip_id LIKE 'NONREV%' AND route_id IN ({ROUTES_SQL})").fetchone()[0]
con.execute(f"DELETE FROM raw WHERE route_id NOT IN ({ROUTES_SQL}) OR trip_id LIKE 'NONREV%'")
q["na_time_rows"] = con.execute("SELECT count(*) FROM raw WHERE sec IS NULL").fetchone()[0]
con.execute("DELETE FROM raw WHERE sec IS NULL")

feed = GTFS_ARCHIVE / ym
con.execute(f"CREATE TABLE gtrips AS SELECT DISTINCT trip_id, route_id FROM {gtfs_reader(feed,'trips')}")
con.execute("""CREATE TABLE ev AS
  SELECT * EXCLUDE (rn, g_route) FROM (
    SELECT r.*, g.route_id g_route, g.trip_id IS NOT NULL AS in_sched,
      row_number() OVER (PARTITION BY service_date, r.trip_id, stop_id, event_type, vehicle_id
                         ORDER BY (r.route_id = g.route_id) DESC, sec) rn
    FROM raw r LEFT JOIN gtrips g ON g.trip_id = r.trip_id AND g.route_id = r.route_id) WHERE rn = 1""")
q["after_na_nonrev_rows"] = con.execute("SELECT count(*) FROM raw").fetchone()[0]
q["rows_after_dedupe"] = con.execute("SELECT count(*) FROM ev").fetchone()[0]
q["duplicate_rows_removed"] = q["after_na_nonrev_rows"] - q["rows_after_dedupe"]
q["share_rows_with_schedule_match"] = con.execute("SELECT avg(in_sched::INT) FROM ev").fetchone()[0]

# ordered sequence per vehicle+trip
con.execute("""CREATE TABLE seq AS SELECT *,
   lag(event_type) OVER w prev_type, lag(stop_id) OVER w prev_stop, lag(parent_station) OVER w prev_parent, lag(sec) OVER w prev_sec,
   first_value(parent_station) OVER (PARTITION BY service_date, vehicle_id, trip_id ORDER BY sec, event_type='DEP' ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) first_parent,
   last_value(parent_station)  OVER (PARTITION BY service_date, vehicle_id, trip_id ORDER BY sec, event_type='DEP' ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_parent
   FROM ev WINDOW w AS (PARTITION BY service_date, vehicle_id, trip_id ORDER BY sec, event_type='DEP')""")

q["arr_after_arr_missing_dep"] = con.execute("SELECT count(*) FROM seq WHERE event_type='ARR' AND prev_type='ARR'").fetchone()[0]
q["dep_after_dep_missing_arr_nonfirst"] = con.execute("SELECT count(*) FROM seq WHERE event_type='DEP' AND prev_type='DEP'").fetchone()[0]

con.execute(f"""COPY (SELECT service_date, route_id, direction_id, trip_id, vehicle_id, parent_station AS parent, stop_id,
    prev_sec AS start_sec, sec - prev_sec AS dwell_sec, (parent_station IN (first_parent, last_parent)) AS trip_end_stop, in_sched
  FROM seq WHERE event_type='DEP' AND prev_type='ARR' AND prev_stop = stop_id AND sec >= prev_sec) TO '{out}/dwell.parquet'""")
q["negative_dwell_pairs"] = con.execute("SELECT count(*) FROM seq WHERE event_type='DEP' AND prev_type='ARR' AND prev_stop=stop_id AND sec<prev_sec").fetchone()[0]

con.execute(f"""COPY (SELECT service_date, route_id, direction_id, trip_id, vehicle_id, prev_parent AS from_parent, parent_station AS to_parent,
    prev_sec AS dep_sec, sec AS arr_sec, sec - prev_sec AS run_sec, in_sched
  FROM seq WHERE event_type='ARR' AND prev_type='DEP' AND prev_parent <> parent_station AND sec > prev_sec) TO '{out}/segs.parquet'""")
q["nonpositive_run_pairs"] = con.execute("SELECT count(*) FROM seq WHERE event_type='ARR' AND prev_type='DEP' AND prev_parent<>parent_station AND sec<=prev_sec").fetchone()[0]

# station-day activity for planned-shutdown detection
con.execute(f"""COPY (SELECT route_id, parent_station AS parent, service_date, count(*) n_events FROM ev GROUP BY ALL) TO '{out}/station_day.parquet'""")

q["dwell_rows"] = con.execute(f"SELECT count(*) FROM '{out}/dwell.parquet'").fetchone()[0]
q["segment_rows"] = con.execute(f"SELECT count(*) FROM '{out}/segs.parquet'").fetchone()[0]
json.dump(q, open(out / "quality.json", "w"), indent=1)
print(ym); [print(f"  {k}: {v:,.4f}" if isinstance(v, float) else f"  {k}: {v:,}") for k, v in q.items()]
