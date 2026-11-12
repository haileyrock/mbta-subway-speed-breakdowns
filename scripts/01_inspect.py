"""Step 1: profile the MBTA events + GTFS data. Read-only; prints a report."""
import duckdb, sys
ROOT = sys.argv[1] if len(sys.argv) > 1 else "."
ev = f"{ROOT}/Events_2026/*.csv"
g = f"{ROOT}/MBTA_GTFS"
con = duckdb.connect()
con.execute(f"""CREATE VIEW ev AS SELECT service_date::DATE service_date, route_id, trunk_route_id, trip_id, direction_id::INT direction_id,
  direction, parent_station, stop_id, stop_name, try_cast(sync_stop_sequence AS INT) sync_stop_sequence, vehicle_id, vehicle_label, event_type,
  try_cast(event_time AS TIMESTAMP) event_time, try_cast(event_time_sec AS INT) event_time_sec,
  regexp_extract(filename,'(\\d{{4}}-\\d{{2}})_',1) AS ym, regexp_extract(filename,'_(HR|LR)Events',1) AS mode
  FROM read_csv('{ev}', filename=true, header=true, all_varchar=true, nullstr=['NA',''])""")
def q(title, sql, n=60):
    print(f"\n=== {title}"); print(con.execute(sql).df().head(n).to_string(index=False))
q("rows per file", "SELECT ym, mode, count(*) n, min(service_date) d0, max(service_date) d1 FROM ev GROUP BY ALL ORDER BY ALL")
q("routes", "SELECT mode, route_id, trunk_route_id, count(*) n FROM ev GROUP BY ALL ORDER BY ALL")
q("event types", "SELECT event_type, count(*) n FROM ev GROUP BY ALL")
q("nulls", """SELECT count(*) n, count_if(parent_station IS NULL) null_parent, count_if(stop_id IS NULL) null_stop,
  count_if(vehicle_id IS NULL) null_veh, count_if(event_time IS NULL) null_time, count_if(sync_stop_sequence IS NULL) null_seq,
  count_if(trip_id LIKE 'ADDED%') added_trips, count_if(trip_id LIKE 'NONREV%' OR trip_id LIKE 'CANCEL%') other_nonstd FROM ev""")
q("dup events", "SELECT count(*) dup_groups, sum(c-1) extra_rows FROM (SELECT count(*) c FROM ev GROUP BY service_date,trip_id,stop_id,event_type,vehicle_id HAVING count(*)>1)")
q("days per month with events (gaps)", "SELECT ym, count(DISTINCT service_date) AS n_days FROM ev GROUP BY ALL ORDER BY ym")
q("missing dates 2026-01-01..max", """WITH d AS (SELECT unnest(generate_series(DATE '2026-01-01',(SELECT max(service_date) FROM ev),INTERVAL 1 DAY))::DATE dt)
  SELECT dt FROM d WHERE dt NOT IN (SELECT DISTINCT service_date FROM ev)""")
q("event_time_sec range", "SELECT min(event_time_sec), max(event_time_sec), count_if(event_time_sec>86400) over_24h FROM ev")
q("unmatched ARR/DEP pairing per month (HR sample)", """SELECT event_type, count(*) FROM ev WHERE ym='2026-01' GROUP BY ALL""")
q("stop_ids not in GTFS stops", f"""SELECT count(DISTINCT stop_id) n_missing FROM ev WHERE stop_id NOT IN (SELECT stop_id FROM read_csv('{g}/stops.txt', all_varchar=true))""")
q("parent_station vs GTFS parent mismatch", f"""SELECT count(*) mism FROM (SELECT DISTINCT e.stop_id, e.parent_station p1, s.parent_station p2 FROM ev e JOIN read_csv('{g}/stops.txt', all_varchar=true) s USING(stop_id) WHERE e.parent_station IS DISTINCT FROM s.parent_station)""")
q("station count by route", "SELECT route_id, count(DISTINCT parent_station) stations, count(DISTINCT stop_id) stops FROM ev GROUP BY ALL ORDER BY 1")
q("trips per route per direction", "SELECT route_id, direction_id, count(DISTINCT trip_id) trips FROM ev WHERE ym='2026-01' GROUP BY ALL ORDER BY ALL")
q("sample trip (Red, Jan)", """SELECT stop_name, stop_id, sync_stop_sequence seq, event_type, event_time_sec FROM ev
  WHERE trip_id=(SELECT trip_id FROM ev WHERE ym='2026-01' AND route_id='Red' AND trip_id NOT LIKE 'ADDED%' LIMIT 1) ORDER BY event_time_sec, sync_stop_sequence""", 80)
q("dwell distribution (Jan, same trip+stop ARR->DEP)", """WITH a AS (SELECT service_date,trip_id,stop_id,vehicle_id,event_time_sec t FROM ev WHERE ym='2026-01' AND event_type IN('ARR')),
 d AS (SELECT service_date,trip_id,stop_id,vehicle_id,event_time_sec t FROM ev WHERE ym='2026-01' AND event_type IN('DEP'))
 SELECT count(*) n, quantile_cont(d.t-a.t,[0.5,0.9,0.99,0.999]) q, count_if(d.t-a.t<0) neg, count_if(d.t-a.t>=240) ge4, count_if(d.t-a.t>=300) ge5, count_if(d.t-a.t>=600) ge10
 FROM a JOIN d USING(service_date,trip_id,stop_id,vehicle_id)""")
# GTFS
for f in ["routes","trips","stops","shapes","stop_times","calendar_dates"]:
    q(f"GTFS {f} columns/rows", f"SELECT count(*) n FROM read_csv('{g}/{f}.txt', all_varchar=true)")
q("GTFS shapes columns", f"DESCRIBE SELECT * FROM read_csv('{g}/shapes.txt')")
q("GTFS rapid transit routes", f"SELECT route_id, route_type, route_long_name FROM read_csv('{g}/routes.txt', all_varchar=true) WHERE route_type IN ('0','1') ORDER BY 1")
q("shape_dist_traveled populated?", f"SELECT count(*) n, count(shape_dist_traveled) with_dist FROM read_csv('{g}/shapes.txt', all_varchar=true)")
q("stop_times shape_dist populated (subway)", f"""SELECT count(*) n, count(shape_dist_traveled) with_dist FROM read_csv('{g}/stop_times.txt', all_varchar=true) st
  JOIN read_csv('{g}/trips.txt', all_varchar=true) t USING(trip_id) WHERE t.route_id IN ('Red','Orange','Blue','Green-B','Green-C','Green-D','Green-E','Mattapan')""")
