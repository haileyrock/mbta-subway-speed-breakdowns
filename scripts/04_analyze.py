"""Breakdown incidents per station + speed tables.

Usage: python scripts/04_analyze.py <tag> <YYYY-MM> [<YYYY-MM> ...]
  e.g. python scripts/04_analyze.py jan 2026-01        (prototype)
       python scripts/04_analyze.py all 2026-01 ... 2026-10

Definitions (all parameters at top of file):
  Breakdown candidate  = (a) dwell >= N min at a non-terminal stop, or
                         (b) running time between two consecutive stops >= N min above baseline,
                             baseline = scheduled running time of that trip if it has one, else the
                             median for that (route, direction, segment, hour-of-day)  [BASELINE='sched']
                             or the median always [BASELINE='median'].
  Excluded             = habitual timed holds / recurring layovers (rules A+B, per month; holds.csv, recurring_segment_delays.csv), segments into/out of
                         a terminal (platform queueing / unreliable departure stamps), terminal layovers (trip first/last stop; stations that start/end >=1% of a
                         route's trips), stations adjacent to a planned shutdown (station-day with
                         <15% of its typical daily events), anything longer than MAX_MIN (tracking gap).
  Incident             = candidates on the same service day, line (Green branches = one line) and
                         direction, within CLUSTER_MIN minutes of each other, within the same branch
                         corridor and not more than CLUSTER_MILES behind the frontmost stop,
                         attributed to the frontmost station in the direction of travel.
                         A between-stop delay is attributed to the station ahead (to_parent).
  Speed                = segment length (GTFS shape) / running time (DEP at A -> ARR at B).
"""
import sys, json, collections
import numpy as np, pandas as pd
from common import WORK, OUT, connect

TAG, MONTHS = sys.argv[1], sys.argv[2:]
THRESH = [3, 4, 5, 10]          # minutes (sensitivity); HEADLINE below is the ranked one
HEADLINE = 10                    # headline breakdown threshold (min); 3/4/5 are sensitivity
HOLD_DWELL_MIN = 4               # fixed definition for habitual-hold detection (independent of HEADLINE)
BASELINES = ["sched", "median"]  # both reported; first is the headline
CLUSTER_MIN = 15
CLUSTER_MILES = 6.0
MAX_MIN = 180                    # longer = tracking gap / out of service, not a breakdown
SHUTDOWN_FRAC = 0.15
HOLD_SHARE = 0.05                # (route,dir,station) with >=5% of visits dwelling >=HEADLINE min = habitual/timed hold, not a breakdown
HOLD_MIN_VISITS = 300
MIN_HR_N = 15                    # min observations for an hour-of-day baseline
LINE = lambda r: "Green" if r.startswith("Green") else r
out = OUT / TAG; out.mkdir(parents=True, exist_ok=True)
con = connect()
diag = {}

def load(name, extra=""):
    parts = [f"SELECT '{m}' AS ym, * FROM '{WORK}/{m}/{name}.parquet'" for m in MONTHS]
    return " UNION ALL ".join(parts)

# ---------------------------------------------------------------- load
con.execute(f"CREATE TABLE segs0 AS {load('segs')}")
con.execute(f"CREATE TABLE dwell0 AS {load('dwell')}")
con.execute(f"CREATE TABLE segtab AS SELECT DISTINCT ON (route_id,direction_id,from_parent,to_parent) * FROM ({load('segments')}) ORDER BY ym DESC")
con.execute(f"CREATE TABLE sched0 AS {load('sched')}")
con.execute(f"CREATE TABLE stations AS SELECT DISTINCT ON (parent_station) * FROM ({load('stations')}) ORDER BY parent_station, ym DESC")
term_sql = " UNION ALL ".join("SELECT route_id, parent_station FROM read_csv('%s/%s/terminals.csv')" % (WORK, m) for m in MONTHS)
con.execute(f"CREATE TABLE terms AS SELECT DISTINCT * FROM ({term_sql})")
con.execute(f"CREATE TABLE sd AS {load('station_day')}")

# ---------------------------------------------------------------- planned shutdowns (station-day grid)
# typical = 75th percentile of daily event count for that (route, station, day-type) over all loaded months.
con.execute("""CREATE TABLE grid AS
  SELECT s.route_id, s.parent, d.service_date, coalesce(sd.n_events,0) n,
         CASE dayofweek(d.service_date) WHEN 0 THEN 'sun' WHEN 6 THEN 'sat' ELSE 'wk' END daytype
  FROM (SELECT DISTINCT route_id, parent FROM sd) s CROSS JOIN (SELECT DISTINCT service_date FROM sd) d
  LEFT JOIN sd ON sd.route_id=s.route_id AND sd.parent=s.parent AND sd.service_date=d.service_date""")
con.execute(f"""CREATE TABLE shut AS
  SELECT route_id, parent, service_date FROM (
    SELECT *, quantile_cont(n,0.75) OVER (PARTITION BY route_id, parent, daytype) typ FROM grid)
  WHERE typ >= 20 AND n < {SHUTDOWN_FRAC}*typ""")
# extend to neighbouring stations (the turnback / approach stations) using the segment graph
con.execute("""CREATE TABLE shut_ext AS
  SELECT route_id, parent, service_date FROM shut
  UNION SELECT s.route_id, g.to_parent, s.service_date FROM shut s JOIN segtab g ON g.route_id=s.route_id AND g.from_parent=s.parent
  UNION SELECT s.route_id, g.from_parent, s.service_date FROM shut s JOIN segtab g ON g.route_id=s.route_id AND g.to_parent=s.parent""")
diag["shutdown_station_days"] = con.execute("SELECT count(*) FROM shut").fetchone()[0]
diag["shutdown_ext_station_days"] = con.execute("SELECT count(*) FROM shut_ext").fetchone()[0]
con.execute("COPY (SELECT s.route_id, st.station_name, s.parent, min(service_date) first_day, max(service_date) last_day, count(*) n_days FROM shut s JOIN stations st ON st.parent_station=s.parent GROUP BY ALL ORDER BY n_days DESC) TO '%s/shutdown_stations.csv' (HEADER)" % out)

# ---------------------------------------------------------------- habitual holds / recurring layovers (per month)
# MBTA's schedule has zero dwell, so planned/timed holds and layovers must be found in the data. Per (month, route, dir, station):
#   rule A: >= HOLD_SHARE of visits dwell >= HOLD_DWELL_MIN  (timed holds, e.g. Green Line inbound)
#   rule B: >= 40 long dwells that are tightly clustered (IQR/median <= 0.30) with median >= 8 min
#           (a regular layover/diversion; real breakdowns have widely varying durations)
#   rule C (whole period): among dwells >= 20 min, >= 40 events with IQR/median <= 0.40 = a standing layover/diversion;
#           dwells inside that cluster's 2-98th percentile band are excluded (shorter dwells stay candidates)
con.execute(f"""CREATE TABLE holds AS
  WITH v AS (SELECT ym, route_id, direction_id, parent, count(*) visits FROM dwell0 WHERE NOT trip_end_stop GROUP BY ALL),
       l AS (SELECT ym, route_id, direction_id, parent, count(*) n_long, quantile_cont(dwell_sec,0.5) med_long,
                    (quantile_cont(dwell_sec,0.75)-quantile_cont(dwell_sec,0.25))/quantile_cont(dwell_sec,0.5) iqr_rel
             FROM dwell0 WHERE NOT trip_end_stop AND dwell_sec >= {HOLD_DWELL_MIN*60} GROUP BY ALL)
  SELECT v.*, l.n_long, l.n_long*1.0/v.visits share_long, l.med_long/60.0 med_long_min, l.iqr_rel,
         CASE WHEN l.n_long*1.0/v.visits >= {HOLD_SHARE} THEN 'A: share>=5%' ELSE 'B: tight long cluster' END reason
  FROM v JOIN l USING (ym, route_id, direction_id, parent)
  WHERE v.visits >= {HOLD_MIN_VISITS}
    AND (l.n_long*1.0/v.visits >= {HOLD_SHARE} OR (l.n_long >= 40 AND l.iqr_rel <= 0.30 AND l.med_long >= 480))""")
con.execute("COPY (SELECT h.*, s.station_name FROM holds h JOIN stations s ON s.parent_station=h.parent ORDER BY h.parent, h.route_id, h.direction_id, h.ym) TO '%s/holds.csv' (HEADER)" % out)
con.execute(f"""CREATE TABLE holds_all AS
  SELECT route_id, direction_id, parent, count(*) n_long, quantile_cont(dwell_sec,0.5)/60.0 med_long_min,
         (quantile_cont(dwell_sec,0.75)-quantile_cont(dwell_sec,0.25))/quantile_cont(dwell_sec,0.5) iqr_rel,
         quantile_cont(dwell_sec,0.02) lo, quantile_cont(dwell_sec,0.98) hi
  FROM dwell0 WHERE NOT trip_end_stop AND dwell_sec >= 1200 AND dwell_sec <= {MAX_MIN*60} GROUP BY ALL
  HAVING count(*) >= 40 AND iqr_rel <= 0.40""")
con.execute("COPY (SELECT h.*, s.station_name FROM holds_all h JOIN stations s ON s.parent_station=h.parent) TO '%s/standing_long_stops.csv' (HEADER)" % out)
diag["habitual_hold_station_months"] = con.execute("SELECT count(*) FROM holds").fetchone()[0]
diag["habitual_hold_groups"] = con.execute("SELECT count(DISTINCT (route_id, direction_id, parent)) FROM holds").fetchone()[0]

# ---------------------------------------------------------------- segments with distance / schedule / baseline
con.execute(f"""CREATE TABLE sch AS
  SELECT ym, trip_id, from_parent, to_parent, any_value(sched_arr - sched_dep) sched_run, any_value(next_dep) sched_next_dep, any_value(sched_arr) sched_arr FROM (
     SELECT *, lead(sched_dep) OVER (PARTITION BY ym, trip_id ORDER BY sched_dep, sched_arr) next_dep FROM sched0)
  WHERE sched_arr > sched_dep GROUP BY ALL""")
con.execute(f"""CREATE TABLE seg AS
  SELECT a.*, g.dist_mi, s.sched_run, s.sched_next_dep - s.sched_arr AS sched_dwell_after, d.dwell_sec AS dwell_after,
         (a.dep_sec/3600)::INT % 24 hr,
         NOT EXISTS (SELECT 1 FROM shut_ext x WHERE x.route_id=a.route_id AND x.service_date=a.service_date AND x.parent IN (a.from_parent,a.to_parent)) AS not_shutdown
  FROM segs0 a
  LEFT JOIN segtab g ON g.route_id=a.route_id AND g.direction_id=a.direction_id AND g.from_parent=a.from_parent AND g.to_parent=a.to_parent
  LEFT JOIN sch s ON s.ym=a.ym AND s.trip_id=a.trip_id AND s.from_parent=a.from_parent AND s.to_parent=a.to_parent
  LEFT JOIN (SELECT DISTINCT ON (service_date,trip_id,vehicle_id,parent) service_date,trip_id,vehicle_id,parent,dwell_sec FROM dwell0 ORDER BY service_date,trip_id,vehicle_id,parent,start_sec) d
         ON d.service_date=a.service_date AND d.trip_id=a.trip_id AND d.vehicle_id=a.vehicle_id AND d.parent=a.to_parent""")
diag["segments_total"] = con.execute("SELECT count(*) FROM seg").fetchone()[0]
diag["segments_without_gtfs_pair"] = con.execute("SELECT count(*) FROM seg WHERE dist_mi IS NULL").fetchone()[0]
diag["segments_over_max_min"] = con.execute(f"SELECT count(*) FROM seg WHERE run_sec > {MAX_MIN*60}").fetchone()[0]
con.execute(f"""CREATE TABLE base_hr AS SELECT route_id,direction_id,from_parent,to_parent,hr, quantile_cont(run_sec,0.5) med_hr
   FROM seg WHERE dist_mi IS NOT NULL AND run_sec <= {MAX_MIN*60} GROUP BY ALL HAVING count(*) >= {MIN_HR_N}""")
con.execute(f"""CREATE TABLE base_all AS SELECT route_id,direction_id,from_parent,to_parent, quantile_cont(run_sec,0.5) med_all
   FROM seg WHERE dist_mi IS NOT NULL AND run_sec <= {MAX_MIN*60} GROUP BY ALL""")
con.execute("""CREATE TABLE segb AS SELECT s.*, coalesce(h.med_hr, a.med_all) AS med_base,
   EXISTS (SELECT 1 FROM terms t WHERE t.route_id=s.route_id AND t.parent_station IN (s.from_parent, s.to_parent)) AS is_term_adj FROM seg s
   LEFT JOIN base_hr h USING (route_id,direction_id,from_parent,to_parent,hr) LEFT JOIN base_all a USING (route_id,direction_id,from_parent,to_parent)""")

# recurring between-stop delays (same tight-cluster rule, per month and segment): planned detours / standing slow orders / layover waits
con.execute(f"""CREATE TABLE seg_holds AS
  SELECT ym, route_id, direction_id, from_parent, to_parent, count(*) n, quantile_cont(ex,0.5)/60.0 med_excess_min,
         (quantile_cont(ex,0.75)-quantile_cont(ex,0.25))/quantile_cont(ex,0.5) iqr_rel
  FROM (SELECT *, run_sec - coalesce(sched_run, med_base) ex FROM segb
        WHERE dist_mi IS NOT NULL AND NOT is_term_adj AND not_shutdown AND run_sec <= {MAX_MIN*60})
  WHERE ex >= {HOLD_DWELL_MIN*60} GROUP BY ALL
  HAVING count(*) >= 40 AND iqr_rel <= 0.30 AND med_excess_min >= 8""")
con.execute("COPY (SELECT h.*, a.station_name from_station, b.station_name to_station FROM seg_holds h JOIN stations a ON a.parent_station=h.from_parent JOIN stations b ON b.parent_station=h.to_parent ORDER BY h.route_id, h.from_parent, h.ym) TO '%s/recurring_segment_delays.csv' (HEADER)" % out)
con.execute(f"""CREATE TABLE seg_holds_all AS
  SELECT route_id, direction_id, from_parent, to_parent, count(*) n, quantile_cont(ex,0.5)/60.0 med_excess_min,
         (quantile_cont(ex,0.75)-quantile_cont(ex,0.25))/quantile_cont(ex,0.5) iqr_rel,
         quantile_cont(ex,0.02) lo, quantile_cont(ex,0.98) hi
  FROM (SELECT *, run_sec - coalesce(sched_run, med_base) ex FROM segb
        WHERE dist_mi IS NOT NULL AND NOT is_term_adj AND not_shutdown AND run_sec <= {MAX_MIN*60})
  WHERE ex >= 1200 GROUP BY ALL
  HAVING count(*) >= 40 AND iqr_rel <= 0.45""")
con.execute("COPY (SELECT h.*, a.station_name from_station, b.station_name to_station FROM seg_holds_all h JOIN stations a ON a.parent_station=h.from_parent JOIN stations b ON b.parent_station=h.to_parent) TO '%s/standing_long_segment_delays.csv' (HEADER)" % out)
diag["recurring_segment_delay_segment_months"] = con.execute("SELECT count(*) FROM seg_holds").fetchone()[0]

# ---------------------------------------------------------------- positions along each line/direction (DAG longest path, miles)
edges = con.execute("SELECT route_id, direction_id, from_parent, to_parent, dist_mi FROM segtab").df()
edges["line"] = edges.route_id.map(LINE)
pos = {}; st_routes = collections.defaultdict(set)
for (line, d), g in edges.groupby(["line", "direction_id"]):
    nxt = collections.defaultdict(list); indeg = collections.Counter(); nodes = set()
    for r in g.drop_duplicates(["from_parent", "to_parent"]).itertuples():
        nxt[r.from_parent].append((r.to_parent, r.dist_mi)); indeg[r.to_parent] += 1; nodes |= {r.from_parent, r.to_parent}
    p = {n: 0.0 for n in nodes}; queue = [n for n in nodes if indeg[n] == 0]; seen = 0
    while queue:
        n = queue.pop(); seen += 1
        for m, w in nxt[n]:
            p[m] = max(p[m], p[n] + w); indeg[m] -= 1
            if indeg[m] == 0: queue.append(m)
    if seen < len(nodes): diag.setdefault("cyclic_line_dirs", []).append(f"{line}/{d}")
    for n, v in p.items(): pos[(line, d, n)] = v
for r in edges.itertuples():
    st_routes[(LINE(r.route_id), r.from_parent)].add(r.route_id); st_routes[(LINE(r.route_id), r.to_parent)].add(r.route_id)

# ---------------------------------------------------------------- candidates + incidents
terminal_pairs = set(map(tuple, con.execute("SELECT route_id, parent_station FROM terms").fetchall()))

def candidates(N, base):
    thr = N * 60
    base_expr = "coalesce(sched_run, med_base)" if base == "sched" else "med_base"
    segc = con.execute(f"""SELECT service_date, route_id, direction_id, trip_id, vehicle_id, 'segment' kind, to_parent stn, from_parent,
         dep_sec start_sec, arr_sec end_sec, run_sec dur_sec, run_sec - {base_expr} excess_sec
       FROM segb b WHERE dist_mi IS NOT NULL AND not_shutdown AND NOT is_term_adj AND run_sec <= {MAX_MIN*60} AND run_sec - {base_expr} >= {thr}
         AND NOT EXISTS (SELECT 1 FROM seg_holds h WHERE h.ym=b.ym AND h.route_id=b.route_id AND h.direction_id=b.direction_id AND h.from_parent=b.from_parent AND h.to_parent=b.to_parent)
         AND NOT EXISTS (SELECT 1 FROM seg_holds_all h WHERE h.route_id=b.route_id AND h.direction_id=b.direction_id AND h.from_parent=b.from_parent AND h.to_parent=b.to_parent AND (run_sec - {base_expr}) BETWEEN h.lo AND h.hi)""").df()
    dwc = con.execute(f"""SELECT service_date, route_id, direction_id, trip_id, vehicle_id, 'dwell' kind, parent stn, parent from_parent,
         start_sec, start_sec+dwell_sec end_sec, dwell_sec dur_sec, dwell_sec excess_sec
       FROM dwell0 d WHERE dwell_sec >= {thr} AND dwell_sec <= {MAX_MIN*60} AND NOT trip_end_stop
         AND NOT EXISTS (SELECT 1 FROM shut_ext x WHERE x.route_id=d.route_id AND x.service_date=d.service_date AND x.parent=d.parent)
         AND NOT EXISTS (SELECT 1 FROM holds h WHERE h.ym=d.ym AND h.route_id=d.route_id AND h.direction_id=d.direction_id AND h.parent=d.parent)
         AND NOT EXISTS (SELECT 1 FROM holds_all h WHERE h.route_id=d.route_id AND h.direction_id=d.direction_id AND h.parent=d.parent AND d.dwell_sec BETWEEN h.lo AND h.hi)""").df()
    dwc = dwc[[ (r, p) not in terminal_pairs for r, p in zip(dwc.route_id, dwc.stn)]]
    c = pd.concat([segc, dwc], ignore_index=True)
    c["line"] = c.route_id.map(LINE)
    c["pos"] = [pos.get((l, d, s), np.nan) for l, d, s in zip(c.line, c.direction_id, c.stn)]
    return c.dropna(subset=["pos"]).sort_values(["service_date", "line", "direction_id", "start_sec"]).reset_index(drop=True)

def cluster(c):
    """Greedy time/space clustering of candidates into incidents (see module docstring)."""
    inc = np.zeros(len(c), dtype=int); nxt_id = 0
    for _, g in c.groupby(["service_date", "line", "direction_id"], sort=False):
        open_ = []  # each: dict(id, last, front, routes)
        for i, r in zip(g.index, g.itertuples()):
            my_routes = st_routes[(r.line, r.stn)]
            hit = None
            for cl in open_:
                if (r.start_sec - cl["last"] <= CLUSTER_MIN * 60 and r.pos <= cl["front"] + 0.5
                        and cl["front"] - r.pos <= CLUSTER_MILES and (my_routes & cl["routes"])):
                    hit = cl; break
            if hit is None:
                hit = dict(id=nxt_id, last=r.start_sec, front=r.pos, routes=set()); nxt_id += 1; open_.append(hit)
            hit["last"] = max(hit["last"], r.start_sec); hit["front"] = max(hit["front"], r.pos); hit["routes"] |= my_routes
            inc[c.index.get_loc(i)] = hit["id"]
    c = c.copy(); c["incident"] = inc
    return c

stn = con.execute("SELECT parent_station, station_name, lat, lon FROM stations").df().set_index("parent_station")
visits = con.execute("SELECT parent, count(*) visits FROM dwell0 GROUP BY 1").df().set_index("parent")["visits"]
results = {}
for base in BASELINES:
    for N in THRESH:
        c = cluster(candidates(N, base))
        # one row per incident: frontmost station
        c = c.sort_values(["incident", "pos", "start_sec"], ascending=[True, False, True])
        front = c.groupby("incident").first()
        agg = c.groupby("incident").agg(n_events=("stn", "size"), n_vehicles=("vehicle_id", "nunique"), max_excess_min=("excess_sec", lambda x: x.max()/60),
                                        first_start=("start_sec", "min"), last_end=("end_sec", "max"))
        inc = front[["service_date", "line", "route_id", "direction_id", "stn", "kind", "from_parent", "trip_id", "vehicle_id"]].join(agg)
        results[(base, N)] = (c, inc)
        if base == BASELINES[0]: diag[f"incident_size_{N}min_events_mean_p99_max"] = [round(float(inc.n_events.mean()),2), float(inc.n_events.quantile(.99)), int(inc.n_events.max())]
        diag[f"candidates_{base}_{N}min"] = int(len(c)); diag[f"incidents_{base}_{N}min"] = int(len(inc))

# ranked station table (headline baseline, all thresholds side by side)
def station_table(base):
    t = pd.DataFrame(index=stn.index)
    for N in THRESH:
        c, inc = results[(base, N)]
        t[f"incidents_{N}min"] = inc.groupby("stn").size()
        if N == HEADLINE:
            t[f"events_{HEADLINE}min"] = c.groupby("stn").size()
            t["lead_dwell_share"] = inc.assign(d=(inc.kind == "dwell")).groupby("stn").d.mean()
            t["lines"] = inc.groupby("stn").line.agg(lambda s: "/".join(sorted(set(s))))
            t["median_max_excess_min"] = inc.groupby("stn").max_excess_min.median()
    t = t.dropna(subset=[f"incidents_{HEADLINE}min"]).fillna(0)
    t["station"] = stn.station_name.reindex(t.index); t["lat"] = stn.lat; t["lon"] = stn.lon
    t["train_visits"] = visits.reindex(t.index)
    t[f"incidents_per_10k_visits_{HEADLINE}min"] = t[f"incidents_{HEADLINE}min"] / t.train_visits * 1e4
    for N in THRESH: t[f"rank_{N}min"] = t[f"incidents_{N}min"].rank(ascending=False, method="min")
    return t.sort_values([f"incidents_{HEADLINE}min", "train_visits"], ascending=False).reset_index().rename(columns={"index": "parent_station"})

for base in BASELINES:
    t = station_table(base); t.to_csv(out / f"station_breakdowns_{base}.csv", index=False)
    for N in THRESH:
        results[(base, N)][1].reset_index().merge(stn[["station_name"]], left_on="stn", right_index=True, how="left").to_csv(out / f"incidents_{base}_{N}min.csv", index=False)
    print(f"\n=== TOP 15 stations by incidents (baseline={base}) ===")
    print(t.head(15)[["station", "lines"] + [f"incidents_{N}min" for N in THRESH] + [f"events_{HEADLINE}min", "lead_dwell_share", "train_visits", f"incidents_per_10k_visits_{HEADLINE}min"]].round(2).to_string(index=False))
a, b = (station_table(x).set_index("parent_station")[f"incidents_{HEADLINE}min"] for x in BASELINES)
diag["rank_corr_sched_vs_median_baseline_headline"] = float(a.rank().corr(b.reindex(a.index).fillna(0).rank()))

# ---------------------------------------------------------------- speed
# clean = not in any 4-min (headline) incident and not shutdown-adjacent
c4, _ = results[(BASELINES[0], HEADLINE)]
seg_keys = c4[c4.kind == "segment"][["service_date", "trip_id", "vehicle_id", "from_parent", "stn"]].rename(columns={"stn": "to_parent"})
seg_keys["flag"] = True
con.register("flagged", seg_keys)
con.execute(f"""CREATE TABLE spd AS
  SELECT s.*, f.flag IS NOT NULL AS in_incident,
         (dist_mi / (run_sec/3600.0)) mph
  FROM segb s LEFT JOIN flagged f USING (service_date, trip_id, vehicle_id, from_parent, to_parent)
  WHERE dist_mi IS NOT NULL AND run_sec >= 10 AND run_sec <= {MAX_MIN*60}""")
diag["segments_dropped_over_70mph"] = con.execute("SELECT count(*) FROM spd WHERE mph > 70").fetchone()[0]
con.execute("DELETE FROM spd WHERE mph > 70")
# Green shared (trunk) vs branch-only: a directed station pair served by >=2 Green routes is 'shared'
con.execute("""CREATE TABLE green_shared AS SELECT direction_id, from_parent, to_parent FROM segtab
   WHERE route_id LIKE 'Green%' GROUP BY ALL HAVING count(DISTINCT route_id) >= 2""")
con.execute("""ALTER TABLE spd ADD COLUMN scope VARCHAR""")
con.execute("""UPDATE spd SET scope = CASE WHEN route_id LIKE 'Green%' THEN
      CASE WHEN EXISTS (SELECT 1 FROM green_shared g WHERE g.direction_id=spd.direction_id AND g.from_parent=spd.from_parent AND g.to_parent=spd.to_parent)
           THEN 'Green shared trunk' ELSE route_id || ' branch-only' END
    ELSE route_id END""")

def spd_table(group_sql, where="TRUE"):
    """Speeds = sum(distance)/sum(time).
    running = moving time only (DEP at A -> ARR at B).
    door-to-door = running + dwell at B; the like-for-like pair to the schedule, because GTFS arrival==departure at
    every stop, so a scheduled segment time (arr_B - dep_A) already contains the station dwell.
    d2d columns use only segments where both actual dwell and a scheduled time exist, so actual and scheduled
    cover exactly the same trips/segments."""
    D = "dwell_after IS NOT NULL AND sched_run > 0"
    CLEAN = "NOT in_incident AND not_shutdown AND NOT is_term_adj"
    return con.execute(f"""SELECT {group_sql} AS name, count(*) n_segments, round(sum(dist_mi),0) miles_observed,
      round(sum(dist_mi)/(sum(run_sec)/3600.0),2) running_mph,
      round(sum(CASE WHEN {CLEAN} THEN dist_mi END)/(sum(CASE WHEN {CLEAN} THEN run_sec END)/3600.0),2) running_mph_clean,
      round(sum(CASE WHEN {D} THEN dist_mi END)/(sum(CASE WHEN {D} THEN run_sec+dwell_after END)/3600.0),2) actual_door_to_door_mph,
      round(sum(CASE WHEN {D} THEN dist_mi END)/(sum(CASE WHEN {D} THEN sched_run END)/3600.0),2) scheduled_mph,
      round(sum(CASE WHEN {D} AND {CLEAN} THEN dist_mi END)/(sum(CASE WHEN {D} AND {CLEAN} THEN run_sec+dwell_after END)/3600.0),2) actual_door_to_door_mph_clean
      FROM spd WHERE {where} GROUP BY ALL ORDER BY 1""").df()

tabs = []
line_t = spd_table("CASE WHEN route_id LIKE 'Green%' THEN 'Green (all branches)' ELSE route_id END"); line_t.insert(0, "level", "line"); tabs.append(line_t)
route_t = spd_table("route_id", "route_id LIKE 'Green%'"); route_t.insert(0, "level", "branch (incl. shared trunk)"); tabs.append(route_t)
scope_t = spd_table("scope", "route_id LIKE 'Green%'"); scope_t.insert(0, "level", "green segment class"); tabs.append(scope_t)
speed = pd.concat(tabs, ignore_index=True)
speed["actual_vs_sched_pct"] = ((speed.actual_door_to_door_mph / speed.scheduled_mph - 1) * 100).round(1)
speed.to_csv(out / "speed_table.csv", index=False)
print("\n=== SPEED (mph) ==="); print(speed.to_string(index=False))

seg_out = con.execute("""SELECT route_id, direction_id, from_parent, to_parent, any_value(dist_mi) dist_mi, count(*) n,
   sum(dist_mi)/(sum(run_sec)/3600.0) actual_mph,
   sum(CASE WHEN NOT in_incident AND not_shutdown AND NOT is_term_adj THEN dist_mi END)/(sum(CASE WHEN NOT in_incident AND not_shutdown AND NOT is_term_adj THEN run_sec END)/3600.0) actual_mph_clean,
   sum(CASE WHEN sched_run>0 THEN dist_mi END)/nullif(sum(CASE WHEN sched_run>0 THEN sched_run END)/3600.0,0) sched_mph_incl_dwell
   FROM spd GROUP BY ALL""").df()
seg_out.to_csv(out / "segment_speeds.csv", index=False)
json.dump(diag, open(out / "diagnostics.json", "w"), indent=1, default=str)
print("\n=== DIAGNOSTICS ==="); print(json.dumps(diag, indent=1, default=str))
