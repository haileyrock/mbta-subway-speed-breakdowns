"""Build the interactive HTML map from analysis outputs.

Segments are coloured by actual MOVING speed (DEP at A -> ARR at B; both directions and all routes
sharing a station pair are combined as sum(distance)/sum(time)). Stations are sized by breakdown
incidents (threshold selectable: 3/4/5/10 min).
Usage: python scripts/05_build_map.py <tag> <YYYY-MM|all>   # tag = outputs/<tag>; month(s) = GTFS geometry source
Output: outputs/<tag>/map.html  (self-contained except for Leaflet + basemap tiles from CDNs)
"""
import sys, json
import pandas as pd
from common import OUT, WORK

tag, ym = sys.argv[1], sys.argv[2]
src = OUT / tag
seg = pd.read_csv(src / "segment_speeds.csv")
months = sorted(p.name for p in WORK.glob("2026-??") if p.is_dir()) if ym == "all" else [ym]
geo = (pd.concat([pd.read_parquet(WORK / m / "segments.parquet") for m in months])[["route_id", "direction_id", "from_parent", "to_parent", "geom"]]
       .drop_duplicates(["route_id", "direction_id", "from_parent", "to_parent"], keep="last"))
stn = (pd.concat([pd.read_parquet(WORK / m / "stations.parquet") for m in months]).drop_duplicates("parent_station", keep="last").set_index("parent_station"))
inc = pd.read_csv(src / "station_breakdowns_sched.csv").set_index("parent_station")
spd = pd.read_csv(src / "speed_table.csv")
diag = json.load(open(src / "diagnostics.json"))

seg = seg.merge(geo, on=["route_id", "direction_id", "from_parent", "to_parent"], how="left")
seg["a"] = seg[["from_parent", "to_parent"]].min(axis=1); seg["b"] = seg[["from_parent", "to_parent"]].max(axis=1)
seg["line"] = seg.route_id.str.replace(r"-.*", "", regex=True)
seg["t_all"] = seg.n * seg.dist_mi / seg.actual_mph                    # n*dist = miles observed; /mph = hours
seg["t_clean"] = seg.n * seg.dist_mi / seg.actual_mph_clean
rows = []
for (a, b), g in seg.groupby(["a", "b"]):
    gc = g.dropna(subset=["actual_mph_clean"])
    rows.append(dict(a=stn.station_name.get(a, a), b=stn.station_name.get(b, b), line=sorted(set(g.line)), routes=sorted(set(g.route_id)),
        mi=round(float(g.dist_mi.median()), 2), n=int(g.n.sum()),
        mph=round(float((g.n * g.dist_mi).sum() / g.t_all.sum()), 1),
        mph_clean=round(float((gc.n * gc.dist_mi).sum() / gc.t_clean.sum()), 1) if len(gc) else None,
        geom=json.loads(g.geom.dropna().iloc[0])))
stations = []
used = set(seg.a) | set(seg.b)
for p in used:
    r = inc.loc[p] if p in inc.index else None
    stations.append(dict(id=p, name=stn.station_name[p], lat=round(stn.lat[p], 5), lon=round(stn.lon[p], 5),
        lines=(r.lines if r is not None else "/".join(sorted(set(seg[(seg.a == p) | (seg.b == p)].line)))),
        inc={str(n): int(r[f"incidents_{n}min"]) if r is not None else 0 for n in (3, 4, 5, 10)},
        visits=int(r.train_visits) if r is not None and pd.notna(r.train_visits) else None))
lines = spd[spd.level == "line"].sort_values("running_mph", ascending=False)
speed_rows = [dict(name=r["name"].replace(" (all branches)", ""), moving=r.running_mph, d2d=r.actual_door_to_door_mph, sched=r.scheduled_mph) for _, r in lines.iterrows()]
br = spd[spd.level == "branch (incl. shared trunk)"].sort_values("running_mph", ascending=False)
speed_rows += [dict(name=r["name"], moving=r.running_mph, d2d=r.actual_door_to_door_mph, sched=r.scheduled_mph, sub=True) for _, r in br.iterrows()]
data = dict(segments=rows, stations=stations, speeds=speed_rows, label=f"{ym} prototype" if tag == "jan" else "Jan–Oct 2026",
            habitual_holds=diag.get("habitual_hold_groups"))
html = open(__file__.replace("05_build_map.py", "map_template.html")).read()
(src / "map.html").write_text(html.replace("__DATA__", json.dumps(data, separators=(",", ":"))))
print("wrote", src / "map.html", f"{len(rows)} segments, {len(stations)} stations")
