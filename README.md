# MBTA subway speed & breakdowns (Jan–Oct 2026)

Interactive map of MBTA subway track speeds (colored by line, lighter = faster) and station breakdown counts (grey dots, darker = more).

## View the map
The map needs to be served over http (OpenStreetMap blocks tiles for `file://` pages):

    cd outputs/all && python3 -m http.server 8000
    # open http://localhost:8000/map.html

## Rebuild
Raw data (`Events_2026/`, `MBTA_GTFS*/`, `data_work/`) is not in the repo. With the data in place, run scripts `00`–`05` in `scripts/` in order (see each file's docstring for usage). `05_build_map.py all all` regenerates `outputs/all/map.html` from `scripts/map_template.html`.
