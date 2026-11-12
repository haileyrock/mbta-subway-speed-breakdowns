"""Download one archived MBTA GTFS feed per month of 2026 (the feed in force on the 15th).

Source: https://cdn.mbta.com/archive/archived_feeds.txt  (public, no API key needed).
Why: the events' trip_ids match the GTFS feed that was live on that date (verified 100% for
Jan 7-13), so each month is compared against its own schedule.
Usage: python -I scripts/00_download_gtfs.py [YYYY-MM ...]   (default: 2026-01 .. 2026-10)
"""
import csv, io, sys, urllib.request, zipfile
from common import GTFS_ARCHIVE

INDEX = "https://cdn.mbta.com/archive/archived_feeds.txt"
months = sys.argv[1:] or [f"2026-{m:02d}" for m in range(1, 11)]
GTFS_ARCHIVE.mkdir(exist_ok=True)

def fetch(url):
    with urllib.request.urlopen(url, timeout=120) as r:
        return r.read()

rows = list(csv.DictReader(io.StringIO(fetch(INDEX).decode())))
for ym in months:
    day = ym.replace("-", "") + "15"
    cands = [r for r in rows if r["feed_start_date"] <= day <= r["feed_end_date"]]
    if not cands:
        print(ym, "no feed covers", day); continue
    r = cands[0]
    dest = GTFS_ARCHIVE / ym
    if (dest / "stop_times.txt").exists():
        print(ym, "already downloaded"); continue
    print(ym, "->", r["archive_url"], "|", r["feed_version"])
    zipfile.ZipFile(io.BytesIO(fetch(r["archive_url"]))).extractall(dest)
