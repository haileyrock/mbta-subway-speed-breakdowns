"""Shared paths and helpers for the MBTA analysis scripts."""
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent          # .../question
EVENTS = ROOT / "Events_2026"
GTFS_ARCHIVE = ROOT / "MBTA_GTFS_archive"              # one feed per month (see 00_download_gtfs.py)
WORK = ROOT / "data_work"                              # intermediate parquet (re-creatable)
OUT = ROOT / "outputs"                                 # final tables / maps

# Subway routes in scope (Mattapan is excluded by request).
ROUTES = ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E"]
ROUTES_SQL = ", ".join(f"'{r}'" for r in ROUTES)

def gtfs_reader(feed_dir, name):
    """DuckDB read_csv expression for a GTFS txt file. trips.txt has an unquoted-comma row,
    so parse leniently and keep everything as text."""
    return (f"read_csv('{feed_dir}/{name}.txt', all_varchar=true, quote='\"', "
            f"null_padding=true, strict_mode=false)")

def connect():
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    return con
