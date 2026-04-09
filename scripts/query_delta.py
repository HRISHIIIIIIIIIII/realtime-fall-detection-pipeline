"""
Delta Lake Query Script — Read and inspect Delta Lake tables.

Usage:
    python3 scripts/query_delta.py              # Show summary of all tables
    python3 scripts/query_delta.py silver_fds   # Show details of a specific table
    python3 scripts/query_delta.py falls        # Show only fall events
    python3 scripts/query_delta.py history       # Show Delta Lake version history
"""

import sys
import os
from deltalake import DeltaTable
import pandas as pd

BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "delta")
TABLES = ["bronze_fds", "bronze_obj", "silver_fds", "silver_obj"]


def load_table(name):
    """Load a Delta table, return DeltaTable object and DataFrame."""
    path = os.path.join(BASE_PATH, name)
    if not os.path.exists(os.path.join(path, "_delta_log")):
        return None, None
    try:
        dt = DeltaTable(path)
        df = dt.to_pandas()
        return dt, df
    except Exception as e:
        print(f"\n  {name}: CORRUPT ({e})")
        return None, None


def show_summary():
    """Show a summary of all tables."""
    print("\n" + "=" * 70)
    print("  DELTA LAKE TABLES SUMMARY")
    print("=" * 70)

    for name in TABLES:
        dt, df = load_table(name)
        if df is None:
            print(f"\n  {name}: NO DATA")
            continue

        print(f"\n  {name}")
        print(f"  {'─' * 40}")
        print(f"  Rows:    {len(df)}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Version: {dt.version()}")

        if "ingestion_time" in df.columns:
            print(f"  First:   {df['ingestion_time'].min()}")
            print(f"  Last:    {df['ingestion_time'].max()}")

        if "is_falling" in df.columns:
            falls = df[df["is_falling"] == True]
            print(f"  Falls:   {len(falls)}")

        if "object_id" in df.columns:
            unique_objects = df["object_id"].nunique()
            print(f"  Unique objects tracked: {unique_objects}")

    print("\n" + "=" * 70)


def show_table(name):
    """Show details of a specific table."""
    dt, df = load_table(name)
    if df is None:
        print(f"Table '{name}' has no data yet.")
        return

    print(f"\n{'=' * 70}")
    print(f"  TABLE: {name} ({len(df)} rows, version {dt.version()})")
    print(f"{'=' * 70}")
    print(f"\nSchema:")
    print(dt.schema())
    print(f"\nLatest 10 rows:")
    print(df.tail(10).to_string())
    print()


def show_falls():
    """Show fall events from silver_fds."""
    _, df = load_table("silver_fds")
    if df is None:
        print("silver_fds table has no data yet.")
        return

    falls = df[df["is_falling"] == True]
    print(f"\n{'=' * 70}")
    print(f"  FALL EVENTS ({len(falls)} out of {len(df)} total)")
    print(f"{'=' * 70}")

    if len(falls) == 0:
        print("  No falls detected yet.")
    else:
        display_cols = [
            "device_id", "event_time", "frame_number",
            "position_x", "position_y", "location", "activity"
        ]
        cols = [c for c in display_cols if c in falls.columns]
        print(falls[cols].to_string())
    print()


def show_history():
    """Show Delta Lake version history for all tables."""
    print(f"\n{'=' * 70}")
    print(f"  DELTA LAKE VERSION HISTORY")
    print(f"{'=' * 70}")

    for name in TABLES:
        path = os.path.join(BASE_PATH, name)
        if not os.path.exists(os.path.join(path, "_delta_log")):
            continue

        dt = DeltaTable(path)
        history = dt.history()
        print(f"\n  {name} (current version: {dt.version()})")
        print(f"  {'─' * 40}")
        for entry in history[:5]:  # Show last 5 versions
            ts = entry.get("timestamp", "?")
            op = entry.get("operation", "?")
            params = entry.get("operationParameters", {})
            print(f"    v{entry.get('version', '?')} | {ts} | {op} | {params}")
    print()


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None

    if arg is None:
        show_summary()
    elif arg == "falls":
        show_falls()
    elif arg == "history":
        show_history()
    elif arg in TABLES:
        show_table(arg)
    else:
        print(f"Unknown argument: {arg}")
        print(f"Usage: python3 {sys.argv[0]} [{'|'.join(TABLES)}|falls|history]")
