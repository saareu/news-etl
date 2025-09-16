"""Append non-duplicated rows from a source master CSV into a unified master CSV.

This script accepts two relative paths:
- `--source` : the per-source master CSV (input)
- `--master` : the unified master CSV to append into (output)

Behavior:
- Reads both CSVs (master may not exist yet).
- Finds rows in `source` that are not present in `master` (by `id`).
- Appends those new rows to `master`.
- Sorts the resulting master by `pubDate` descending and writes it back.
- Prints the relative path to the unified master as the final stdout line.

Usage:
  py -m etl.load.merge_by_source --source data/canonical/hayom/hayom_..._master.csv --master data/master/master_news.csv
"""
from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime
from typing import Dict, List, Optional


def read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv(path: str, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def parse_pubdate(val: Optional[str]) -> float:
    if not val:
        return 0.0
    try:
        dt = datetime.fromisoformat(val)
        return dt.timestamp()
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(val, fmt).timestamp()
            except Exception:
                continue
    return 0.0


def get_id_key(row):
    # Handle BOM-prefixed id field
    return row.get('id') or row.get('\ufeffid')

def merge_by_source(source_path: str, master_path: str) -> str:
    source_rows = read_csv(source_path)
    master_rows = read_csv(master_path)
    print(f"[merge_by_source] source: {source_path} rows={len(source_rows)}; master: {master_path} rows={len(master_rows)}")
    def safe_print(msg):
        try:
            print(msg)
        except UnicodeEncodeError:
            print(msg.encode('ascii', errors='replace').decode('ascii', errors='replace'))
    if source_rows:
        safe_print(f"[merge_by_source] source fieldnames: {list(source_rows[0].keys())}")
        safe_print(f"[merge_by_source] source first row: {repr(source_rows[0])}")
    if master_rows:
        safe_print(f"[merge_by_source] master fieldnames: {list(master_rows[0].keys())}")
        safe_print(f"[merge_by_source] master first row: {repr(master_rows[0])}")

    # Debug: print all id keys in master and source
    master_ids = set(get_id_key(r) for r in master_rows if get_id_key(r))
    source_ids = set(get_id_key(r) for r in source_rows if get_id_key(r))
    safe_print(f"[merge_by_source] master id keys (sample 10): {list(master_ids)[:10]}")
    safe_print(f"[merge_by_source] source id keys (sample 10): {list(source_ids)[:10]}")

    # build set of keys present in master
    existing_keys = set()
    for r in master_rows:
        key = get_id_key(r)
        if key:
            existing_keys.add(key)

    # collect new rows from source not in master
    new_rows = []
    for r in source_rows:
        key = get_id_key(r)
        if not key or key not in existing_keys:
            new_rows.append(r)
    safe_print(f"[merge_by_source] new rows to add: {len(new_rows)}")

    if not new_rows:
        # nothing to do; still ensure master exists and is sorted
        if not master_rows and source_rows:
            # master is empty, source has data: copy all source rows
            combined = source_rows
        else:
            combined = master_rows
    else:
        combined = master_rows + new_rows

    # dedupe by id preserving last occurrence
    deduped: Dict[str, Dict[str, str]] = {}
    for r in combined:
        key = get_id_key(r)
        if key is not None:
            deduped[key] = r

    rows = list(deduped.values())
    print(f"[merge_by_source] after merge: unified master rows={len(rows)} (added {len(rows)-len(master_rows)})")

    # sort by pubDate desc
    rows.sort(key=lambda r: parse_pubdate(r.get("pubDate")), reverse=True)

    # determine fieldnames as union preserving order from master then source
    fieldnames: List[str] = []
    for src in (master_rows + source_rows):
        for k in src.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    write_csv(master_path, rows, fieldnames)
    return master_path


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Append non-duplicated rows from a source master into a unified master CSV")
    p.add_argument("--source", required=True, help="Relative path to source master CSV")
    p.add_argument("--master", required=True, help="Relative path to unified master CSV to append into")
    args = p.parse_args(argv)

    if not os.path.exists(args.source):
        print(f"source not found: {args.source}")
        return 2

    out = merge_by_source(args.source, args.master)
    print(os.path.relpath(out).replace("\\", "/"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
