# News ETL Pipeline

ETL pipeline for processing RSS feeds from Israeli news sources (Ynet, Hayom, Haaretz) into canonical CSVs and unified masters. The pipeline is modular and CI-friendly and can be run end-to-end per source or step-by-step.

## Overview

- Extract: fetch RSS and convert XML → JSON → CSV
- Transform: expand rich fields and map into a canonical schema
- Enhance: optionally scrape missing fields (selectors-driven)
- Load: create per-source masters and a unified master file

Data, logs, and outputs live under `data/` and `logs/`.

## Installation (Windows PowerShell)

```powershell
# from repo root
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Quick Start

Run the orchestrated pipeline per source using the built-in orchestrator. Repeat for each source to build/update the unified master (`data/master/master_news.csv`).

```powershell
# Ynet
py -m etl.pipelines.etl_by_source --source ynet --rss https://www.ynet.co.il/Integration/StoryRss2.xml

# Hayom
for israel hayom, use: 
py -m etl.pipelines.etl_by_source --source hayom --rss https://www.israelhayom.co.il/rss.xml --force-tz-offset 3

# Haaretz
py -m etl.pipelines.etl_by_source --source haaretz --rss https://www.haaretz.co.il/srv/htz---all-articles
```
![alt text](<Drawing 1-1.png>)
Each run executes the following steps:

```
etl.transform.extract_by_source      → Download & convert to CSV
etl.transform.expand_by_source       → Expand HTML/JSON-like fields into columns
etl.transform.canonized_by_source    → Map into canonical schema (via mapping.csv)
etl.load.create_csv_to_load_by_source→ Compute per-source delta to load
etl.load.enhancer_by_source          → Optional enrichment from article pages
etl.pipelines.download_images        → Download images and sanitize text
etl.load.load_by_source              → Update per-source master (master_{source}.csv)
etl.load.merge_by_source             → Append into unified master (master_news.csv)
```

The final line printed by each step is the output file path consumed by the next step. The orchestrator wires this automatically.

### Step-by-step (manual)

If you prefer to run steps yourself:

```powershell
# 1) Extract RSS → CSV
py -m etl.transform.extract_by_source --source ynet --rss-url https://www.ynet.co.il/Integration/StoryRss2.xml
# Output: data/raw/ynet/ynet_YYYYmmdd_HHMMSS.csv

# 2) Expand
py -m etl.transform.expand_by_source --input data/raw/ynet/ynet_YYYYmmdd_HHMMSS.csv --output data/raw/ynet/ynet_YYYYmmdd_HHMMSS_expanded.csv

# 3) Canonicalize (uses mapping at etl/schema/mapping.csv)
py -m etl.transform.canonized_by_source --input data/raw/ynet/ynet_YYYYmmdd_HHMMSS_expanded.csv
# Output: data/canonical/ynet/ynet_YYYYmmdd_HHMMSS_expanded_canonical.csv

# 4) Create per-source delta to load
py -m etl.load.create_csv_to_load_by_source --input data/canonical/ynet/ynet_..._canonical.csv
# Output: ..._canonical_unenhanced.csv

# 5) Optional: Enhance from article pages (needs etl/enhance/selectors.csv)
py -m etl.load.enhancer_by_source --input data/canonical/ynet/ynet_..._canonical_unenhanced.csv
# Output: ..._canonical_enhanced.csv

# 6) Download images and sanitize text
py -m etl.pipelines.download_images --input data/canonical/ynet/ynet_..._canonical_enhanced.csv
# Output: ..._master.csv

# 7) Update per-source master
py -m etl.load.load_by_source --input data/canonical/ynet/ynet_..._master.csv --source ynet
# Output: data/master/master_ynet.csv

# 8) Append to unified master
py -m etl.load.merge_by_source --source data/master/master_ynet.csv --master data/master/master_news.csv
```

## Output Layout

```
data/
├── raw/{source}/
│   ├── {source}_YYYYmmdd_HHMMSS.xml     # Raw RSS
│   ├── {source}_YYYYmmdd_HHMMSS.json    # Parsed JSON
│   └── {source}_YYYYmmdd_HHMMSS.csv     # Extracted items
├── canonical/{source}/
│   ├── ..._canonical.csv                # Canonical schema
│   └── ..._master.csv                   # Step output after image download/sanitize
└── master/
    ├── master_{source}.csv              # Per-source master
    └── master_news.csv                  # Unified master (all sources)
```

## Configuration

- Default RSS URLs are in `etl/config.py`.
- Canonical mapping is read from `etl/schema/mapping.csv`.
- Enhancer selectors are read from `etl/enhance/selectors.csv`.

### Time normalization
Publication times are normalized to UTC with a fixed `+0000` suffix, regardless of source offset. Example: `Mon, 15 Sep 2025 23:16:57 +0300` → `Mon, 15 Sep 2025 23:16:57+0000`.

### IDs and Deduplication
- Canonical `id` is a SHA1 of `title|pubDate|source`.
- Per-source masters dedupe by `id`/`guid` and sort by `pubDate` (newest first).
- The unified master dedupes by `id` and sorts by `pubDate`.

## Scheduling

Run each source hourly or as needed. Example Windows Task Scheduler action (run three commands sequentially):

```powershell
powershell.exe -ExecutionPolicy Bypass -NoProfile -Command "cd C:\code\news-etl; \
 py -m etl.pipelines.etl_by_source --source ynet --rss https://www.ynet.co.il/Integration/StoryRss2.xml; \
 py -m etl.pipelines.etl_by_source --source hayom --rss https://www.israelhayom.co.il/rss.xml; \
 py -m etl.pipelines.etl_by_source --source haaretz --rss https://www.haaretz.co.il/srv/htz---all-articles"
```

Note: `run_etl.ps1` exists but references legacy modules. Prefer the Python commands above.

## Troubleshooting

- Mapping file missing: ensure `etl/schema/mapping.csv` is present and tracked in git.
- Selectors missing: ensure `etl/enhance/selectors.csv` exists (enhancer is optional; pipeline soft-fails this step).
- Unified master empty: check for BOM-prefixed headers like `\ufeffid`; the merge step already handles this.

## Development

Key modules:

- `etl/transform/extract_by_source.py` – Fetch RSS, convert to JSON/CSV, print CSV path.
- `etl/transform/expand_by_source.py` – Expand HTML/JSON-like fields.
- `etl/transform/canonized_by_source.py` – Map into canonical schema and compute `id`.
- `etl/load/create_csv_to_load_by_source.py` – Compute delta vs per-source master.
- `etl/load/enhancer_by_source.py` – Optional page scraping based on selectors.
- `etl/pipelines/download_images.py` – Download images and sanitize text.
- `etl/load/load_by_source.py` – Update `master_{source}.csv`.
- `etl/load/merge_by_source.py` – Append into `master_news.csv`.
- `etl/pipelines/etl_by_source.py` – Orchestrates all the above per source.

## License

MIT (or project-specific; update if needed)