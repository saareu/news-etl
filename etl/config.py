# etl/config.py
from __future__ import annotations
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR        = BASE_DIR / "data"
RAW_DIR         = DATA_DIR / "raw"
RAW_YNET_DIR    = RAW_DIR / "ynet"
RAW_HAYOM_DIR   = RAW_DIR / "hayom"
RAW_HAARETZ_DIR = RAW_DIR / "haaretz"

CANON_DIR       = DATA_DIR / "canonical"
CANON_YNET_DIR  = CANON_DIR / "ynet"
CANON_HAYOM_DIR = CANON_DIR / "hayom"
CANON_HAARETZ_DIR = CANON_DIR / "haaretz"

MASTER_DIR      = DATA_DIR / "master"

# Source-specific master files
MASTER_YNET_CSV    = MASTER_DIR / "master_ynet.csv"
MASTER_YNET_JSON   = MASTER_DIR / "master_ynet.json"
MASTER_HAYOM_CSV   = MASTER_DIR / "master_hayom.csv"
MASTER_HAYOM_JSON  = MASTER_DIR / "master_hayom.json"
MASTER_HAARETZ_CSV = MASTER_DIR / "master_haaretz.csv"
MASTER_HAARETZ_JSON = MASTER_DIR / "master_haaretz.json"

# Default filenames (override with CLI flags any time)
YNET_ITEMS_JSON    = RAW_YNET_DIR / "ynet_items.json"
YNET_ITEMS_CSV    = RAW_YNET_DIR / "ynet_items.csv"
YNET_CANON_JSON  = CANON_YNET_DIR / "ynet_canonical.json"
YNET_CANON_CSV   = CANON_YNET_DIR / "ynet_canonical.csv"

HAYOM_ITEMS_JSON = RAW_HAYOM_DIR / "hayom_items.json"
HAYOM_ITEMS_CSV  = RAW_HAYOM_DIR / "hayom_items.csv"
HAYOM_CANON_CSV  = CANON_HAYOM_DIR / "hayom_canonical.csv"
HAYOM_CANON_JSON = CANON_HAYOM_DIR / "hayom_canonical.json"

HAARETZ_ITEMS_JSON = RAW_HAARETZ_DIR / "haaretz_items.json"
HAARETZ_ITEMS_CSV  = RAW_HAARETZ_DIR / "haaretz_items.csv"
HAARETZ_CANON_CSV  = CANON_HAARETZ_DIR / "haaretz_canonical.csv"
HAARETZ_CANON_JSON = CANON_HAARETZ_DIR / "haaretz_canonical.json"

# RSS URLs for each news source
YNET_RSS_URL     = "https://www.ynet.co.il/Integration/StoryRss2.xml"
HAYOM_RSS_URL    = "https://www.israelhayom.co.il/rss.xml"
HAARETZ_RSS_URL  = "https://www.haaretz.co.il/srv/htz---all-articles"


# Note: Instead of creating directories at import time,
# we now use etl.utils.timestamp_manager.ensure_directories() to create all
# necessary directories when needed
