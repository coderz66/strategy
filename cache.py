"""
Read/write helpers for data/ cache files.
No API calls here — pure I/O.
"""
import os
import json
import logging
import pandas as pd
from datetime import datetime

from config import FUND_STALE_DAYS

logger = logging.getLogger(__name__)

DATA_DIR     = "data"
PRICES_CSV   = os.path.join(DATA_DIR, "prices.csv")
FUND_JSON    = os.path.join(DATA_DIR, "fundamentals.json")
CALENDAR_JSON = os.path.join(DATA_DIR, "calendar.json")


# ── Price cache ───────────────────────────────────────────────────────────────

def load_prices() -> pd.DataFrame:
    if not os.path.exists(PRICES_CSV):
        return pd.DataFrame()
    try:
        df = pd.read_csv(PRICES_CSV, index_col=0, parse_dates=True)
        logger.info(f"Price cache: {len(df)} rows × {len(df.columns)} tickers, last={df.index.max().date()}")
        return df
    except Exception as e:
        logger.warning(f"Price cache read error: {e}")
        return pd.DataFrame()


def save_prices(df: pd.DataFrame) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(PRICES_CSV)
    logger.info(f"Price cache saved: {len(df)} rows × {len(df.columns)} tickers")


def merge_prices(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if old.empty:
        return new
    if new.empty:
        return old
    combined = pd.concat([old, new])
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined.sort_index()


# ── Fundamental cache ─────────────────────────────────────────────────────────

def load_fundamentals() -> dict:
    if not os.path.exists(FUND_JSON):
        return {}
    try:
        with open(FUND_JSON) as f:
            data = json.load(f)
        n = len([k for k in data if not k.startswith("_")])
        fetched = data.get("_meta", {}).get("fetched_at", "?")
        logger.info(f"Fundamental cache: {n} tickers, fetched={fetched}")
        return data
    except Exception as e:
        logger.warning(f"Fundamental cache read error: {e}")
        return {}


def save_fundamentals(data: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    data["_meta"] = {"fetched_at": datetime.utcnow().strftime("%Y-%m-%d")}
    with open(FUND_JSON, "w") as f:
        json.dump(data, f, indent=2)
    n = len([k for k in data if not k.startswith("_")])
    logger.info(f"Fundamental cache saved: {n} tickers")


def is_fund_stale(cache: dict) -> bool:
    fetched_at = cache.get("_meta", {}).get("fetched_at")
    if not fetched_at:
        return True
    last = datetime.strptime(fetched_at, "%Y-%m-%d")
    return (datetime.utcnow() - last).days >= FUND_STALE_DAYS


# ── Earnings calendar cache ───────────────────────────────────────────────────

def load_calendar() -> list:
    if not os.path.exists(CALENDAR_JSON):
        return []
    try:
        with open(CALENDAR_JSON) as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Calendar cache read error: {e}")
        return []


def save_calendar(events: list) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CALENDAR_JSON, "w") as f:
        json.dump(events, f, indent=2)
    logger.info(f"Calendar cache saved: {len(events)} events")
