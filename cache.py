import os
import json
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

DATA_DIR    = "data"
PRICES_CSV  = os.path.join(DATA_DIR, "prices.csv")
FUND_JSON   = os.path.join(DATA_DIR, "fundamentals.json")
FUND_STALE_DAYS = 7


# ── Price cache ───────────────────────────────────────────────────────────────

def load_price_cache() -> pd.DataFrame:
    if not os.path.exists(PRICES_CSV):
        return pd.DataFrame()
    try:
        df = pd.read_csv(PRICES_CSV, index_col=0, parse_dates=True)
        logger.info(f"Price cache: {len(df)} rows, last={df.index.max().date()}")
        return df
    except Exception as e:
        logger.warning(f"Price cache read error: {e}")
        return pd.DataFrame()


def save_price_cache(df: pd.DataFrame) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(PRICES_CSV)
    logger.info(f"Price cache saved: {len(df)} rows, last={df.index.max().date()}")


def merge_prices(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if old.empty:
        return new
    if new.empty:
        return old
    combined = pd.concat([old, new])
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined.sort_index()


# ── Fundamental cache ─────────────────────────────────────────────────────────

def load_fund_cache() -> dict:
    if not os.path.exists(FUND_JSON):
        return {}
    try:
        with open(FUND_JSON, "r") as f:
            data = json.load(f)
        meta = data.get("_meta", {})
        n = len([k for k in data if not k.startswith("_")])
        logger.info(f"Fundamental cache: {n} tickers, fetched={meta.get('fetched_at', '?')}")
        return data
    except Exception as e:
        logger.warning(f"Fundamental cache read error: {e}")
        return {}


def save_fund_cache(data: dict) -> None:
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
