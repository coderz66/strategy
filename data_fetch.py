"""
Data fetcher — the ONLY file that makes external API calls.
Two price modes:
  api  → Tiingo (incremental, requires TIINGO_API_KEY)
  csv  → user-supplied data/user_prices.csv (no API call)
  auto → csv if file exists, else api

Run standalone:  python data_fetch.py
"""
import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import UNIVERSE, PRICE_SOURCE, USER_CSV_PATH, PRICE_FETCH_PERIOD
from cache import load_prices, save_prices, merge_prices, save_calendar

logger = logging.getLogger(__name__)

TIINGO_TOKEN = os.environ.get("TIINGO_API_KEY", "")
FMP_TOKEN    = os.environ.get("FMP_API_KEY", "")
FMP_BASE     = "https://financialmodelingprep.com/api/v3"


# ── Mode 2: user CSV ──────────────────────────────────────────────────────────

def _read_user_csv(path: str) -> pd.DataFrame:
    """
    Read user-uploaded CSV into a date-indexed, ticker-columned DataFrame.
    Accepts any format where the first column (or index) is dates and the
    remaining columns are ticker symbols with closing prices.
    """
    raw = pd.read_csv(path, header=0)

    # detect date column: try first column, then the index
    date_col = None
    for col in list(raw.columns[:2]):
        try:
            pd.to_datetime(raw[col].dropna().iloc[:3])
            date_col = col
            break
        except Exception:
            pass

    if date_col:
        raw = raw.set_index(date_col)
    raw.index = pd.to_datetime(raw.index, infer_datetime_format=True)
    raw.index.name = "date"

    # coerce all columns to numeric (prices), drop non-numeric
    df = raw.apply(pd.to_numeric, errors="coerce").dropna(how="all", axis=1)
    df = df.sort_index().dropna(how="all")
    return df


def _load_from_user_csv() -> None:
    """Read user_prices.csv, normalize, and save to data/prices.csv."""
    if not os.path.exists(USER_CSV_PATH):
        logger.error(f"User CSV not found: {USER_CSV_PATH}")
        return
    logger.info(f"Price source: user CSV ({USER_CSV_PATH})")
    df = _read_user_csv(USER_CSV_PATH)
    if df.empty:
        logger.error("User CSV parsed to empty DataFrame — check format")
        return
    save_prices(df)
    logger.info(f"User CSV loaded: {len(df)} rows × {len(df.columns)} tickers "
                f"({df.index.min().date()} → {df.index.max().date()})")


# ── Mode 1: Tiingo API ────────────────────────────────────────────────────────

def _tiingo_ticker(ticker: str, start: str, end: str) -> tuple[str, pd.Series | None]:
    try:
        resp = requests.get(
            f"https://api.tiingo.com/tiingo/daily/{ticker}/prices",
            params={"startDate": start, "endDate": end, "token": TIINGO_TOKEN},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"Tiingo {ticker} HTTP {resp.status_code}: {resp.text[:100]}")
            return ticker, None
        data = resp.json()
        if not isinstance(data, list) or not data:
            return ticker, None
        s = pd.Series(
            {pd.Timestamp(d["date"][:10]): d.get("adjClose") or d.get("close") for d in data},
            name=ticker,
        ).dropna()
        return ticker, s
    except Exception as e:
        logger.debug(f"Tiingo {ticker}: {e}")
        return ticker, None


def _load_from_api(tickers: list, period: str) -> None:
    """Fetch from Tiingo incrementally and save to data/prices.csv."""
    cached = load_prices()
    today  = datetime.today().date()

    if not cached.empty:
        last = cached.index.max().date()
        if last >= today - timedelta(days=1):
            logger.info("Prices already current — skipping Tiingo fetch")
            return
        start = (cached.index.max() + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        days  = {"1mo": 35, "3mo": 95, "4mo": 130, "6mo": 185, "1y": 370}.get(period, 130)
        start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")

    if not TIINGO_TOKEN:
        logger.error("TIINGO_API_KEY not set")
        return

    end = today.strftime("%Y-%m-%d")
    logger.info(f"Tiingo fetch: {start} → {end} for {len(tickers)} tickers")

    closes: dict[str, pd.Series] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_tiingo_ticker, t, start, end): t for t in tickers}
        for f in as_completed(futures):
            ticker, s = f.result()
            if s is not None and not s.empty:
                closes[ticker] = s

    if not closes:
        logger.warning("Tiingo returned no new data — cache unchanged")
        return

    new_df = pd.DataFrame(closes).sort_index().dropna(how="all")
    merged = merge_prices(cached, new_df)
    save_prices(merged)
    logger.info(f"Prices updated: {len(new_df.columns)}/{len(tickers)} tickers, {len(new_df)} new rows")


# ── Public entry points ───────────────────────────────────────────────────────

def fetch_and_save_prices(tickers: list = None, period: str = PRICE_FETCH_PERIOD) -> None:
    """Fetch price history and save to data/prices.csv (API or user CSV)."""
    tickers = tickers or UNIVERSE

    source = PRICE_SOURCE
    if source == "auto":
        source = "csv" if os.path.exists(USER_CSV_PATH) else "api"

    if source == "csv":
        _load_from_user_csv()
    else:
        _load_from_api(tickers, period)


def fetch_and_save_calendar(tickers: list = None) -> None:
    """Fetch upcoming earnings dates from FMP and save to data/calendar.json."""
    tickers = tickers or UNIVERSE
    if not FMP_TOKEN:
        logger.warning("FMP_API_KEY not set — skipping calendar fetch")
        return

    today  = date.today().isoformat()
    future = (date.today() + timedelta(days=30)).isoformat()
    try:
        resp = requests.get(
            f"{FMP_BASE}/earning_calendar",
            params={"from": today, "to": future, "apikey": FMP_TOKEN},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"FMP calendar HTTP {resp.status_code}: {resp.text[:120]}")
            return
        data = resp.json()
        if not isinstance(data, list):
            return
        ticker_set = set(tickers)
        events = [
            {"ticker": d["symbol"], "date": str(d.get("date", ""))[:10]}
            for d in data if d.get("symbol") in ticker_set
        ]
        save_calendar(events)
    except Exception as e:
        logger.warning(f"Calendar fetch error: {e}")


# ── Standalone ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
    tickers = sys.argv[1:] or UNIVERSE
    logger.info(f"Fetching prices for {len(tickers)} tickers | source={PRICE_SOURCE}")
    fetch_and_save_prices(tickers)
    fetch_and_save_calendar(tickers)
    logger.info("Done.")
