"""
Data fetcher — the ONLY file that makes external API calls.
Each function fetches from one source and saves to data/.
Run standalone:  python data_fetch.py
"""
import os
import sys
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import UNIVERSE, PRICE_FETCH_PERIOD
from cache import (
    load_prices, save_prices, merge_prices,
    load_fundamentals, save_fundamentals, is_fund_stale,
    load_calendar, save_calendar,
)

logger = logging.getLogger(__name__)

TIINGO_TOKEN = os.environ.get("TIINGO_API_KEY", "")
FMP_TOKEN    = os.environ.get("FMP_API_KEY", "")
FMP_BASE     = "https://financialmodelingprep.com/api/v3"


# ── Tiingo: price history ─────────────────────────────────────────────────────

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


def fetch_and_save_prices(tickers: list = None, period: str = PRICE_FETCH_PERIOD) -> None:
    """Fetch Tiingo prices incrementally and save to data/prices.csv."""
    tickers = tickers or UNIVERSE
    cached  = load_prices()
    today   = datetime.today().date()

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
        logger.error("TIINGO_API_KEY not set — cannot fetch prices")
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


# ── FMP helpers ───────────────────────────────────────────────────────────────

def _fmp_get(path: str, params: dict) -> list | dict | None:
    url  = f"{FMP_BASE}/{path}"
    resp = requests.get(url, params={**params, "apikey": FMP_TOKEN}, timeout=10)
    if resp.status_code != 200:
        logger.warning(f"FMP HTTP {resp.status_code}: {path} — {resp.text[:200]}")
        return None
    data = resp.json()
    if isinstance(data, dict) and ("Error Message" in data or "message" in data):
        logger.warning(f"FMP error ({path}): {data}")
        return None
    return data


# ── FMP: fundamental data ─────────────────────────────────────────────────────

def _fetch_one_fundamental(ticker: str) -> dict | None:
    result = {}
    # EPS beat
    data = _fmp_get(f"earnings-surprises/{ticker}", {})
    if isinstance(data, list) and data:
        d = data[0]
        actual   = d.get("actualEarningResult")
        estimate = d.get("estimatedEarning")
        if actual is not None and estimate and estimate != 0:
            result["eps_beat"]     = float((actual - estimate) / abs(estimate))
            result["eps_actual"]   = float(actual)
            result["eps_estimate"] = float(estimate)
            result["eps_quarter"]  = str(d.get("date", ""))[:10]

    # Revenue QoQ
    data = _fmp_get(f"income-statement/{ticker}", {"period": "quarter", "limit": 4})
    if isinstance(data, list) and len(data) >= 2:
        rev0 = data[0].get("revenue")
        rev1 = data[1].get("revenue")
        if rev0 and rev1 and rev1 != 0:
            result["rev_qoq"]    = float((rev0 - rev1) / abs(rev1))
            result["rev_latest"] = float(rev0)

    return result if result else None


def fetch_and_save_fundamentals(tickers: list = None) -> None:
    """Fetch FMP fundamentals (EPS beat + revenue QoQ) and save to data/fundamentals.json."""
    tickers = tickers or UNIVERSE
    cached  = load_fundamentals()

    if cached and not is_fund_stale(cached):
        logger.info("Fundamentals already current — skipping FMP fetch")
        return

    if not FMP_TOKEN:
        logger.error("FMP_API_KEY not set — cannot fetch fundamentals")
        return

    logger.info(f"FMP fundamentals fetch for {len(tickers)} tickers…")
    result: dict = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(_fetch_one_fundamental, t): t for t in tickers}
        for f in as_completed(futures):
            ticker = futures[f]
            data   = f.result()
            if data:
                result[ticker] = data

    logger.info(f"Fundamentals: {len(result)}/{len(tickers)} tickers returned data")

    if result:
        save_fundamentals(result)
    elif cached:
        logger.warning("FMP returned nothing — keeping existing cache")
    else:
        logger.warning("FMP returned nothing and no cache exists")


# ── FMP: earnings calendar ────────────────────────────────────────────────────

def fetch_and_save_calendar(tickers: list = None) -> None:
    """Fetch upcoming earnings dates and save to data/calendar.json."""
    tickers = tickers or UNIVERSE
    if not FMP_TOKEN:
        logger.error("FMP_API_KEY not set — cannot fetch calendar")
        return

    today  = date.today().isoformat()
    future = (date.today() + timedelta(days=30)).isoformat()
    data   = _fmp_get("earning_calendar", {"from": today, "to": future})

    if not isinstance(data, list):
        logger.warning("Calendar fetch returned no data")
        return

    ticker_set = set(tickers)
    events = [
        {"ticker": d["symbol"], "date": str(d.get("date", ""))[:10]}
        for d in data
        if d.get("symbol") in ticker_set
    ]
    save_calendar(events)


# ── Standalone entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
    tickers = sys.argv[1:] or UNIVERSE
    logger.info(f"Fetching data for {len(tickers)} tickers: {tickers}")
    fetch_and_save_prices(tickers)
    fetch_and_save_fundamentals(tickers)
    fetch_and_save_calendar(tickers)
    logger.info("Done.")
