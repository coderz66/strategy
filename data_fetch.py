import os
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed

from cache import (
    load_price_cache, save_price_cache, merge_prices,
    load_fund_cache, save_fund_cache, is_fund_stale,
)

logger = logging.getLogger(__name__)

TIINGO_TOKEN = os.environ.get("TIINGO_API_KEY", "")
FMP_TOKEN    = os.environ.get("FMP_API_KEY", "")
FMP_BASE     = "https://financialmodelingprep.com/api/v3"


# ── Tiingo price fetch ────────────────────────────────────────────────────────

def _fetch_tiingo_ticker(ticker: str, start: str, end: str) -> tuple[str, pd.Series | None]:
    try:
        url  = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
        resp = requests.get(
            url,
            params={"startDate": start, "endDate": end, "token": TIINGO_TOKEN},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"{ticker} Tiingo HTTP {resp.status_code}: {resp.text[:120]}")
            return ticker, None
        data = resp.json()
        if not isinstance(data, list) or not data:
            return ticker, None
        series = pd.Series(
            {pd.Timestamp(d["date"][:10]): d.get("adjClose") or d.get("close") for d in data},
            name=ticker,
        ).dropna()
        return ticker, series
    except Exception as e:
        logger.debug(f"{ticker} Tiingo: {e}")
        return ticker, None


def _tiingo_fetch_range(tickers: list, start: str, end: str) -> pd.DataFrame:
    closes: dict[str, pd.Series] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_tiingo_ticker, t, start, end): t for t in tickers}
        for f in as_completed(futures):
            ticker, series = f.result()
            if series is not None and not series.empty:
                closes[ticker] = series
    if not closes:
        return pd.DataFrame()
    return pd.DataFrame(closes).sort_index().dropna(how="all")


def fetch_price_history(tickers: list, period: str = "4mo") -> pd.DataFrame:
    """Return daily adj-close prices, using cache where possible."""
    cached = load_price_cache()

    today     = datetime.today().date()
    last_bday = today - timedelta(days=1)   # conservative: yesterday's close

    if not cached.empty:
        last_cached = cached.index.max().date()
        if last_cached >= last_bday:
            logger.info("Price cache is current — skipping Tiingo fetch")
            return cached
        fetch_start = (cached.index.max() + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        days        = {"1mo": 35, "3mo": 95, "4mo": 130, "6mo": 185, "1y": 370}.get(period, 130)
        fetch_start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")

    if not TIINGO_TOKEN:
        logger.error("TIINGO_API_KEY not set")
        return cached

    fetch_end = today.strftime("%Y-%m-%d")
    logger.info(f"Tiingo fetch: {fetch_start} → {fetch_end} for {len(tickers)} tickers")
    new_df = _tiingo_fetch_range(tickers, fetch_start, fetch_end)

    if new_df.empty:
        logger.warning("Tiingo returned no new data — using cache")
        return cached

    merged = merge_prices(cached, new_df)
    save_price_cache(merged)
    logger.info(f"Price history: {len(merged.columns)}/{len(tickers)} tickers, {len(merged)} rows")
    return merged


# ── FMP helpers ───────────────────────────────────────────────────────────────

def _fmp_get(url: str, params: dict) -> list | dict | None:
    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code != 200:
        logger.warning(f"FMP HTTP {resp.status_code}: {url} — {resp.text[:200]}")
        return None
    data = resp.json()
    if isinstance(data, dict) and ("Error Message" in data or "message" in data):
        logger.warning(f"FMP error: {data}")
        return None
    return data


def fetch_earnings_history(ticker: str) -> pd.DataFrame:
    if not FMP_TOKEN:
        return pd.DataFrame()
    try:
        data = _fmp_get(f"{FMP_BASE}/earnings-surprises/{ticker}", {"apikey": FMP_TOKEN})
        if not isinstance(data, list) or not data:
            return pd.DataFrame()
        rows = [
            {
                "epsActual":   d.get("actualEarningResult"),
                "epsEstimate": d.get("estimatedEarning"),
                "quarter":     d.get("date", "")[:10],
            }
            for d in data
            if d.get("actualEarningResult") is not None
        ]
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        logger.debug(f"fetch_earnings_history {ticker}: {e}")
        return pd.DataFrame()


def fetch_quarterly_income(ticker: str) -> pd.DataFrame:
    if not FMP_TOKEN:
        return pd.DataFrame()
    try:
        data = _fmp_get(
            f"{FMP_BASE}/income-statement/{ticker}",
            {"period": "quarter", "limit": 4, "apikey": FMP_TOKEN},
        )
        if not isinstance(data, list) or not data:
            return pd.DataFrame()
        rev = {d["date"]: d.get("revenue") for d in data if d.get("revenue")}
        if not rev:
            return pd.DataFrame()
        series = pd.Series(rev, name="Total Revenue")
        return pd.DataFrame([series])
    except Exception as e:
        logger.debug(f"fetch_quarterly_income {ticker}: {e}")
        return pd.DataFrame()


def fetch_earnings_calendar(tickers: list) -> list[dict]:
    if not FMP_TOKEN:
        return []
    try:
        today  = date.today().isoformat()
        future = (date.today() + timedelta(days=30)).isoformat()
        data   = _fmp_get(
            f"{FMP_BASE}/earning_calendar",
            {"from": today, "to": future, "apikey": FMP_TOKEN},
        )
        if not isinstance(data, list):
            return []
        ticker_set = set(tickers)
        return [
            {"ticker": d["symbol"], "date": d.get("date", "")[:10]}
            for d in data
            if d.get("symbol") in ticker_set
        ]
    except Exception:
        return []


# ── Fundamental data with cache ───────────────────────────────────────────────

import numpy as np

def _fetch_one_fundamental(ticker: str) -> dict | None:
    row = {"ticker": ticker}
    try:
        eh = fetch_earnings_history(ticker)
        if eh is not None and not eh.empty:
            latest   = eh.iloc[0]
            actual   = latest.get("epsActual", np.nan)
            estimate = latest.get("epsEstimate", np.nan)
            if pd.notna(actual) and pd.notna(estimate) and estimate != 0:
                row["eps_beat"]     = float((actual - estimate) / abs(estimate))
                row["eps_actual"]   = float(actual)
                row["eps_estimate"] = float(estimate)
    except Exception:
        pass
    try:
        inc = fetch_quarterly_income(ticker)
        if inc is not None and not inc.empty and "Total Revenue" in inc.index:
            rev = inc.loc["Total Revenue"].dropna()
            if len(rev) >= 2:
                row["rev_qoq"]    = float((rev.iloc[0] - rev.iloc[1]) / abs(rev.iloc[1]))
                row["rev_latest"] = float(rev.iloc[0])
    except Exception:
        pass
    return row if len(row) > 1 else None


def fetch_all_fundamentals(tickers: list) -> dict:
    """
    Fetch fundamental data for all tickers.
    Returns dict keyed by ticker with eps_beat, rev_qoq, etc.
    Loads from cache if fresh; fetches and saves if stale.
    """
    cache = load_fund_cache()
    if cache and not is_fund_stale(cache):
        logger.info("Fundamental cache is current — skipping FMP fetch")
        return cache

    logger.info(f"Fetching fundamentals from FMP for {len(tickers)} tickers…")
    result: dict = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_fetch_one_fundamental, t): t for t in tickers}
        for f in as_completed(futures):
            row = f.result()
            if row:
                ticker = row.pop("ticker")
                result[ticker] = row

    logger.info(f"Fundamentals fetched: {len(result)}/{len(tickers)} tickers with data")
    if result:
        save_fund_cache(result)
    elif cache:
        logger.warning("FMP returned no data — keeping existing cache")
        return cache
    return result
