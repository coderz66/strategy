import time
import yfinance as yf
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# yf.download() does NOT use curl_cffi and gets blocked by Yahoo on CI/datacenter IPs.
# Ticker.history() uses curl_cffi (when installed) and bypasses the consent wall.
# All price fetching uses the Ticker path below.

def _fetch_one(ticker: str, period: str) -> tuple[str, pd.Series | None]:
    try:
        hist = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=True)
        if hist.empty:
            return ticker, None
        return ticker, hist["Close"].rename(ticker)
    except Exception as e:
        logger.debug(f"{ticker} price fetch failed: {e}")
        return ticker, None


def fetch_price_history(tickers: list, period: str = "4mo") -> pd.DataFrame:
    """Daily Close prices via individual Ticker.history() calls (curl_cffi-aware)."""
    closes: dict[str, pd.Series] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_fetch_one, t, period): t for t in tickers}
        for f in as_completed(futures):
            ticker, series = f.result()
            if series is not None:
                closes[ticker] = series

    if not closes:
        logger.error("fetch_price_history: all tickers failed — Yahoo Finance may be blocking this IP")
        return pd.DataFrame()

    df = pd.DataFrame(closes).sort_index().dropna(how="all")
    logger.info(f"fetch_price_history: got {len(df.columns)}/{len(tickers)} tickers, {len(df)} rows")
    return df


def fetch_earnings_history(ticker: str) -> pd.DataFrame:
    try:
        return yf.Ticker(ticker).earnings_history or pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_quarterly_income(ticker: str) -> pd.DataFrame:
    try:
        return yf.Ticker(ticker).quarterly_income_stmt or pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_earnings_calendar(tickers: list) -> list[dict]:
    upcoming = []
    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            cal = t.calendar
            if cal is None:
                continue
            dates = cal.get("Earnings Date", [])
            if isinstance(dates, list) and dates:
                upcoming.append({"ticker": ticker, "date": str(dates[0])[:10]})
            elif hasattr(dates, "strftime"):
                upcoming.append({"ticker": ticker, "date": str(dates)[:10]})
        except Exception:
            continue
    return upcoming


def fetch_info(ticker: str) -> dict:
    try:
        return yf.Ticker(ticker).info or {}
    except Exception:
        return {}
