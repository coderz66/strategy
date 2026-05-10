import yfinance as yf
import pandas as pd
import pandas_datareader.data as web
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


# ── Price data via stooq (works from CI; Yahoo Finance blocks GitHub Actions IPs) ──

def _period_days(period: str) -> int:
    return {"1mo": 35, "3mo": 95, "4mo": 130, "6mo": 185, "1y": 370}.get(period, 130)


def _fetch_stooq(ticker: str, start: datetime, end: datetime) -> tuple[str, pd.Series | None]:
    try:
        df = web.DataReader(ticker, "stooq", start, end)
        if df.empty:
            return ticker, None
        return ticker, df["Close"].rename(ticker).sort_index()
    except Exception as e:
        logger.debug(f"{ticker} stooq failed: {e}")
        return ticker, None


def fetch_price_history(tickers: list, period: str = "4mo") -> pd.DataFrame:
    """Daily Close prices via stooq (CI-safe, no API key required)."""
    end   = datetime.today()
    start = end - timedelta(days=_period_days(period))

    closes: dict[str, pd.Series] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_stooq, t, start, end): t for t in tickers}
        for f in as_completed(futures):
            ticker, series = f.result()
            if series is not None:
                closes[ticker] = series

    if not closes:
        logger.error("fetch_price_history: no data from stooq — check network or ticker list")
        return pd.DataFrame()

    df = pd.DataFrame(closes).sort_index().dropna(how="all")
    logger.info(f"fetch_price_history: {len(df.columns)}/{len(tickers)} tickers, {len(df)} rows")
    return df


# ── Fundamental data via yfinance (individual Ticker calls, lighter than bulk) ──

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
            t   = yf.Ticker(ticker)
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
