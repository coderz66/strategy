import os
import requests
import yfinance as yf
import pandas as pd
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

TIINGO_TOKEN = os.environ.get("TIINGO_API_KEY", "")


# ── Price data via Tiingo (free API key, works from CI) ───────────────────────

def _period_days(period: str) -> int:
    return {"1mo": 35, "3mo": 95, "4mo": 130, "6mo": 185, "1y": 370}.get(period, 130)


def _fetch_tiingo_ticker(ticker: str, start: str, end: str) -> tuple[str, pd.Series | None]:
    try:
        url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
        resp = requests.get(
            url,
            params={"startDate": start, "endDate": end, "token": TIINGO_TOKEN},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.debug(f"{ticker} Tiingo HTTP {resp.status_code}")
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


def fetch_price_history(tickers: list, period: str = "4mo") -> pd.DataFrame:
    """Daily adjusted Close prices via Tiingo (requires TIINGO_API_KEY env var)."""
    if not TIINGO_TOKEN:
        logger.error("fetch_price_history: TIINGO_API_KEY not set — add it as a GitHub Secret")
        return pd.DataFrame()

    end   = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=_period_days(period))).strftime("%Y-%m-%d")

    closes: dict[str, pd.Series] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_tiingo_ticker, t, start, end): t for t in tickers}
        for f in as_completed(futures):
            ticker, series = f.result()
            if series is not None and not series.empty:
                closes[ticker] = series

    if not closes:
        logger.error("fetch_price_history: no data returned from Tiingo")
        return pd.DataFrame()

    df = pd.DataFrame(closes).sort_index().dropna(how="all")
    logger.info(f"fetch_price_history: {len(df.columns)}/{len(tickers)} tickers, {len(df)} rows")
    return df


# ── Fundamental data via yfinance (individual calls, graceful fallback) ───────

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
