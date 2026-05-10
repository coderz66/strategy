import yfinance as yf
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def fetch_price_history(tickers: list, period: str = "4mo") -> pd.DataFrame:
    """Daily Close prices for given tickers. Returns DataFrame[ticker → price series]."""
    try:
        raw = yf.download(tickers, period=period, interval="1d", auto_adjust=True, progress=False)
        close = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
        return close.dropna(how="all")
    except Exception as e:
        logger.error(f"fetch_price_history: {e}")
        return pd.DataFrame()


def fetch_earnings_history(ticker: str) -> pd.DataFrame:
    """EPS actual vs estimate history for one ticker."""
    try:
        return yf.Ticker(ticker).earnings_history or pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_quarterly_income(ticker: str) -> pd.DataFrame:
    """Quarterly income statement (rows = line items, cols = quarters)."""
    try:
        return yf.Ticker(ticker).quarterly_income_stmt or pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_earnings_calendar(tickers: list) -> list[dict]:
    """Tickers with an upcoming earnings date in their yfinance calendar."""
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
