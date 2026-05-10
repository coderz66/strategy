import pandas as pd
import numpy as np
import logging

from config import NDX100, PRICE_ANOMALY_THRESHOLD
from data_fetch import fetch_price_history, fetch_earnings_calendar, fetch_earnings_history

logger = logging.getLogger(__name__)


def get_price_anomalies(price_df: pd.DataFrame, threshold: float = PRICE_ANOMALY_THRESHOLD) -> list[dict]:
    """Tickers whose latest daily move ≥ threshold."""
    if price_df.empty or len(price_df) < 2:
        return []
    ret = (price_df.iloc[-1] - price_df.iloc[-2]) / price_df.iloc[-2]
    result = []
    for ticker in ret.index:
        v = ret[ticker]
        if pd.notna(v) and abs(v) >= threshold:
            result.append({
                "ticker":    ticker,
                "ret":       float(v),
                "price":     round(float(price_df.iloc[-1][ticker]), 2),
                "direction": "up" if v > 0 else "down",
            })
    return sorted(result, key=lambda x: abs(x["ret"]), reverse=True)


def get_earnings_events(tickers: list = None) -> dict:
    tickers = tickers or NDX100

    logger.info("Events: fetching earnings calendar…")
    upcoming = fetch_earnings_calendar(tickers)

    logger.info("Events: fetching recent earnings results…")
    recent = []
    for ticker in tickers[:55]:   # rate-limit buffer
        try:
            eh = fetch_earnings_history(ticker)
            if eh is None or eh.empty:
                continue
            row = eh.iloc[0]
            actual   = row.get("epsActual",   np.nan)
            estimate = row.get("epsEstimate", np.nan)
            if pd.notna(actual) and pd.notna(estimate) and estimate != 0:
                beat = float((actual - estimate) / abs(estimate))
                recent.append({
                    "ticker":       ticker,
                    "eps_actual":   float(actual),
                    "eps_estimate": float(estimate),
                    "beat_pct":     beat,
                    "result":       "beat" if beat >= 0 else "miss",
                    "quarter":      str(row.get("quarter", ""))[:10],
                })
        except Exception:
            continue

    recent.sort(key=lambda x: abs(x["beat_pct"]), reverse=True)
    return {"upcoming": upcoming[:25], "recent_earnings": recent[:30]}


def run_events(price_df: pd.DataFrame = None) -> dict:
    logger.info("Events: detecting price anomalies…")
    if price_df is None or price_df.empty:
        price_df = fetch_price_history(NDX100, period="5d")
    anomalies = get_price_anomalies(price_df)
    earnings  = get_earnings_events()
    return {"price_anomalies": anomalies, **earnings}
