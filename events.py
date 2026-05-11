"""
Events — pure analysis, no API calls.
Reads from data/ cache only.
"""
import pandas as pd
import numpy as np
import logging

from config import PRICE_ANOMALY_THRESHOLD
from cache import load_prices, load_fundamentals, load_calendar

logger = logging.getLogger(__name__)


def get_price_anomalies(price_df: pd.DataFrame = None) -> list[dict]:
    if price_df is None:
        price_df = load_prices()
    if price_df.empty or len(price_df) < 2:
        return []

    ret = (price_df.iloc[-1] - price_df.iloc[-2]) / price_df.iloc[-2]
    result = []
    for ticker in ret.index:
        v = ret[ticker]
        if pd.notna(v) and abs(v) >= PRICE_ANOMALY_THRESHOLD:
            result.append({
                "ticker":    ticker,
                "ret":       float(v),
                "price":     round(float(price_df.iloc[-1][ticker]), 2),
                "direction": "up" if v > 0 else "down",
            })
    return sorted(result, key=lambda x: abs(x["ret"]), reverse=True)


def get_earnings_events() -> dict:
    upcoming = load_calendar()

    fund = load_fundamentals()
    recent = []
    for ticker, data in fund.items():
        if ticker.startswith("_") or not isinstance(data, dict):
            continue
        actual   = data.get("eps_actual")
        estimate = data.get("eps_estimate")
        beat     = data.get("eps_beat")
        if actual is None or estimate is None or beat is None:
            continue
        recent.append({
            "ticker":       ticker,
            "eps_actual":   float(actual),
            "eps_estimate": float(estimate),
            "beat_pct":     float(beat),
            "result":       "beat" if beat >= 0 else "miss",
            "quarter":      str(data.get("eps_quarter", ""))[:10],
        })
    recent.sort(key=lambda x: abs(x["beat_pct"]), reverse=True)
    return {"upcoming": upcoming[:25], "recent_earnings": recent[:30]}


def run_events(price_df: pd.DataFrame = None) -> dict:
    anomalies = get_price_anomalies(price_df)
    earnings  = get_earnings_events()
    return {"price_anomalies": anomalies, **earnings}
