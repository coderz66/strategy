"""
Screener — pure analysis, no API calls.
Reads from data/ cache only.
"""
import pandas as pd
import numpy as np
import logging

from config import MOMENTUM_DISPLAY_N
from cache import load_prices, load_fundamentals

logger = logging.getLogger(__name__)


def compute_price_momentum(price_df: pd.DataFrame = None) -> pd.DataFrame:
    if price_df is None:
        price_df = load_prices()
    if price_df.empty:
        logger.warning("Price cache empty — run data_fetch.py first")
        return pd.DataFrame(columns=["price", "1w", "4w", "12w", "score"])

    rows = []
    for ticker in price_df.columns:
        s = price_df[ticker].dropna()
        if len(s) < 6:
            continue

        def pct(n):
            return float(s.iloc[-1] / s.iloc[-n] - 1) if len(s) >= n else np.nan

        rows.append({
            "ticker": ticker,
            "price":  round(float(s.iloc[-1]), 2),
            "1w":     pct(6),
            "4w":     pct(21),
            "12w":    pct(63),
        })

    if not rows:
        return pd.DataFrame(columns=["price", "1w", "4w", "12w", "score"])

    df = pd.DataFrame(rows).set_index("ticker")
    for col in ["1w", "4w", "12w"]:
        df[f"_{col}_r"] = df[col].rank(pct=True, na_option="bottom")
    df["score"] = df[["_1w_r", "_4w_r", "_12w_r"]].mean(axis=1)
    df.drop(columns=["_1w_r", "_4w_r", "_12w_r"], inplace=True)
    return df.sort_values("score", ascending=False)


def compute_fundamental_momentum() -> pd.DataFrame:
    fund = load_fundamentals()
    if not fund:
        logger.warning("Fundamental cache empty — run data_fetch.py first")
        return pd.DataFrame()

    rows = []
    for ticker, data in fund.items():
        if ticker.startswith("_"):
            continue
        if isinstance(data, dict) and data:
            rows.append({"ticker": ticker, **data})

    logger.info(f"Fundamental momentum: {len(rows)} tickers with cached data")
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("ticker")
    score_cols = []
    if "eps_beat" in df.columns:
        df["_eps_r"] = df["eps_beat"].rank(pct=True, na_option="bottom")
        score_cols.append("_eps_r")
    if "rev_qoq" in df.columns:
        df["_rev_r"] = df["rev_qoq"].rank(pct=True, na_option="bottom")
        score_cols.append("_rev_r")
    if score_cols:
        df["score"] = df[score_cols].mean(axis=1)
        df.drop(columns=score_cols, inplace=True)
        df.sort_values("score", ascending=False, inplace=True)
    return df


def run_screener() -> dict:
    price_df = load_prices()
    pm = compute_price_momentum(price_df)
    fm = compute_fundamental_momentum()
    return {"price_momentum": pm, "fundamental": fm, "price_df": price_df}
