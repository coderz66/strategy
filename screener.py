import pandas as pd
import numpy as np
import logging

from config import NDX100, MOMENTUM_DISPLAY_N, FUNDAMENTAL_FETCH_N
from data_fetch import fetch_price_history, fetch_all_fundamentals

logger = logging.getLogger(__name__)


# ── Price Momentum ────────────────────────────────────────────────────────────

def compute_price_momentum(price_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker in price_df.columns:
        s = price_df[ticker].dropna()
        if len(s) < 6:
            continue

        def pct(n):
            return float((s.iloc[-1] / s.iloc[-n] - 1)) if len(s) >= n else np.nan

        rows.append({
            "ticker": ticker,
            "price":  round(float(s.iloc[-1]), 2),
            "1w":     pct(6),
            "4w":     pct(21),
            "12w":    pct(63),
        })

    if not rows:
        logger.warning("compute_price_momentum: no data rows")
        return pd.DataFrame(columns=["price", "1w", "4w", "12w", "score"])

    df = pd.DataFrame(rows).set_index("ticker")
    for col in ["1w", "4w", "12w"]:
        df[f"_{col}_rank"] = df[col].rank(pct=True, na_option="bottom")
    df["score"] = df[["_1w_rank", "_4w_rank", "_12w_rank"]].mean(axis=1)
    df = df.drop(columns=["_1w_rank", "_4w_rank", "_12w_rank"])
    return df.sort_values("score", ascending=False)


# ── Fundamental Momentum ──────────────────────────────────────────────────────

def compute_fundamental_momentum(tickers: list = None) -> pd.DataFrame:
    tickers = (tickers or NDX100)[:FUNDAMENTAL_FETCH_N]
    fund    = fetch_all_fundamentals(tickers)

    rows = []
    for ticker in tickers:
        data = fund.get(ticker)
        if data and isinstance(data, dict):
            rows.append({"ticker": ticker, **data})

    logger.info(f"compute_fundamental_momentum: {len(rows)} tickers with data out of {len(tickers)}")
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
        df = df.drop(columns=score_cols)
        df = df.sort_values("score", ascending=False)
    return df


# ── Entry point ───────────────────────────────────────────────────────────────

def run_screener() -> dict:
    logger.info("Screener: fetching price history…")
    price_df = fetch_price_history(NDX100, period="4mo")

    if price_df.empty:
        logger.warning("Screener: price_df is empty — skipping price momentum")
        pm = pd.DataFrame(columns=["price", "1w", "4w", "12w", "score"])
    else:
        logger.info("Screener: computing price momentum…")
        pm = compute_price_momentum(price_df)

    logger.info("Screener: computing fundamental momentum…")
    fm = compute_fundamental_momentum()

    return {
        "price_momentum": pm,
        "fundamental":    fm,
        "price_df":       price_df,
    }
