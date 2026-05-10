import pandas as pd
import numpy as np
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import NDX100, MOMENTUM_DISPLAY_N, FUNDAMENTAL_FETCH_N
from data_fetch import fetch_price_history, fetch_earnings_history, fetch_quarterly_income

logger = logging.getLogger(__name__)


# ── Price Momentum ────────────────────────────────────────────────────────────

def compute_price_momentum(price_df: pd.DataFrame) -> pd.DataFrame:
    """1w / 4w / 12w returns + composite percentile rank for each ticker."""
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
        logger.warning("compute_price_momentum: no data rows — price_df may be empty")
        return pd.DataFrame(columns=["price", "1w", "4w", "12w", "score"])

    df = pd.DataFrame(rows).set_index("ticker")
    for col in ["1w", "4w", "12w"]:
        df[f"_{col}_rank"] = df[col].rank(pct=True, na_option="bottom")
    df["score"] = df[["_1w_rank", "_4w_rank", "_12w_rank"]].mean(axis=1)
    df = df.drop(columns=["_1w_rank", "_4w_rank", "_12w_rank"])
    return df.sort_values("score", ascending=False)


# ── Fundamental Momentum ──────────────────────────────────────────────────────

def _fundamental_row(ticker: str) -> dict | None:
    row = {"ticker": ticker}
    try:
        eh = fetch_earnings_history(ticker)
        if eh is not None and not eh.empty:
            latest = eh.iloc[0]
            actual   = latest.get("epsActual", np.nan)
            estimate = latest.get("epsEstimate", np.nan)
            if pd.notna(actual) and pd.notna(estimate) and estimate != 0:
                row["eps_beat"] = float((actual - estimate) / abs(estimate))
                row["eps_actual"]   = float(actual)
                row["eps_estimate"] = float(estimate)
    except Exception:
        pass

    try:
        inc = fetch_quarterly_income(ticker)
        if inc is not None and not inc.empty and "Total Revenue" in inc.index:
            rev = inc.loc["Total Revenue"].dropna()
            if len(rev) >= 2:
                row["rev_qoq"] = float((rev.iloc[0] - rev.iloc[1]) / abs(rev.iloc[1]))
                row["rev_latest"] = float(rev.iloc[0])
    except Exception:
        pass

    return row if len(row) > 1 else None


def compute_fundamental_momentum(tickers: list = None) -> pd.DataFrame:
    tickers = (tickers or NDX100)[:FUNDAMENTAL_FETCH_N]
    rows = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(_fundamental_row, t): t for t in tickers}
        for f in as_completed(futures):
            r = f.result()
            if r:
                rows.append(r)

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
        logger.error("Screener: price_df is empty — Yahoo Finance may be rate-limiting. Returning empty results.")
        empty = pd.DataFrame(columns=["price", "1w", "4w", "12w", "score"])
        return {"price_momentum": empty, "fundamental": pd.DataFrame(), "price_df": price_df}

    logger.info("Screener: computing price momentum…")
    pm = compute_price_momentum(price_df)

    logger.info("Screener: computing fundamental momentum…")
    fm = compute_fundamental_momentum()

    return {
        "price_momentum": pm,
        "fundamental":    fm,
        "price_df":       price_df,
    }
