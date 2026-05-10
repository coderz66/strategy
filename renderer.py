import math
import pandas as pd
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape

from config import ET

_env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape(["html"]),
)


# ── Formatters (available inside all templates) ───────────────────────────────

def _fmt_pct(val, dp: int = 1) -> str:
    try:
        v = float(val)
        if math.isnan(v):
            return "—"
        sign = "+" if v >= 0 else ""
        return f"{sign}{v * 100:.{dp}f}%"
    except Exception:
        return "—"


def _fmt_price(val) -> str:
    try:
        return f"${float(val):,.2f}"
    except Exception:
        return "—"


def _fmt_rev(val) -> str:
    try:
        v = float(val)
        if v >= 1e9:
            return f"${v / 1e9:.1f}B"
        if v >= 1e6:
            return f"${v / 1e6:.0f}M"
        return f"${v:,.0f}"
    except Exception:
        return "—"


def _pct_class(val) -> str:
    try:
        return "pos" if float(val) >= 0 else "neg"
    except Exception:
        return ""


_env.globals.update(fmt_pct=_fmt_pct, fmt_price=_fmt_price,
                    fmt_rev=_fmt_rev, pct_class=_pct_class)


# ── Per-page renderers ────────────────────────────────────────────────────────

def _now_str() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")


def _df_rows(df: pd.DataFrame, n: int = None) -> list[dict]:
    if df is None or df.empty:
        return []
    sub = df.head(n) if n else df
    return sub.reset_index().to_dict("records")


def render_index(screener_data: dict, events_data: dict) -> str:
    return _env.get_template("index.html").render(
        generated_at=_now_str(),
        top_price=_df_rows(screener_data.get("price_momentum"), 10),
        top_fund=_df_rows(screener_data.get("fundamental"), 10),
        anomalies=events_data.get("price_anomalies", [])[:10],
        upcoming=events_data.get("upcoming", [])[:8],
    )


def render_screener(screener_data: dict) -> str:
    return _env.get_template("screener.html").render(
        generated_at=_now_str(),
        price_rows=_df_rows(screener_data.get("price_momentum")),
        fund_rows=_df_rows(screener_data.get("fundamental")),
    )


def render_events(events_data: dict) -> str:
    return _env.get_template("events.html").render(
        generated_at=_now_str(),
        anomalies=events_data.get("price_anomalies", []),
        recent=events_data.get("recent_earnings", []),
        upcoming=events_data.get("upcoming", []),
    )
