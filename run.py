"""
Pipeline entry point — called by GitHub Actions after market close.
Phase 1: fetch data  →  Phase 2: analyze  →  Phase 3: render HTML
"""
import os
import logging
from datetime import datetime
import pytz

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")


def main():
    from config import UNIVERSE, PRICE_SOURCE
    from data_fetch import fetch_and_save_prices, fetch_and_save_calendar
    from screener import run_screener
    from events import run_events
    from renderer import render_index, render_screener, render_events

    os.makedirs("docs", exist_ok=True)
    os.makedirs("data", exist_ok=True)

    # ── Phase 1: Fetch ────────────────────────────────────────────────────────
    logger.info(f"=== FETCH phase | universe={len(UNIVERSE)} tickers | source={PRICE_SOURCE} ===")
    fetch_and_save_prices(UNIVERSE)
    fetch_and_save_calendar(UNIVERSE)

    # ── Phase 2: Analyze ──────────────────────────────────────────────────────
    logger.info("=== ANALYZE phase ===")
    screener_data = run_screener()
    events_data   = run_events(screener_data.get("price_df"))

    # ── Phase 3: Render ───────────────────────────────────────────────────────
    logger.info("=== RENDER phase ===")
    pages = {
        "docs/index.html":    render_index(screener_data, events_data),
        "docs/screener.html": render_screener(screener_data),
        "docs/events.html":   render_events(events_data),
    }
    for path, html in pages.items():
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"  wrote {path}")

    logger.info(f"Done — {datetime.now(ET).strftime('%Y-%m-%d %H:%M ET')}")


if __name__ == "__main__":
    main()
