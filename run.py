"""
Daily pipeline entry point.
Called by GitHub Actions after market close; generates docs/*.html.
"""
import os
import logging
from datetime import datetime
import pytz

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")


def main():
    from screener import run_screener
    from events import run_events
    from renderer import render_index, render_screener, render_events

    os.makedirs("docs", exist_ok=True)

    logger.info("Running screener…")
    screener_data = run_screener()

    logger.info("Running events pipeline…")
    events_data = run_events(screener_data.get("price_df"))

    logger.info("Rendering HTML…")
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
