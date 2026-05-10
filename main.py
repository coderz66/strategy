import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from config import ET, SCHEDULER_HOUR, SCHEDULER_MINUTE
from screener import run_screener
from events import run_events
from renderer import render_index, render_screener, render_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

_LOADING = "<html><body style='font-family:sans-serif;padding:60px;color:#1B2635'><h2>Initializing — first data pull in progress…</h2><p>Refresh in ~60 seconds.</p></body></html>"

cache: dict = {
    "index":    _LOADING,
    "screener": _LOADING,
    "events":   _LOADING,
    "updated":  None,
    "lock":     None,   # set in lifespan
}


async def _refresh():
    if cache["lock"].locked():
        logger.info("Refresh already running, skipping.")
        return
    async with cache["lock"]:
        logger.info("Daily refresh starting…")
        loop = asyncio.get_event_loop()
        try:
            screener_data = await loop.run_in_executor(None, run_screener)
            price_df = screener_data.get("price_df")
            events_data   = await loop.run_in_executor(None, lambda: run_events(price_df))

            cache["index"]    = render_index(screener_data, events_data)
            cache["screener"] = render_screener(screener_data)
            cache["events"]   = render_events(events_data)
            cache["updated"]  = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
            logger.info(f"Refresh complete — {cache['updated']}")
        except Exception as e:
            logger.error(f"Refresh failed: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache["lock"] = asyncio.Lock()

    # Initial pull on startup (non-blocking)
    asyncio.create_task(_refresh())

    scheduler = AsyncIOScheduler(timezone="America/New_York")
    scheduler.add_job(_refresh, "cron", hour=SCHEDULER_HOUR, minute=SCHEDULER_MINUTE)
    scheduler.start()
    logger.info(f"Scheduler armed: daily refresh at {SCHEDULER_HOUR:02d}:{SCHEDULER_MINUTE:02d} ET")

    yield
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(cache["index"])


@app.get("/screener", response_class=HTMLResponse)
async def screener():
    return HTMLResponse(cache["screener"])


@app.get("/events", response_class=HTMLResponse)
async def events():
    return HTMLResponse(cache["events"])


@app.post("/refresh")
async def manual_refresh():
    """Trigger an out-of-schedule data refresh (e.g. after a market holiday)."""
    asyncio.create_task(_refresh())
    return {"status": "refresh triggered", "note": "check back in ~90 seconds"}


@app.get("/health")
async def health():
    return {"status": "ok", "last_updated": cache["updated"]}
