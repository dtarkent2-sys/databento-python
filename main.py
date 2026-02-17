import databento as db
import os
import asyncio
from datetime import datetime, timezone
from aiohttp import web

API_KEY = os.getenv("DATABENTO_API_KEY")
if not API_KEY:
    print("ERROR: DATABENTO_API_KEY not set")
    exit(1)

PORT = int(os.getenv("PORT", "8080"))

MAX_RETRIES = 10
INITIAL_BACKOFF = 2  # seconds


def on_record(record):
    print(f"[{datetime.now(timezone.utc)}] Record: {record}")


def create_and_subscribe():
    """Create a new Live client and subscribe to feeds."""
    client = db.Live(key=API_KEY)
    client.subscribe(
        dataset="OPRA.PILLAR",
        schema="definition",
        stype_in="parent",
        symbols="SPY.OPT",
    )
    client.subscribe(
        dataset="OPRA.PILLAR",
        schema="statistics",
        stype_in="parent",
        symbols="SPY.OPT",
    )
    client.add_callback(on_record)
    return client


async def health_check(request):
    return web.Response(text="ok")


async def run():
    # Start health check server first so Railway knows the service is alive
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[{datetime.now(timezone.utc)}] Health check server listening on port {PORT}")

    # Connect to Databento with retry logic
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[{datetime.now(timezone.utc)}] Connecting to Databento (attempt {attempt}/{MAX_RETRIES})...")
            client = create_and_subscribe()
            print(f"[{datetime.now(timezone.utc)}] Connected. Starting stream...")
            await client.start()
            return  # clean exit
        except db.common.error.BentoError as e:
            print(f"[{datetime.now(timezone.utc)}] Connection failed: {e}")
            if attempt < MAX_RETRIES:
                backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
                print(f"[{datetime.now(timezone.utc)}] Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
            else:
                print(f"[{datetime.now(timezone.utc)}] Max retries reached. Exiting.")
                raise


if __name__ == "__main__":
    asyncio.run(run())
