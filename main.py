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

client = db.Live(key=API_KEY)

print(f"[{datetime.now(timezone.utc)}] Initializing Databento Live client...")

client.subscribe(
    dataset="OPRA",
    schema="definition",
    stype_in="parent",
    symbols="SPY.OPT",
)

client.subscribe(
    dataset="OPRA",
    schema="equity-open-interest",
    stype_in="parent",
    symbols="SPY.OPT",
)


def on_record(record):
    print(f"[{datetime.now(timezone.utc)}] Record: {record}")


client.add_callback(on_record)


async def health_check(request):
    return web.Response(text="ok")


async def run():
    # Start health check server so Railway knows the service is alive
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[{datetime.now(timezone.utc)}] Health check server listening on port {PORT}")

    # Start the Databento live stream
    print(f"[{datetime.now(timezone.utc)}] Starting stream...")
    await client.start()


if __name__ == "__main__":
    asyncio.run(run())
