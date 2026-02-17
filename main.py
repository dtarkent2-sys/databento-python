import databento as db
import os
import asyncio
from datetime import datetime, timezone

API_KEY = os.getenv("DATABENTO_API_KEY")
if not API_KEY:
    print("ERROR: DATABENTO_API_KEY not set")
    exit(1)

client = db.Live(key=API_KEY)

print(f"[{datetime.now(timezone.utc)}] Initializing Databento Live client...")

# Try "OPRA" or "OPRA.PILLAR" – test variations
client.subscribe(
    dataset="OPRA",  # <-- Try this first; if fails, "OPRA.PILLAR"
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

async def run():
    print(f"[{datetime.now(timezone.utc)}] Starting stream...")
    await client.start()

if __name__ == "__main__":
    asyncio.run(run())