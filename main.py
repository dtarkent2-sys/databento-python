# main.py - Simple live OPRA streamer for SPY options (run by Railway)
import databento as db
import os
import asyncio
from datetime import datetime

API_KEY = os.getenv("DATABENTO_API_KEY")
if not API_KEY:
    print("ERROR: DATABENTO_API_KEY not set in environment variables")
    exit(1)

print(f"[{datetime.utcnow()}] Initializing Databento Live client...")

client = db.Live(key=API_KEY)

# Subscribe to SPY options chain
client.subscribe(
    dataset="OPRA.PILLAR",
    schema="definition",              # strikes, expiries, instrument info
    stype_in="parent",
    symbols="SPY.OPT",                # all SPY options under parent symbol
)

client.subscribe(
    dataset="OPRA.PILLAR",
    schema="equity-open-interest",    # live open interest updates
    stype_in="parent",
    symbols="SPY.OPT",
)

# Optional: add quotes/NBBO for IV/greeks (uncomment when ready)
# client.subscribe(
#     dataset="OPRA.PILLAR",
#     schema="cmbp-1",
#     stype_in="parent",
#     symbols="SPY.OPT",
# )

def on_record(record):
    print(f"[{datetime.utcnow()}] Record received: {record}")
    # TODO: Parse record → update chain dict → compute GEX/vanna/charm/etc.
    # Later: publish to Redis or print JSON for Node dashboard

client.add_callback(on_record)

async def run():
    print(f"[{datetime.utcnow()}] Starting live subscription to OPRA.PILLAR for SPY...")
    try:
        await client.start()
    except Exception as e:
        print(f"Stream error: {e}")
        # Add retry logic here if needed

if __name__ == "__main__":
    asyncio.run(run())