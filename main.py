import databento as db
import os
import json
import asyncio
import redis
from datetime import datetime, timezone
from aiohttp import web

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_KEY = os.getenv("DATABENTO_API_KEY")
if not API_KEY:
    print("ERROR: DATABENTO_API_KEY not set")
    exit(1)

PORT = int(os.getenv("PORT", "8080"))
REDIS_URL = os.getenv("REDIS_URL")

MAX_RETRIES = 10
INITIAL_BACKOFF = 2  # seconds

SYMBOLS = ["SPY.OPT", "QQQ.OPT", "IWM.OPT"]

# Undefined price sentinel used by Databento
UNDEF_PRICE = 2**63 - 1

# ---------------------------------------------------------------------------
# Redis connection
# ---------------------------------------------------------------------------
redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL)
        redis_client.ping()
        print(f"[{datetime.now(timezone.utc)}] Connected to Redis")
    except Exception as e:
        print(f"[{datetime.now(timezone.utc)}] WARNING: Redis connection failed: {e}")
        redis_client = None
else:
    print(f"[{datetime.now(timezone.utc)}] WARNING: REDIS_URL not set - records will be logged only, not published")

# ---------------------------------------------------------------------------
# Instrument cache: instrument_id -> parsed definition dict
# ---------------------------------------------------------------------------
instruments: dict[int, dict] = {}

# ---------------------------------------------------------------------------
# Price converter
# ---------------------------------------------------------------------------

def to_price(raw) -> float | None:
    """Convert a raw Databento fixed-point price to a float, or None."""
    if raw is None or raw == UNDEF_PRICE:
        return None
    return raw / 1e9


# ---------------------------------------------------------------------------
# OCC / OSI symbol parser
# ---------------------------------------------------------------------------

def parse_occ_symbol(raw_symbol: str) -> dict | None:
    """
    Parse an OCC/OSI option symbol like 'SPY   260213C00605000' into its
    component parts.

    Returns dict with: underlying, optionType, strike, expirationDate
    or None if parsing fails.
    """
    if not raw_symbol or len(raw_symbol) < 10:
        return None

    try:
        # Find where the date digits start (scan entire symbol for first digit)
        date_start = None
        for i, ch in enumerate(raw_symbol):
            if ch.isdigit():
                date_start = i
                break

        if date_start is None:
            return None

        underlying = raw_symbol[:date_start].strip()
        if not underlying:
            return None

        # Date is 6 digits YYMMDD starting at date_start
        date_str = raw_symbol[date_start:date_start + 6]
        if len(date_str) != 6 or not date_str.isdigit():
            return None

        yy = int(date_str[0:2])
        mm = int(date_str[2:4])
        dd = int(date_str[4:6])
        expiration_date = f"20{yy:02d}-{mm:02d}-{dd:02d}"

        # C or P immediately follows the date
        cp_char = raw_symbol[date_start + 6]
        if cp_char == "C":
            option_type = "call"
        elif cp_char == "P":
            option_type = "put"
        else:
            return None

        # Strike: remaining digits / 1000
        strike_str = raw_symbol[date_start + 7:]
        if not strike_str.isdigit():
            return None
        strike = int(strike_str) / 1000

        return {
            "underlying": underlying,
            "optionType": option_type,
            "strike": strike,
            "expirationDate": expiration_date,
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Redis publish helper
# ---------------------------------------------------------------------------

def publish(channel: str, payload: dict):
    """Publish a JSON payload to a Redis channel."""
    if redis_client is None:
        return
    try:
        redis_client.publish(channel, json.dumps(payload))
    except Exception as e:
        print(f"[{datetime.now(timezone.utc)}] Redis publish error on {channel}: {e}")


# ---------------------------------------------------------------------------
# Timestamp helper
# ---------------------------------------------------------------------------

def ts_to_iso(ts_ns) -> str | None:
    """Convert a nanosecond Unix timestamp to ISO 8601 string."""
    if ts_ns is None or ts_ns == 0:
        return None
    try:
        dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Enrich record with cached instrument definition
# ---------------------------------------------------------------------------

def enrich(instrument_id: int) -> dict:
    """Return cached definition fields for an instrument, or empty dict."""
    cached = instruments.get(instrument_id)
    if cached:
        return {
            "rawSymbol": cached.get("rawSymbol", ""),
            "underlying": cached.get("underlying", ""),
            "strike": cached.get("strike"),
            "optionType": cached.get("optionType", ""),
            "expirationDate": cached.get("expirationDate", ""),
        }
    return {
        "rawSymbol": "",
        "underlying": "",
        "strike": None,
        "optionType": "",
        "expirationDate": "",
    }


# ---------------------------------------------------------------------------
# Record callback
# ---------------------------------------------------------------------------

def on_record(record):
    """Handle each incoming Databento record."""
    try:
        # -- Instrument Definition --
        if isinstance(record, db.InstrumentDefMsg):
            raw_symbol = getattr(record, "raw_symbol", "") or ""
            parsed = parse_occ_symbol(raw_symbol)

            defn = {
                "rawSymbol": raw_symbol,
                "underlying": parsed["underlying"] if parsed else "",
                "strike": parsed["strike"] if parsed else None,
                "optionType": parsed["optionType"] if parsed else "",
                "expirationDate": parsed["expirationDate"] if parsed else "",
                "multiplier": to_price(getattr(record, "unit_of_measure_qty", None)) or 100,
                "minPriceIncrement": to_price(getattr(record, "min_price_increment", None)),
            }

            instruments[record.instrument_id] = defn

            payload = {
                "type": "definition",
                "instrumentId": record.instrument_id,
                **defn,
                "ts": ts_to_iso(getattr(record, "ts_event", None)),
            }
            publish("dbn:def", payload)
            return

        # -- Trade --
        if isinstance(record, db.TradeMsg):
            info = enrich(record.instrument_id)
            payload = {
                "type": "trade",
                "instrumentId": record.instrument_id,
                "price": to_price(record.price),
                "size": record.size,
                "side": getattr(record, "side", ""),
                "action": getattr(record, "action", ""),
                "publisherId": getattr(record, "publisher_id", None),
                "flags": getattr(record, "flags", 0),
                "sequence": getattr(record, "sequence", 0),
                **info,
                "multiplier": instruments.get(record.instrument_id, {}).get("multiplier", 100),
                "ts": ts_to_iso(getattr(record, "ts_event", None)),
            }
            publish("dbn:trade", payload)
            return

        # -- Statistics --
        if isinstance(record, db.StatMsg):
            info = enrich(record.instrument_id)
            payload = {
                "type": "statistic",
                "instrumentId": record.instrument_id,
                "statType": int(getattr(record, "stat_type", 0)),
                "quantity": getattr(record, "quantity", None),
                "price": to_price(getattr(record, "price", None)),
                **info,
                "ts": ts_to_iso(getattr(record, "ts_event", None)),
            }
            publish("dbn:stat", payload)
            return

        # -- Quotes (CBBOMsg / MBP1Msg — anything with .levels) --
        if hasattr(record, "levels") and record.levels:
            info = enrich(record.instrument_id)
            level = record.levels[0]
            payload = {
                "type": "quote",
                "instrumentId": record.instrument_id,
                "bidPx": to_price(level.bid_px),
                "askPx": to_price(level.ask_px),
                "bidSz": level.bid_sz,
                "askSz": level.ask_sz,
                "bidCt": getattr(level, "bid_ct", 0),
                "askCt": getattr(level, "ask_ct", 0),
                **info,
                "ts": ts_to_iso(getattr(record, "ts_event", None)),
            }
            publish("dbn:quote", payload)
            return

        # -- All other record types (SymbolMappingMsg, SystemMsg, etc.) --
        # Skip silently
        return

    except Exception as e:
        print(f"[{datetime.now(timezone.utc)}] on_record error: {e}")


# ---------------------------------------------------------------------------
# Databento client setup
# ---------------------------------------------------------------------------

def create_and_subscribe():
    """Create a new Live client and subscribe to feeds."""
    client = db.Live(key=API_KEY)

    for symbol in SYMBOLS:
        # Trades — live only (no start param)
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="trades",
            stype_in="parent",
            symbols=symbol,
        )

        # CBBO-1s quotes — live only (no start param)
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="cbbo-1s",
            stype_in="parent",
            symbols=symbol,
        )

        # Statistics — start=0 for session replay (accumulated OI)
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="statistics",
            stype_in="parent",
            symbols=symbol,
            start=0,
        )

        # Definitions — start=0 for session replay (full instrument universe)
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="definition",
            stype_in="parent",
            symbols=symbol,
            start=0,
        )

    client.add_callback(on_record)
    return client


# ---------------------------------------------------------------------------
# Health check server
# ---------------------------------------------------------------------------

async def health_check(request):
    return web.Response(text="ok")


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------

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
            print(f"[{datetime.now(timezone.utc)}] Subscribing to {SYMBOLS} across trades, cbbo-1s, statistics, definition")
            client = create_and_subscribe()
            print(f"[{datetime.now(timezone.utc)}] Connected. Starting stream...")
            client.start()
            await client.wait_for_close()
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
