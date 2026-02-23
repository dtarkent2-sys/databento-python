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

# Equity instrument mappings (for OHLCV bars): instrument_id -> ticker
_equity_instruments: dict[int, str] = {}

# Equity symbols to subscribe for OHLCV bars
EQUITY_SYMBOLS = ["SPY", "QQQ", "IWM"]

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

class DbnEncoder(json.JSONEncoder):
    """JSON encoder that handles Databento enum types (Side, Action, StatType, etc.)."""
    def default(self, obj):
        if hasattr(obj, 'value'):
            return obj.value
        if hasattr(obj, 'name'):
            return obj.name
        return super().default(obj)

def publish(channel: str, payload: dict):
    """Publish a JSON payload to a Redis channel."""
    if redis_client is None:
        return
    try:
        msg = json.dumps(payload, cls=DbnEncoder)
        redis_client.publish(channel, msg)
        # Persist definitions in a hash so late-joining subscribers can bootstrap
        if channel == "dbn:def" and "instrumentId" in payload:
            redis_client.hset("dbn:instruments", str(payload["instrumentId"]), msg)
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

_record_counts = {"trade": 0, "quote": 0, "definition": 0, "statistic": 0, "mapping": 0, "ohlcv": 0, "other": 0}
_total_records = 0
_LOG_INTERVAL = 50000  # Log stats every N records

def on_record(record):
    """Handle each incoming Databento record."""
    global _total_records
    try:
        _total_records += 1

        # Periodic stats
        if _total_records % _LOG_INTERVAL == 0:
            print(f"[{datetime.now(timezone.utc)}] Stats after {_total_records} records: {_record_counts} | instruments cached: {len(instruments)}")

        # -- SymbolMappingMsg: instrument definitions via parent symbol resolution --
        # When subscribing with stype_in="parent", Databento delivers instrument
        # info as SymbolMappingMsg (not InstrumentDefMsg). stype_out_symbol contains
        # the OCC option symbol which we parse for underlying/strike/expiry/type.
        if isinstance(record, db.SymbolMappingMsg):
            _record_counts["mapping"] += 1
            occ_symbol = getattr(record, "stype_out_symbol", "") or ""
            parsed = parse_occ_symbol(occ_symbol)

            # Equity mapping (no OCC parse) — just track instrument_id -> ticker
            if not parsed:
                in_sym = getattr(record, "stype_in_symbol", "") or ""
                ticker = occ_symbol.strip() or in_sym.strip()
                if ticker:
                    _equity_instruments[record.instrument_id] = ticker.upper()
                return

            defn = {
                "rawSymbol": occ_symbol,
                "underlying": parsed["underlying"] if parsed else "",
                "strike": parsed["strike"] if parsed else None,
                "optionType": parsed["optionType"] if parsed else "",
                "expirationDate": parsed["expirationDate"] if parsed else "",
                "multiplier": 100,  # Standard equity option multiplier
                "minPriceIncrement": None,
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

        # -- Instrument Definition (fallback — may arrive during session replay) --
        if isinstance(record, db.InstrumentDefMsg):
            _record_counts["definition"] += 1
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
            _record_counts["trade"] += 1
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
            _record_counts["statistic"] += 1
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

        # -- Quotes (CBBOMsg / MBP1Msg / CMBP1Msg / TBBOMsg — anything with .levels) --
        if hasattr(record, "levels") and record.levels:
            _record_counts["quote"] += 1
            info = enrich(record.instrument_id)
            level = record.levels[0]
            bid_px = to_price(level.bid_px)
            ask_px = to_price(level.ask_px)
            payload = {
                "type": "quote",
                "instrumentId": record.instrument_id,
                "bidPx": bid_px,
                "askPx": ask_px,
                "bidSz": level.bid_sz,
                "askSz": level.ask_sz,
                "bidCt": getattr(level, "bid_ct", 0),
                "askCt": getattr(level, "ask_ct", 0),
                **info,
                "ts": ts_to_iso(getattr(record, "ts_event", None)),
            }
            publish("dbn:quote", payload)

            # TCBBO / CMBP-1 records also carry a trade component
            # (price > 0 and size > 0 indicates a trade occurred)
            trade_price = to_price(getattr(record, "price", None))
            trade_size = getattr(record, "size", 0)
            if trade_price and trade_price > 0 and trade_size and trade_size > 0:
                trade_payload = {
                    "type": "trade",
                    "instrumentId": record.instrument_id,
                    "price": trade_price,
                    "size": trade_size,
                    "side": getattr(record, "side", ""),
                    "action": getattr(record, "action", ""),
                    "flags": getattr(record, "flags", 0),
                    "bidPx": bid_px,
                    "askPx": ask_px,
                    **info,
                    "multiplier": instruments.get(record.instrument_id, {}).get("multiplier", 100),
                    "ts": ts_to_iso(getattr(record, "ts_event", None)),
                }
                publish("dbn:trade", trade_payload)
            return

        # -- OHLCV bars (equity price candles) --
        if isinstance(record, db.OHLCVMsg):
            _record_counts["ohlcv"] = _record_counts.get("ohlcv", 0) + 1
            # For equity bars, instrument_id maps to a ticker via instruments cache
            # but equity symbols won't be in our OPRA instruments cache.
            # Use stype_in mapping: the raw_symbol on the record or resolve from instrument_id.
            ticker = ""
            cached = instruments.get(record.instrument_id)
            if cached:
                ticker = cached.get("underlying", "")
            # Equity OHLCV: map instrument_id to ticker via _equity_instruments
            if not ticker:
                ticker = _equity_instruments.get(record.instrument_id, "")

            payload = {
                "type": "ohlcv",
                "instrumentId": record.instrument_id,
                "ticker": ticker,
                "open": to_price(record.open),
                "high": to_price(record.high),
                "low": to_price(record.low),
                "close": to_price(record.close),
                "volume": record.volume,
                "ts": ts_to_iso(getattr(record, "ts_event", None)),
            }
            publish("dbn:ohlcv", payload)
            return

        # -- SystemMsg, ErrorMsg, etc. — skip --
        _record_counts["other"] += 1
        return

    except Exception as e:
        print(f"[{datetime.now(timezone.utc)}] on_record error: {e}")


# ---------------------------------------------------------------------------
# Databento client setup
# ---------------------------------------------------------------------------

def create_opra_client():
    """Create a Live client for OPRA options data."""
    client = db.Live(key=API_KEY)

    for symbol in SYMBOLS:
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="trades",
            stype_in="parent",
            symbols=symbol,
        )
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="cbbo-1s",
            stype_in="parent",
            symbols=symbol,
        )
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="cmbp-1",
            stype_in="parent",
            symbols=symbol,
        )
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="tcbbo",
            stype_in="parent",
            symbols=symbol,
        )
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="statistics",
            stype_in="parent",
            symbols=symbol,
            start=0,
        )
        client.subscribe(
            dataset="OPRA.PILLAR",
            schema="definition",
            stype_in="parent",
            symbols=symbol,
            start=0,
        )

    client.add_callback(on_record)
    return client


def create_equity_client():
    """Create a separate Live client for DBEQ equity OHLCV bars."""
    client = db.Live(key=API_KEY)

    for ticker in EQUITY_SYMBOLS:
        client.subscribe(
            dataset="DBEQ.BASIC",
            schema="ohlcv-1s",
            stype_in="raw_symbol",
            symbols=ticker,
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

    async def run_feed(name, create_fn):
        """Persistent reconnect loop for a single Databento feed."""
        consecutive_failures = 0
        while True:
            try:
                print(f"[{datetime.now(timezone.utc)}] [{name}] Connecting...")
                client = create_fn()
                client.start()
                consecutive_failures = 0
                print(f"[{datetime.now(timezone.utc)}] [{name}] Connected. Streaming...")
                await client.wait_for_close()
                print(f"[{datetime.now(timezone.utc)}] [{name}] Stream closed — reconnecting in 5s...")
                await asyncio.sleep(5)
            except db.common.error.BentoError as e:
                consecutive_failures += 1
                backoff = min(INITIAL_BACKOFF * (2 ** (consecutive_failures - 1)), 120)
                print(f"[{datetime.now(timezone.utc)}] [{name}] Error (attempt {consecutive_failures}): {e}")
                if consecutive_failures >= MAX_RETRIES:
                    print(f"[{datetime.now(timezone.utc)}] [{name}] {MAX_RETRIES} failures — waiting 5 min...")
                    await asyncio.sleep(300)
                    consecutive_failures = 0
                else:
                    await asyncio.sleep(backoff)
            except Exception as e:
                print(f"[{datetime.now(timezone.utc)}] [{name}] Unexpected error: {e} — reconnecting in 10s...")
                await asyncio.sleep(10)

    # Run OPRA feed only (DBEQ.BASIC requires separate live license we don't have)
    await run_feed("OPRA", create_opra_client)


if __name__ == "__main__":
    asyncio.run(run())
