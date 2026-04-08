"""
=================================================================
Kafka Producer — Real-Time Stock Data Ingestion (Upgraded)
=================================================================
Architecture:
  - 4 worker processes, each responsible for a subset of stocks
  - A shared multiprocessing.Manager().Queue() buffers all records
  - A dedicated KafkaSender process drains the queue → Kafka
  - GBM simulation when market is closed (labeled data_type="simulated")
  - 40+ stocks across 6 sectors for Big Data Volume/Variety demonstration
  - Load balancing metrics logged to data/logs/lb_metrics.jsonl

Run on Mac terminal (from project root, venv activated):
  python producer/producer.py
=================================================================
"""

import json
import math
import os
import random
import time
import logging
import multiprocessing
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewPartitions, NewTopic
from kafka.errors import TopicAlreadyExistsError

# =================================================================
# CONFIGURATION
# =================================================================
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock-data"
FETCH_INTERVAL = 5          # seconds between fetch cycles per worker
N_WORKERS = 4               # number of parallel fetcher processes
QUEUE_WARN_THRESHOLD = 1000  # warn if queue depth exceeds this
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "logs")

# GBM parameters
GBM_MU = 0.0       # zero drift (short-term simulation)
GBM_SIGMA = 0.015  # ~1.5% daily volatility
# dt: fraction of a trading year represented by one FETCH_INTERVAL tick
GBM_DT = FETCH_INTERVAL / (252 * 6.5 * 3600)

# =================================================================
# STOCK UNIVERSE — 40+ stocks across 6 sectors
# =================================================================
STOCKS = {
    "Technology": [
        "AAPL", "MSFT", "GOOGL", "NVDA", "META",
        "TSLA", "AMD", "INTC", "ORCL", "CRM",
        "ADBE", "QCOM", "NFLX", "UBER", "SNOW"
    ],
    "Finance": [
        "JPM", "BAC", "GS", "MS", "WFC",
        "C", "BLK", "AXP", "V", "MA"
    ],
    "Healthcare": [
        "JNJ", "PFE", "UNH", "ABBV",
        "MRK", "LLY", "BMY", "AMGN"
    ],
    "Energy": [
        "XOM", "CVX", "COP",
        "SLB", "EOG", "PXD"
    ],
    "Consumer": [
        "AMZN", "WMT", "HD",
        "MCD", "NKE", "SBUX", "COST"
    ],
    "ETFs": [
        "SPY", "QQQ", "IWM", "DIA"
    ]
}

# Flat list of all symbols
ALL_SYMBOLS = [s for sector in STOCKS.values() for s in sector]

# Reverse lookup: symbol → sector
SECTOR_MAP = {
    symbol: sector
    for sector, symbols in STOCKS.items()
    for symbol in symbols
}


def assign_stocks_to_workers(symbols: list, n_workers: int) -> list:
    """Split symbols across n_workers using round-robin interleaving."""
    return [symbols[i::n_workers] for i in range(n_workers)]


# =================================================================
# LOGGING SETUP
# =================================================================
def setup_logging():
    """Configure logging for the main process."""
    os.makedirs(LOG_DIR, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(processName)s] %(levelname)s — %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(LOG_DIR, "producer.log")),
            logging.StreamHandler(),
        ],
    )


def log_lb_metrics(worker_id: int, symbols_count: int, messages_sent: int, queue_depth: int):
    """Append one JSON line to lb_metrics.jsonl for load-balancing telemetry."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "worker_id": worker_id,
        "symbols_count": symbols_count,
        "messages_sent": messages_sent,
        "queue_depth": queue_depth,
    }
    lb_path = os.path.join(LOG_DIR, "lb_metrics.jsonl")
    try:
        with open(lb_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass  # never crash the producer over logging


# =================================================================
# GBM SIMULATION
# =================================================================
def gbm_next_price(last_price: float) -> float:
    """
    Simulate one GBM step from last_price.
    Uses: S_new = S * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
    where Z ~ N(0,1).
    """
    z = random.gauss(0, 1)
    log_return = (GBM_MU - 0.5 * GBM_SIGMA ** 2) * GBM_DT + GBM_SIGMA * math.sqrt(GBM_DT) * z
    return round(last_price * math.exp(log_return), 2)


def simulate_ohlcv(close_price: float, base_volume: int) -> dict:
    """
    Generate a realistic simulated OHLCV bar from a simulated close price.
    Open is slightly random around close, high/low bracket the range.
    Volume is jittered ±20% from base_volume.
    """
    spread = close_price * 0.003  # 0.3% spread
    open_price = round(close_price + random.uniform(-spread, spread), 2)
    high_price = round(max(open_price, close_price) + random.uniform(0, spread), 2)
    low_price = round(min(open_price, close_price) - random.uniform(0, spread), 2)
    volume = max(1000, int(base_volume * random.uniform(0.8, 1.2)))
    return {
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
    }


# =================================================================
# DATA FETCHER
# =================================================================
def fetch_stock_data(symbol: str, shared_prices: dict, shared_volumes: dict) -> dict | None:
    """
    Fetch the latest 1-minute candle for a stock using yfinance.
    Falls back to GBM simulation if market is closed or data unavailable.
    Always uses actual market timestamp (or last known) — never datetime.now() alone.

    Args:
        symbol: Stock ticker symbol (e.g. "AAPL")
        shared_prices: Manager dict for last known close prices across workers
        shared_volumes: Manager dict for last known volumes across workers

    Returns:
        A dict with all fields ready to send to Kafka, or None on hard error.
    """
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")

        if not data.empty:
            # Real market data available
            latest = data.iloc[-1]
            close = round(float(latest["Close"]), 2)
            volume = int(latest["Volume"])

            # Save to shared state for GBM fallback
            shared_prices[symbol] = close
            shared_volumes[symbol] = volume if volume > 0 else shared_volumes.get(symbol, 100000)

            # Use the actual candle timestamp (market event time)
            ts = data.index[-1]
            if hasattr(ts, "to_pydatetime"):
                ts = ts.to_pydatetime()
            timestamp_str = ts.strftime("%Y-%m-%d %H:%M:%S")

            return {
                "symbol": symbol,
                "sector": SECTOR_MAP[symbol],
                "open": round(float(latest["Open"]), 2),
                "high": round(float(latest["High"]), 2),
                "low": round(float(latest["Low"]), 2),
                "close": close,
                "volume": volume,
                "timestamp": timestamp_str,
                "kafka_produce_time": int(time.time() * 1000),  # ms epoch for latency tracking
                "data_type": "real",
            }

        # Market closed or no data — simulate with GBM
        last_price = shared_prices.get(symbol)
        if last_price is None:
            # Bootstrap: fetch last close from a longer period
            hist = ticker.history(period="5d", interval="1d")
            if not hist.empty:
                last_price = round(float(hist["Close"].iloc[-1]), 2)
                shared_prices[symbol] = last_price
                shared_volumes[symbol] = int(hist["Volume"].iloc[-1]) or 100000
            else:
                return None  # cannot bootstrap at all

        sim_close = gbm_next_price(last_price)
        shared_prices[symbol] = sim_close
        base_vol = shared_volumes.get(symbol, 100000)
        ohlcv = simulate_ohlcv(sim_close, base_vol)

        return {
            "symbol": symbol,
            "sector": SECTOR_MAP[symbol],
            **ohlcv,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "kafka_produce_time": int(time.time() * 1000),
            "data_type": "simulated",
        }

    except Exception as e:
        logging.warning(f"[Worker] Error fetching {symbol}: {e}")
        return None


# =================================================================
# WORKER PROCESS — fetches a subset of stocks, pushes to queue
# =================================================================
def worker_process(
    worker_id: int,
    symbols: list,
    queue: multiprocessing.Queue,
    shared_prices: dict,
    shared_volumes: dict,
    stop_event: multiprocessing.Event,
):
    """
    Fetcher worker: continuously fetches data for assigned symbols and
    puts records into the shared queue for the KafkaSender to consume.

    Args:
        worker_id: Numeric ID for this worker (0-3)
        symbols: Subset of stock symbols this worker is responsible for
        queue: Shared multiprocessing queue (producer→sender buffer)
        shared_prices: Shared dict of last known close prices (for GBM)
        shared_volumes: Shared dict of last known volumes
        stop_event: Signal to shut down gracefully
    """
    logging.info(f"[Worker {worker_id}] Started — responsible for: {symbols}")
    messages_this_cycle = 0

    while not stop_event.is_set():
        cycle_start = time.time()
        messages_this_cycle: int = 0

        for symbol in symbols:
            if stop_event.is_set():
                break
            record = fetch_stock_data(symbol, shared_prices, shared_volumes)
            if record:
                queue.put(record)
                messages_this_cycle = messages_this_cycle + 1

        # Log load-balancing metrics
        q_depth = queue.qsize()
        log_lb_metrics(worker_id, len(symbols), messages_this_cycle, q_depth)

        if q_depth > QUEUE_WARN_THRESHOLD:
            logging.warning(
                f"[Worker {worker_id}] Queue depth {q_depth} exceeds threshold "
                f"{QUEUE_WARN_THRESHOLD} — sender may be falling behind!"
            )

        # Sleep remaining time in the cycle
        elapsed = time.time() - cycle_start
        sleep_time = max(0, FETCH_INTERVAL - elapsed)
        time.sleep(sleep_time)

    logging.info(f"[Worker {worker_id}] Stopped.")


# =================================================================
# KAFKA SENDER PROCESS — drains queue, sends to Kafka
# =================================================================
def kafka_sender_process(
    queue: multiprocessing.Queue,
    stop_event: multiprocessing.Event,
):
    """
    Dedicated sender process: KafkaProducer is created HERE (not in main process
    or worker processes — KafkaProducer is not multiprocessing-safe to share).
    Drains the shared queue and sends each record to the Kafka topic.

    Args:
        queue: Shared multiprocessing queue (reads from here)
        stop_event: Signal to flush and shut down
    """
    # Create producer INSIDE this process — never share across processes
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",             # wait for all replicas to acknowledge
        retries=3,
        linger_ms=50,           # batch records for 50ms before sending
    )
    logging.info(f"[KafkaSender] Connected to {KAFKA_BROKER}, publishing to '{KAFKA_TOPIC}'")

    total_sent = 0

    while not stop_event.is_set() or not queue.empty():
        # Drain all available records from the queue in one shot
        batch = []
        try:
            while True:
                record = queue.get_nowait()
                batch.append(record)
        except Exception:
            pass  # queue is empty

        for record in batch:
            try:
                producer.send(KAFKA_TOPIC, value=record)
                total_sent += 1
                if total_sent % 50 == 0:
                    logging.info(
                        f"[KafkaSender] Total sent: {total_sent} | "
                        f"Latest: {record['symbol']} @ ${record['close']} "
                        f"[{record['data_type']}]"
                    )
            except Exception as e:
                logging.error(f"[KafkaSender] Failed to send {record.get('symbol')}: {e}")

        if batch:
            producer.flush()

        time.sleep(0.1)  # short sleep to avoid spinning

    producer.flush()
    producer.close()
    logging.info(f"[KafkaSender] Stopped. Total messages sent: {total_sent}")


# =================================================================
# KAFKA TOPIC SETUP — ensure 4 partitions exist
# =================================================================
def ensure_topic_partitions():
    """
    Use KafkaAdminClient to create the topic with 4 partitions if it doesn't
    exist, or update partition count to 4 if topic already exists with fewer.
    Safe to call repeatedly — idempotent.
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

        # Try to create the topic
        try:
            admin.create_topics([
                NewTopic(
                    name=KAFKA_TOPIC,
                    num_partitions=4,
                    replication_factor=1,
                )
            ])
            logging.info(f"[Setup] Created topic '{KAFKA_TOPIC}' with 4 partitions")
        except TopicAlreadyExistsError:
            # Topic exists — check current partition count and increase if needed
            meta = admin.describe_topics([KAFKA_TOPIC])
            current_partitions = len(meta[0]["partitions"]) if meta else 0
            if current_partitions < 4:
                admin.create_partitions({KAFKA_TOPIC: NewPartitions(total_count=4)})
                logging.info(
                    f"[Setup] Increased '{KAFKA_TOPIC}' partitions: {current_partitions} → 4"
                )
            else:
                logging.info(
                    f"[Setup] Topic '{KAFKA_TOPIC}' already has {current_partitions} partitions — OK"
                )

        admin.close()
    except Exception as e:
        logging.warning(f"[Setup] Could not configure topic partitions: {e} (continuing anyway)")


# =================================================================
# MAIN — orchestrate all processes
# =================================================================
def main():
    """
    Entry point: sets up shared state, splits stock universe across N_WORKERS,
    starts worker fetcher processes and one KafkaSender process.
    Blocks until KeyboardInterrupt.
    """
    setup_logging()
    os.makedirs(LOG_DIR, exist_ok=True)

    logging.info("=" * 60)
    logging.info(" Real-Time Stock Market Producer — Starting Up")
    logging.info("=" * 60)
    logging.info(f" Stocks: {len(ALL_SYMBOLS)} symbols across {len(STOCKS)} sectors")
    logging.info(f" Workers: {N_WORKERS} | Fetch interval: {FETCH_INTERVAL}s")
    logging.info(f" Kafka: {KAFKA_BROKER} → topic: {KAFKA_TOPIC}")
    logging.info("=" * 60)

    # Wait briefly then ensure Kafka topic has 4 partitions
    time.sleep(2)
    ensure_topic_partitions()

    # Shared state — Manager creates cross-process dict/queue
    manager = multiprocessing.Manager()
    shared_prices = manager.dict()    # symbol → last known close price
    shared_volumes = manager.dict()   # symbol → last known volume
    queue = manager.Queue()           # records buffer: workers → sender
    stop_event = manager.Event()

    # Split symbols across workers using round-robin (i::n_workers)
    # so every worker gets ~equal load and ALL symbols are always fetched
    symbol_chunks = assign_stocks_to_workers(ALL_SYMBOLS, N_WORKERS)

    # Spawn worker fetcher processes
    worker_procs = []
    for i, chunk in enumerate(symbol_chunks):
        p = multiprocessing.Process(
            target=worker_process,
            args=(i, chunk, queue, shared_prices, shared_volumes, stop_event),
            name=f"Worker-{i}",
            daemon=True,
        )
        p.start()
        worker_procs.append(p)
        logging.info(f"[Main] Worker-{i} started (PID {p.pid}): {chunk}")

    # Spawn the dedicated Kafka sender process
    sender = multiprocessing.Process(
        target=kafka_sender_process,
        args=(queue, stop_event),
        name="KafkaSender",
        daemon=True,
    )
    sender.start()
    logging.info(f"[Main] KafkaSender started (PID {sender.pid})")

    # Block main process until Ctrl+C
    try:
        while True:
            # Print a heartbeat every 30 seconds showing active worker count
            alive = sum(1 for p in worker_procs if p.is_alive())
            logging.info(
                f"[Main] Heartbeat — Active workers: {alive}/{N_WORKERS} | "
                f"Queue depth: {queue.qsize()}"
            )
            time.sleep(30)
    except KeyboardInterrupt:
        logging.info("\n[Main] Shutdown signal received — stopping all processes...")
        stop_event.set()

        for p in worker_procs:
            p.join(timeout=5)
        sender.join(timeout=10)

        logging.info("[Main] All processes stopped. Producer shut down cleanly.")


if __name__ == "__main__":
    main()
