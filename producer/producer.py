"""
=================================================================
Kafka Producer — Real-Time Stock Data Ingestion
=================================================================
What this does:
  1. Fetches live stock prices for 5 companies using Yahoo Finance
  2. Converts each data point to JSON
  3. Sends (produces) each JSON message to a Kafka topic "stock-data"
  4. Repeats every 10 seconds in an infinite loop

Run on Mac terminal:
  cd /Users/akshatsingh/Desktop/BDAProject
  python producer/producer.py
=================================================================
"""

import json
import time
from datetime import datetime

import yfinance as yf
from kafka import KafkaProducer

# ----- Configuration -----
KAFKA_BROKER = "localhost:9092"       # Kafka is exposed on port 9092 via Docker
KAFKA_TOPIC = "stock-data"           # topic name our pipeline uses
STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
FETCH_INTERVAL = 10                  # seconds between each fetch cycle


def create_producer():
    """
    Create a KafkaProducer instance.
    - bootstrap_servers: address of the Kafka broker
    - value_serializer: converts Python dict → JSON bytes before sending
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print(f"[✓] Connected to Kafka broker at {KAFKA_BROKER}")
    return producer


def fetch_stock_data(symbol):
    """
    Use yfinance to fetch the latest 1-minute candle for a stock symbol.
    Returns a dict with: symbol, open, high, low, close, volume, timestamp
    Returns None if the fetch fails (market closed, network error, etc.)
    """
    try:
        ticker = yf.Ticker(symbol)
        # period="1d", interval="1m" gives today's 1-minute candles
        data = ticker.history(period="1d", interval="1m")

        if data.empty:
            print(f"[!] No data for {symbol} — market may be closed")
            return None

        # Take the most recent row (latest candle)
        latest = data.iloc[-1]

        record = {
            "symbol": symbol,
            "open": round(float(latest["Open"]), 2),
            "high": round(float(latest["High"]), 2),
            "low": round(float(latest["Low"]), 2),
            "close": round(float(latest["Close"]), 2),
            "volume": int(latest["Volume"]),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return record

    except Exception as e:
        print(f"[✗] Error fetching {symbol}: {e}")
        return None


def run_producer():
    """
    Main loop:
      - Creates Kafka producer
      - Every FETCH_INTERVAL seconds, fetches data for all symbols
      - Sends each record to the Kafka topic
      - Prints confirmation for each message sent
    """
    producer = create_producer()
    print(f"[▶] Starting producer — fetching {STOCK_SYMBOLS} every {FETCH_INTERVAL}s")
    print(f"[▶] Sending to topic: '{KAFKA_TOPIC}'")
    print("-" * 60)

    message_count = 0

    try:
        while True:
            for symbol in STOCK_SYMBOLS:
                record = fetch_stock_data(symbol)

                if record:
                    # Send the record to Kafka
                    producer.send(KAFKA_TOPIC, value=record)
                    message_count += 1
                    print(
                        f"[→] Sent #{message_count}: {record['symbol']} | "
                        f"Close: ${record['close']} | "
                        f"Vol: {record['volume']:,} | "
                        f"Time: {record['timestamp']}"
                    )

            # Ensure all buffered messages are actually sent to Kafka
            producer.flush()
            print(f"--- Cycle complete. Waiting {FETCH_INTERVAL}s ---\n")
            time.sleep(FETCH_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n[■] Producer stopped. Total messages sent: {message_count}")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
