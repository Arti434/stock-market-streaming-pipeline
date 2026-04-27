import json
import time
import logging
from datetime import datetime,timezone
from google.cloud import pubsub_v1
import yfinance as yf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "stock-stream-pipeline-001"
TOPIC_ID   = "stock-tick-data"
SYMBOLS    = ["GOOGL", "MSFT", "AAPL", "AMZN", "TSLA"]
INTERVAL   = 5


def fetch_stock_data(symbol: str) -> dict:
    try:
        ticker = yf.Ticker(symbol)
        info   = ticker.fast_info
        return {
            "symbol"    : symbol,
            "price"     : round(float(info.last_price or 0), 2),
            "volume"    : int(info.three_month_average_volume or 0),
            "change_pct": round(float(
                ((info.last_price - info.previous_close) / info.previous_close * 100)
                if info.previous_close else 0
            ), 4),
            "market_cap": round(float(info.market_cap or 0), 2),
           "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return None


def publish_message(publisher, topic_path: str, data: dict) -> None:
    message_bytes = json.dumps(data).encode("utf-8")
    future = publisher.publish(
        topic_path,
        message_bytes,
        symbol=data["symbol"]
    )
    message_id = future.result()
    logger.info(f"Published {data['symbol']} @ ${data['price']} | ID: {message_id}")


def run(rounds: int = 10):
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    logger.info(f"Starting producer — topic: {topic_path}")
    logger.info(f"Stocks: {SYMBOLS}")
    logger.info(f"Interval: {INTERVAL}s | Rounds: {rounds}")

    for round_num in range(1, rounds + 1):
        logger.info(f"\n--- Round {round_num}/{rounds} ---")
        for symbol in SYMBOLS:
            data = fetch_stock_data(symbol)
            if data:
                publish_message(publisher, topic_path, data)
        if round_num < rounds:
            logger.info(f"Waiting {INTERVAL} seconds...")
            time.sleep(INTERVAL)

    logger.info(f"Producer complete! Published {rounds * len(SYMBOLS)} messages")


if __name__ == "__main__":
    run(rounds=10)