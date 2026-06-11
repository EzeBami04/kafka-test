import pandas as pd
from kafka import KafkaConsumer
from collections import deque
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = 'kafka.dedamdata.org:9092'
TOPIC_NAME   = 'orders_gadgets'
GROUP_ID     = 'orders-consumer-group'
BATCH_SIZE   = 100


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


def _flush(buf: deque) -> pd.DataFrame:
    df = pd.DataFrame(list(buf))
    df['order_date'] = pd.to_datetime(df['order_date'])
    logging.info(f"Batch ready — {len(df)} rows.")
    return df


def run() -> pd.DataFrame:
    """Consume one batch from Kafka and return as a DataFrame."""
    stream = _build_consumer()
    buffer = deque()

    try:
        for message in stream:
            buffer.append(message.value)
            logging.info(f"Consumed order_id: {message.value.get('order_id')}")

            if len(buffer) >= BATCH_SIZE:
                break
    finally:
        stream.close()

    if not buffer:
        raise RuntimeError("No messages consumed — topic may be empty or offset already at end.")

    return _flush(buffer)