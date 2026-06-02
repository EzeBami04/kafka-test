import pandas as pd
from kafka import KafkaConsumer
# from pyspark.sql import SparkSession

import json
import logging
#============= configurations ============
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
KAFKA_BROKER = 'kafka.dedamdata.org:9092'
TOPIC_NAME = 'orders_gadgets'

stream = KafkaConsumer( TOPIC_NAME, bootstrap_servers=KAFKA_BROKER, auto_offset_reset='earliest', 
                       enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x: json.loads(x.decode('utf-8')))


#======== Get message from broker ===============
def consumer_orders(message):
    """Consumes messages from Kafka and procesSses them."""
    order = message.value
    logging.info(f"Received order: {order}")
    return order



#========== load and transform  message into dataframe ==================
def process_order(order):
    """Process order on real time and transform into dataframe."""
    # Covert dictionary into dataframe
    df = pd.DataFrame([order])
    return df

