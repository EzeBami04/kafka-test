from faker import Faker
import logging
from kafka import KafkaProducer
import json
import time
import os
import csv
import tempfile
from dotenv import load_dotenv

# ========== Config =============
load_dotenv()
fake = Faker()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = 'kafka.dedamdata.org:9092'
TOPIC_NAME = 'orders_gadgets'

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER, enable_idempotence=True, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# ====== Generate Order records ============

def generate_single_order():
    """Generates one unique order record."""
    quantity = fake.random_int(min=1, max=10)
    price = round(fake.random_number(digits=5) / 100, 2)

    return {
        'order_date':     fake.date_time_this_year().isoformat(),
        'order_id':       fake.uuid4(),
        'customer_id':    fake.uuid4(),
        'customer_name':  fake.name(),
        'customer_email':     fake.email(),
        'customer_phone':     fake.phone_number(),
        'customer_segment':   fake.random_element(['retail', 'wholesale', 'vip', 'new']),
        'is_returning_customer': fake.boolean(chance_of_getting_true=70),
        'item_ordered':   fake.random_element(elements=['Laptop', 'Smartphone', 'Headphones', 'Camera', 'Smartwatch']),
        'quantity':       quantity,
        'price':          price,
        'total_amount':   round(quantity * price, 2),
        'product_id':         fake.bothify(text='PROD-####??').upper(),
        'category':           fake.random_element(['Electronics', 'Accessories', 'Wearables']),
        'brand':              fake.random_element(['Apple', 'Samsung', 'Sony', 'Bose', 'Canon']),
        'sku':                fake.bothify(text='??-#####').upper(),
        'discount_pct':       fake.random_element([0, 5, 10, 15, 20]),
        'tax_amount':         round(price * quantity * 0.075, 2),
        'channel':            fake.random_element(['web', 'mobile_app', 'in_store', 'phone']),
        'device_type':        fake.random_element(['desktop', 'mobile', 'tablet']),
        'session_id':         fake.uuid4(),
        'referral_source':    fake.random_element(['organic', 'paid_ad', 'email', 'social', 'direct']),
        'promo_code':         fake.random_element([None, 'SAVE10', 'FLASH20', 'WELCOME5']),
        'region':         fake.state(),
        'city':           fake.city(),
        'zip_code':       fake.zipcode(),
        'transaction_id':  fake.uuid4(),
        'transaction_status': fake.random_element(elements=['completed', 'pending', 'failed']),
        'payment_method': fake.random_element(elements=['credit_card', 'cash', 'bank_transfer'])
    }

def orders(n=1000000):
    """Generates n unique order records."""
    return [generate_single_order() for _ in range(n)]

# ============= Send to Kafka ============

# def send_orders_to_kafka(batch_size=50, continuous=False):
#     """
#     Sends orders to Kafka.
#     - continuous=False: sends one batch and stops
#     - continuous=True:  keeps sending batches in a loop
#     """
#     rec = 0
#     while True:
#         order_records = orders(batch_size)
#         total_records = len(order_records)
#         rec += total_records
#         for order in order_records:
#             producer.send(TOPIC_NAME, value=order)
#             logging.info(f"Sent order: {order['order_id']} to topic: {TOPIC_NAME}")

#         producer.flush()  
#         logging.info(f"Batch of {batch_size} records flushed to topic: {TOPIC_NAME}")

#         if rec >= 10000 and not continuous:
#             break

#         time.sleep(5)

#     logging.info("Done sending orders.")

def send_to_gcs():
    logging.info("loading data to gcs")
    from google.cloud import storage                                                            
    from google.oauth2 import service_account
    from io import BytesIO

    path = os.getenv("gcs_cred")
    cred = service_account.Credentials.from_service_account_file(path)  
    gcs = storage.Client(credentials=cred)
    #========= create a csv data of 100000 orders =======
    try:
        with tempfile.TemporaryDirectory() as dir:

            path = os.path.join(dir, "data.csv")
            order_data = orders(n=100000)
            logging.info("created order data")
            with open(path, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=order_data[0].keys())
                writer.writeheader()
                writer.writerow(order_data)
            logging.info(f"wrote {len(order_data)} to csv")
        
    except Exception as e:
        logging.error(f" error generating sales data{e}")

def main():
    send_to_gcs()

if __name__ == "__main__":
    # main()
    send_to_gcs()