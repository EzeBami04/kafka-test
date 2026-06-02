import psycopg2
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta

# from .src.orders import main
from .ingest.ingest import consumer_orders, process_order

#========= env ==========
db_name = os.getenv('db_name')
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
#========== database connection ===========

def engine():
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )

    return conn