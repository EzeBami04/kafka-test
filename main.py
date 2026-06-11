import psycopg2

import os
from datetime import datetime
import logging

# from .src.orders import main
from ingest.ingest import run
from transform.transform import dim_cust, dim_date, dim_prod, dim_location, transform_fact_orders
from load.load_pst import load_dim_cust, load_dim_date, load_dim_prod, load_dim_location, load_fact_orders


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def main():

    start_time = datetime.now()
    #==== consume kafka  messages =============
    df_batch = run()

    #==== transform data =============
    df_dim_cust = dim_cust(df_batch)
    df_dim_date = dim_date(df_batch)
    df_dim_prod = dim_prod(df_batch)
    df_dim_location = dim_location(df_batch)
    df_fact_orders = transform_fact_orders(df_batch)


    #==== load to postgres =============
    conn = engine()
    try:
        load_dim_cust(df_dim_cust, conn)
        load_dim_date(df_dim_date, conn)    
        load_dim_prod(df_dim_prod, conn)
        load_dim_location(df_dim_location, conn)
        load_fact_orders(df_fact_orders, conn)
    except Exception as e:
        logging.error(f"Error occurred while loading data: {e}")
        conn.rollback()
    finally:
        conn.close()
    
    end_time = datetime.now()
    logging.info(f"ETL process completed in {end_time - start_time}")



if __name__ == "__main__":
    main()