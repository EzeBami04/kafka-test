import pandas as pd
import os
from datetime import datetime
import logging

# from .src.orders import main
from ingest.ingest import run
from ingest.db_con import Conn
from  ingest.ingest import load_to_stg
from transform.transform import dim_cust, dim_date, dim_prod, dim_location, transform_fact_orders
from load.load_pst import load_dim_cust, load_dim_date, load_dim_prod, load_dim_location, load_fact_orders
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

#============================ env ========================
db_name = os.getenv('db_name')
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
host_stg = os.getenv("stg_host")
db_stg = os.getenv("stg_name")
user_stg = os.getenv('stg_user')
pass_stg = os.getenv("stg_password")
port = os.getenv("port")
#========================== database connection ===================================
db_conn = Conn()
conn_stg = db_conn.connect_db(host_stg, db_stg, port, user_stg, pass_stg, sslmode="require")
con_load = db_conn.sql_orm(db_host, db_name, port, db_user, db_password, sslmode="require")

#=======================================================================================

def read_staging():
    try:
        sql_stmt = """
                    Select * From public.stg_orders 
                    where order_id is not null"""
        df = pd.read_sql(sql_stmt, con=conn_stg)
        return df
    except Exception as e:
        logging.info(f"Error reading from database {e}")

    
def main():
    
    start_time = datetime.now()
    #============= consume kafka messages ============

    logging.info("Cosume kafka messages")
    # df_batch = run()

    # logging.info("Load to staging")
    # load_to_stg(df_batch, conn_stg, logger=logging)

    logging.info("read from staging")
    df_stg = read_staging()
    
    #============ transform data =============
    if df_stg is None or df_stg.empty:
        logging.warning("No data to process after reading from staging.")
        return
    logging.info(f"Read {len(df_stg)} rows from staging table.")
    df_dim_cust = dim_cust(df_stg)
    df_dim_date = dim_date(df_stg)
    df_dim_prod = dim_prod(df_stg)
    df_dim_location = dim_location(df_stg)
    df_fact_orders = transform_fact_orders(df_stg)


    #======== load to facts and dimensions =============
    logger = logging
    try:
        load_dim_cust(df_dim_cust, con_load, logger)
        load_dim_date(df_dim_date, con_load, logger)
        load_dim_prod(df_dim_prod, con_load, logger)
        load_dim_location(df_dim_location, con_load, logger)
        load_fact_orders(df_fact_orders, con_load, logger)
    except Exception as e:
        logging.error(f"Error occurred while loading data: {e}")
    
    end_time = datetime.now()
    logging.info(f"ETL process completed in {end_time - start_time}")

    #=================== Create Metadata =======================
    # meta_data = {
    #     "stg_row_count" = df_batch.unique
    # }

if __name__ == "__main__":
    main()