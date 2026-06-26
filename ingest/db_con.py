import psycopg2
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Conn:

    def connect_db(self, host, database, port, user, password, sslmode="require"):
        try:
            con = create_engine(
                f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}')

            logging.info("Database connection successful")
            return con

        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise

    def sql_orm(self, host, database, port, user, password, sslmode="require"):
        try:
            # f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}'
            con = psycopg2.connect(
                host=host, database=database, user=user, password=password, port=port, sslmode=sslmode)

            logging.info("Database connection successful")
            return con

        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise