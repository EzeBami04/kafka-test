import pandas as pd
from psycopg2.extras import execute_values

def load_dim_cust(df: pd.DataFrame, conn) -> None:
    rows = list(df.itertuples(index=False, name=None))
    sql = """
        INSERT INTO dim_customers (cust_id, cust_name, cust_email, cust_phone, cust_seg, is_ret_cust)
        VALUES %s
        ON CONFLICT (cust_id) DO UPDATE SET
            cust_name    = EXCLUDED.cust_name,
            cust_email   = EXCLUDED.cust_email,
            cust_phone   = EXCLUDED.cust_phone,
            cust_seg     = EXCLUDED.cust_seg,
            is_ret_cust  = EXCLUDED.is_ret_cust;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


def load_dim_date(df: pd.DataFrame, conn) -> None:
    rows = list(df.itertuples(index=False, name=None))
    sql = """
        INSERT INTO dim_date (date_id, date, day, month, year, quarter)
        VALUES %s
        ON CONFLICT (date_id) DO NOTHING;
    """
    # date rows never change once created — DO NOTHING is correct here
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


def load_dim_prod(df: pd.DataFrame, conn) -> None:
    rows = list(df.itertuples(index=False, name=None))
    sql = """
        INSERT INTO dim_products (prod_id, item_ordered, category, brand, sku)
        VALUES %s
        ON CONFLICT (prod_id) DO UPDATE SET
            item_ordered = EXCLUDED.item_ordered,
            category     = EXCLUDED.category,
            brand        = EXCLUDED.brand,
            sku          = EXCLUDED.sku;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


def load_dim_location(df: pd.DataFrame, conn) -> None:
    rows = list(df.itertuples(index=False, name=None))
    sql = """
        INSERT INTO dim_location (location_id, country, state, city, zip_code)
        VALUES %s
        ON CONFLICT (location_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


def load_fact_orders(df: pd.DataFrame, conn) -> None:
    rows = list(df.itertuples(index=False, name=None))
    sql = """
        INSERT INTO fact_orders (
            order_id, transaction_id, product_id, customer_id,
            date_id, location_id,
            quantity, price, total_amount, tax_amount, discount_pct,
            payment_method, transaction_status
        )
        VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            transaction_status = EXCLUDED.transaction_status,
            quantity           = EXCLUDED.quantity,
            total_amount       = EXCLUDED.total_amount;
    """
    # Only mutable fields updated on conflict — price/tax are immutable once placed
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()