# ================ transforms.py ================
import pandas as pd


def dim_cust(df: pd.DataFrame) -> pd.DataFrame:
    return df[[
        'customer_id', 'customer_name', 'customer_email',
        'customer_phone', 'customer_segment', 'is_returning_customer'
    ]].rename(columns={
        'customer_id':           'cust_id',
        'customer_name':         'cust_name',
        'customer_email':        'cust_email',
        'customer_phone':        'cust_phone',
        'customer_segment':      'cust_seg',
        'is_returning_customer': 'is_ret_cust'
    }).drop_duplicates(subset=['cust_id'])


def dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dates = df[['order_date']].copy()
    dates['order_date'] = pd.to_datetime(dates['order_date'])
    return pd.DataFrame({
        'date_id': dates['order_date'].dt.strftime('%Y%m%d').astype(int),
        'date':    dates['order_date'].dt.date,
        'day':     dates['order_date'].dt.day,
        'month':   dates['order_date'].dt.month,
        'year':    dates['order_date'].dt.year,
        'quarter': dates['order_date'].dt.quarter,
    }).drop_duplicates(subset=['date_id'])


def dim_prod(df: pd.DataFrame) -> pd.DataFrame:
    return df[[
        'product_id', 'item_ordered', 'category', 'brand', 'sku'
    ]].rename(columns={
        'product_id': 'prod_id'
    }).drop_duplicates(subset=['prod_id'])


def dim_location(df: pd.DataFrame) -> pd.DataFrame:
    """
    Faker produces region/city/zip_code — no location_id or country in source.
    Hash region+city+zip to make a stable surrogate key.
    """
    loc = df[['region', 'city', 'zip_code']].drop_duplicates().copy()
    loc['location_id'] = (
        loc['region'] + loc['city'] + loc['zip_code']
    ).apply(lambda x: abs(hash(x)) % (10 ** 9))
    loc['country'] = 'US'                           
    return loc[[
        'location_id', 'country', 'region', 'city', 'zip_code'
    ]].rename(columns={'region': 'state'})


def transform_fact_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['date_id'] = df['order_date'].dt.strftime('%Y%m%d').astype(int)

    # derive location_id the same way as dim_location
    df['location_id'] = (
        df['region'] + df['city'] + df['zip_code']
    ).apply(lambda x: abs(hash(x)) % (10 ** 9))

    return df[[
        'order_id', 'transaction_id', 'product_id', 'customer_id',
        'date_id', 'location_id',
        'quantity', 'price', 'total_amount', 'tax_amount', 'discount_pct',
        'payment_method', 'transaction_status'
    ]]