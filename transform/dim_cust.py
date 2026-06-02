# ================ dim_cust.py ================

def dim_cust(df, pd):
    cust_dict = {
        "cust_id": df['customer_id'],
        "cust_name": df['customer_name'],
        "cust_email": df['customer_email'],
        "cust_phone": df['customer_phone'],
        "cust_seg": df['customer_segment'],
        "is_ret_cust": df['is_returning_customer']
    }

    return pd.DataFrame(cust_dict)