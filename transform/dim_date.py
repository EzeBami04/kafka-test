# =========== Transformations for dim_date table ===========

def dim_date(df, pd):
    date_dict = {
        "date_id": df['order_date'].dt.strftime('%Y%m%d'),
        "date": df['order_date'].dt.date,
        "day": df['order_date'].dt.day,
        "month": df['order_date'].dt.month,
        "year": df['order_date'].dt.year,
        "quarter": df['order_date'].dt.quarter
    }

    return pd.DataFrame(date_dict)