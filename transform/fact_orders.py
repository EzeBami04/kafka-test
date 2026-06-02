""" transforms ingested data into fact_orders table format. """

def transform_orders(df, pd):
    """receives fact orders"""
    fact_ord = {
        "order_id": df['order_id'],
        "transaction_id": df['transaction_id'],
        "product_id": df['producti_id'],
        "customer_id": df['customer_id'],
        "quantity": df['quantity'],
        "price": df['price'],
        "total_amount": df['total_amount'],
        "tax_amount": df['tax_amount'],
        "discount_pct": df['discount_pct'],
        "order_date": df['order_date'],
        "payment_method": df['payment_mathod'],
        "transaction_status": df['transaction_status']
    }

    fact_ord_df = pd.DataFrame(fact_ord)
    return fact_ord_df