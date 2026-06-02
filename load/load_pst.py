def load_fact_ord(df, conn):
    """loads fact orders into postgres database."""
    # df.to_sql('fact_orders', con=engine, if_exists='append', index=False)
    # no_of_cols = ".join
    conn.execute("""
            INSERT INTO fact_orders (order_id, transaction_id, product_id, customer_id, quantity, price, 
                     total_amount, tax_amount, discount_pct, order_date, payment_method, transaction_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 
            ON CONFLICT(order_id)
            DO UPDATE SET
                transaction_id = EXCLUDED.transaction_id,
                product_id = EXCLUDED.product_id,
                customer_id = EXCLUDED.customer_id,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                total_amount = EXCLUDED.total_amount,
                tax_amount = EXCLUDED.tax_amount,
                discount_pct = EXCLUDED.discount_pct,
                order_date = EXCLUDED.order_date,
                payment_method = EXCLUDED.payment_method,
                transaction_status = EXCLUDED.transaction_status;
                     """)

def load_dim_cust(df, conn):
    conn.execute("""
        INSERT INTO dim_customers(cust_id, cust_name, cust_email, cust_phone, cust_seg, is_ret_cust)
        VALUES(%s, %s, %s, %s, %s, %s)
        ON CONFLICT(cust_id)
        DO UPDATE SET
            cust_id = EXCLUDED.cust_is,
            cust_name = EXCLUDED.cust_name,
            cust_email = EXCLUDED.cust_email,
            cust_phone = EXCLUDED.cust_phone,
            cust_seg = EXCLEDED.cust_seg,
            is_ret_cust = EXCLUDED.is_ret_cust
                 
        """)
        
# def load_dim_date(df,conn):
    