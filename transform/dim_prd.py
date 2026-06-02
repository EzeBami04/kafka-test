#========== Transformation for dim_prd ==============

def dim_prod(df, pd):
    prod_dict = {
        "prod_id": df['prod_id'],
        "item_ordered": df['item_ordered'],
        "category": df['category'],
        "brand": df['brand'],
        "sku": df["sku"],

    }

    return pd.Dataframe(prod_dict)