#========== Location transformation ==========
def transform_location(df, pd):
    loc_dict = {
        "location_id": df['location_id'],
        "country": df['country'],
        "state": df['state'],
        "city": df['city'],
        "zip_code": df['zip_code']
    }

    return pd.DataFrame(loc_dict)