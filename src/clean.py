import pandas as pd

def clean_sales_data(df, reference_path='data/reference/products.csv'):
    """
    Cleans raw sales data from a retail client.
    """
    df = df.dropna(subset=['date', 'product_name'])
    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True, errors='coerce')
    df = df.dropna(subset=['date'])
    df['product_name'] = df['product_name'].str.lower().str.strip()
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df = df[df['quantity'] > 0]
        # Step 6: Flag unrecognized product names
    try:
        products_ref = pd.read_csv(reference_path)
        known_products = products_ref['product_name_clean'].str.lower().str.strip()
        df['is_known_product'] = df['product_name'].isin(known_products)
    except FileNotFoundError:
        # If no reference file exists yet, flag all as unreviewed
        df['is_known_product'] = None
    
    return df