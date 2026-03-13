import pandas as pd

def clean_sales_data(df):
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
    
    return df