import pandas as pd
from src.clean import clean_sales_data
from src.database import load_to_database
from src.queries import top_products, daily_revenue, product_velocity
from src.insights import generate_insights

def run_pipeline(raw_file_path, business_name, business_type):
    """
    Runs the full Clearpath pipeline:
    1. Clean raw sales data
    2. Load into database
    3. Run queries
    4. Generate AI insights
    """

    # Step 1 - Clean
    print("Cleaning data...")
    raw_df = pd.read_csv(raw_file_path)
    clean_df = clean_sales_data(raw_df)

    # Step 2 - Load
    print("Loading to database...")
    load_to_database(clean_df, table_name='sales')

    # Step 3 - Query
    print("Running queries...")
    top_df = top_products()
    revenue_df = daily_revenue()
    velocity_df = product_velocity()

    # Step 4 - Insights
    print("Generating insights...")
    insights = generate_insights(
        top_df, revenue_df, velocity_df,
        business_name, business_type
    )

    print("\n--- CLEARPATH INSIGHTS ---\n")
    print(insights)
    return insights

if __name__ == "__main__":
    run_pipeline(
        raw_file_path='data/raw/sales.csv',
        business_name='Juice Bar NYC',
        business_type='Juice Bar'
    )