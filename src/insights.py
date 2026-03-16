import anthropic
import pandas as pd

def generate_insights(top_products_df, daily_revenue_df, velocity_df, business_name, business_type):
    """
    Sends query results to Claude API and returns
    plain English business recommendations.
    """
    client = anthropic.Anthropic()

    prompt = f"""
    You are a data analyst assistant helping a small business owner understand their sales data.
    
    Business: {business_name}
    Type: {business_type}
    
    Here is their sales data summary:
    
    TOP SELLING PRODUCTS:
    {top_products_df.to_string()}
    
    DAILY REVENUE (last 30 days):
    {daily_revenue_df.to_string()}
    
    PRODUCT VELOCITY (units sold per day - LOW means spoilage risk):
    {velocity_df.to_string()}
    
    Based on this data please provide:
    1. Top 3 actionable recommendations the owner should act on this week
    2. Any spoilage or dead stock risks they should address immediately
    3. One positive trend worth celebrating
    
    Keep the language simple - this owner is not a data person.
    """

    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}]
    )

    return message.content[0].text