
 WITH daily_sales AS (
        SELECT
            date,
            quantity * price as revenue
        FROM stg_sales
    )
    SELECT
        date,
        sum(revenue) as total_revenue
    FROM daily_sales
    WHERE date >= date((SELECT MAX(date) FROM sales), '-30 days')  
    GROUP BY date
    ORDER BY date ASC