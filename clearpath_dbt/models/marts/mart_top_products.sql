SELECT
    date,
    product_name,
    SUM(quantity) as total_sold,
    SUM(quantity * price) as total_revenue
FROM stg_sales
GROUP BY product_name
ORDER BY total_sold DESC 
