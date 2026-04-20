SELECT
    date,
    product_name,
    SUM(quantity) as total_sold,
    SUM(revenue) as total_revenue
FROM {{ ref('int_sales_revenue') }}
GROUP BY product_name
ORDER BY total_sold DESC 
