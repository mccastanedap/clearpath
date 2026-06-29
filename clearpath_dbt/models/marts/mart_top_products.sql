SELECT
    client_id,
    product_name,
    SUM(quantity) as total_sold,
    SUM(total_revenue) as total_revenue
FROM {{ ref('int_sales_revenue') }}
GROUP BY client_id, product_name
ORDER BY total_sold DESC
