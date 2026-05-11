
SELECT
    date,
    SUM(total_revenue) as total_revenue
FROM {{ ref('int_sales_revenue') }}
WHERE date >= (SELECT MAX(date) FROM {{ ref('int_sales_revenue') }}) - INTERVAL '30 days'
GROUP BY date
ORDER BY date ASC