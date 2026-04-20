
SELECT
    date,
    SUM(revenue) as total_revenue
FROM {{ ref('int_sales_revenue') }}
WHERE date >= date((SELECT MAX(date) FROM {{ ref('int_sales_revenue') }}), '-30 days')
GROUP BY date
ORDER BY date ASC