
SELECT
    client_id,
    date,
    SUM(total_revenue) as total_revenue
FROM {{ ref('int_sales_revenue') }}
WHERE date >= (SELECT MAX(date) FROM {{ ref('int_sales_revenue') }}) - INTERVAL '30 days'
GROUP BY client_id, date
ORDER BY date ASC