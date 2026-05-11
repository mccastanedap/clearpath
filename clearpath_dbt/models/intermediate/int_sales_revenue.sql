SELECT *,
ROUND(quantity * price, 2) as total_revenue
FROM {{ ref('stg_sales') }}