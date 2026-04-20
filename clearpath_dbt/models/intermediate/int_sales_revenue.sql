SELECT *,
ROUND(quantity * price, 2) as revenue
FROM {{ ref('stg_sales') }}