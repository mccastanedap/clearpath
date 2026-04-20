-- Staging model for raw sales data
-- Cleans and standardizes the raw sales table
{{ config(materialized='table') }}


SELECT
    date,
    product_name,
    category,
    size,
    CAST(quantity AS INTEGER) as quantity,
    CAST(price AS FLOAT) as price,
    is_known_product
FROM {{ source('main', 'sales') }}
WHERE quantity > 0
    AND price IS NOT NULL
    AND date IS NOT NULL