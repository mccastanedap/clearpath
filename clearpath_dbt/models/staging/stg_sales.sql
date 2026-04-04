-- Staging model for raw sales data
-- Cleans and standardizes the raw sales table

SELECT
    date,
    product_name,
    category,
    size,
    CAST(quantity AS INTEGER) as quantity,
    CAST(price AS FLOAT) as price,
    ROUND(quantity * price, 2) as revenue,
    is_known_product
FROM sales
WHERE quantity > 0
    AND price IS NOT NULL
    AND date IS NOT NULL