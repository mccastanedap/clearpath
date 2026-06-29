 WITH velocity_calc AS (
            SELECT
                client_id,
                product_name,
                SUM(quantity) as total_sold,
                MIN(date) as first_sale,
                MAX(date) as last_sale,
                (MAX(date) - MIN(date)) as days_in_market
            FROM {{ ref('int_sales_revenue') }}
            GROUP BY client_id, product_name
        )
        SELECT
            client_id,
            product_name,
            total_sold,
            days_in_market,
            CASE 
                WHEN days_in_market = 0 THEN total_sold
                ELSE ROUND(total_sold * 1.0 / days_in_market, 2)
            END as velocity
        FROM velocity_calc
        ORDER BY velocity ASC