-- This model lists orders along with customer names, 
-- optimized by replacing a correlated subquery with a join for better performance.

WITH orders AS (
    SELECT * FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
),
customers AS (
    SELECT * FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
),
orders_with_customer AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.name AS customer_name,  -- Join instead of subquery for performance
        o.order_total,
        o.ordered_at
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
)
SELECT * FROM orders_with_customer
