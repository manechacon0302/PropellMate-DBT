{{ config(
    materialized='view'
) }}

WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_total
    FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT
        customer_id,
        name
    FROM {{ ref('stg_customers') }}
),

customer_order_totals AS (
    SELECT
        c.name AS customer_name,
        SUM(o.order_total) AS total_spent
    FROM orders o
    INNER JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.name
)

SELECT
    customer_name,
    total_spent
FROM customer_order_totals
ORDER BY total_spent DESC