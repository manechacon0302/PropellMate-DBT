WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
orders_with_customer AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.name AS customer_name,
        o.order_total,
        o.ordered_at
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
)
SELECT * FROM orders_with_customer