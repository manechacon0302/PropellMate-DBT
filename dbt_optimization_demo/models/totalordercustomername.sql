WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
customer_totals AS (
    SELECT
        c.name AS customer_name,
        SUM(o.order_total) AS total_spent
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.name
)
SELECT
    customer_name,
    total_spent
FROM customer_totals
ORDER BY total_spent DESC