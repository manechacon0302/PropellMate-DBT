{{
  config(
    materialized='view'
  )
}}

SELECT
    customer_name,
    SUM(order_total) AS total_spent
FROM {{ ref('int_orders_with_customers') }}
GROUP BY customer_name
ORDER BY total_spent DESC
