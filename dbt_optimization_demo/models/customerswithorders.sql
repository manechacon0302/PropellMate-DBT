{{
  config(
    materialized='view'
  )
}}

SELECT
    order_id,
    customer_id,
    customer_name,
    order_total,
    ordered_at
FROM {{ ref('int_orders_with_customers') }}
