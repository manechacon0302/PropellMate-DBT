{{ config(
    materialized='view'
) }}

WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_total,
        ordered_at
    FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT
        customer_id,
        name
    FROM {{ ref('stg_customers') }}
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