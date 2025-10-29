{{
  config(
    materialized='view'
  )
}}

WITH customer_order_items AS (
    SELECT
        c.name AS customer_name,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item
    FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
        ON c.customer_id = o.customer_id
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
        ON o.order_id = oi.order_id
),

food_orders AS (
    SELECT
        customer_name AS name,
        COUNT(order_item_id) AS item_count
    FROM customer_order_items
    WHERE is_food_item = 1
    GROUP BY customer_name
),

drink_orders AS (
    SELECT
        customer_name AS name,
        COUNT(order_item_id) AS item_count
    FROM customer_order_items
    WHERE is_drink_item = 1
    GROUP BY customer_name
)

SELECT
    name,
    item_count,
    'food' AS item_type
FROM food_orders

UNION ALL

SELECT
    name,
    item_count,
    'drink' AS item_type
FROM drink_orders
