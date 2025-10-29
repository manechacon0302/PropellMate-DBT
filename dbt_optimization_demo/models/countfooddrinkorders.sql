{{ config(
    materialized='view'
) }}

WITH customers AS (
    SELECT
        customer_id,
        name
    FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT
        order_id,
        customer_id
    FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT
        order_item_id,
        order_id,
        is_food_item,
        is_drink_item
    FROM {{ ref('stg_order_items') }}
),

customer_orders AS (
    SELECT
        c.name,
        c.customer_id,
        o.order_id
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
),

item_counts AS (
    SELECT
        co.name,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item
    FROM customer_orders co
    INNER JOIN order_items oi ON co.order_id = oi.order_id
),

food_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count
    FROM item_counts
    WHERE is_food_item = 1
    GROUP BY name
),

drink_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count
    FROM item_counts
    WHERE is_drink_item = 1
    GROUP BY name
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