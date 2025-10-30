WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
customer_order_items AS (
    SELECT
        c.name,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
),
food_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count
    FROM customer_order_items
    WHERE is_food_item = 1
    GROUP BY name
),
drink_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count
    FROM customer_order_items
    WHERE is_drink_item = 1
    GROUP BY name
),
combined_orders AS (
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
)
SELECT
    name,
    item_count,
    item_type
FROM combined_orders