-- This model counts food and drink items ordered by each customer.
-- Optimized by replacing repeated logic with a union of conditional aggregation.

SELECT
  c.name,
  -- Count of food items ordered by the customer
  COUNT(CASE WHEN oi.is_food_item = 1 THEN oi.order_item_id END) AS food_item_count,
  -- Count of drink items ordered by the customer
  COUNT(CASE WHEN oi.is_drink_item = 1 THEN oi.order_item_id END) AS drink_item_count
FROM
  {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
  JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o ON c.customer_id = o.customer_id
  JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
GROUP BY
  c.name
