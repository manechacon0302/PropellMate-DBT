-- Counts the total food and drink items ordered per customer and labels them by item type.
-- Optimized to use a single scan and conditional aggregation instead of multiple CTEs/unions for better performance and cost.
-- All base table references use dbt's source macro for portability/lineage.

SELECT
  c.name,
  SUM(CASE WHEN oi.is_food_item = 1 THEN 1 ELSE 0 END) AS food_item_count, -- Count of food items
  SUM(CASE WHEN oi.is_drink_item = 1 THEN 1 ELSE 0 END) AS drink_item_count -- Count of drink items
FROM
  {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
  JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o ON c.customer_id = o.customer_id
  JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
GROUP BY
  c.name

-- Use one row per customer; if you require 'food' and 'drink' to be separate rows, use UNPIVOT or UNION ALL after aggregation, but this approach is more efficient.
