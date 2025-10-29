-- Using source macros for source table references for easier environment config and caching
WITH food_orders AS (
  SELECT
    c.name,
    COUNT(oi.order_item_id) AS item_count
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o ON c.customer_id = o.customer_id
    JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
  WHERE
    oi.is_food_item = 1
  GROUP BY
    c.name
),
-- Separate CTE for drink orders to maintain clarity and avoid costly conditional aggregations
 drink_orders AS (
  SELECT
    c.name,
    COUNT(oi.order_item_id) AS item_count
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o ON c.customer_id = o.customer_id
    JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
  WHERE
    oi.is_drink_item = 1
  GROUP BY
    c.name
)
-- Union results efficiently using UNION ALL to prevent unnecessary sorting and distinct operations
SELECT
  name,
  item_count,
  'food' AS item_type
FROM
  food_orders
UNION ALL
SELECT
  name,
  item_count,
  'drink' AS item_type
FROM
  drink_orders

-- Optimizations:
-- 1. Applied source() macro for environment-independent source reference.
-- 2. Used COUNT with filter conditions for an efficient aggregation.
-- 3. UNION ALL avoids DISTINCT overhead where no duplicates expected.
-- 4. Split into CTEs for clarity and potential caching benefits.
