/*
  Model: countfooddrinkorders
  Description: Counts food and drink orders by customer name
  Optimization: 
    - Uses single query with conditional aggregation instead of UNION ALL
    - Leverages dynamic source references via {{ source() }} macro
    - Reduces data scanning by reading base tables only once
    - Improves performance by eliminating duplicate JOINs
*/

-- Materialize as view for real-time reporting without storage costs
{{ config(materialized='view') }}

WITH 
-- Base CTE: Join all necessary tables once to reduce redundant scans
order_items_with_customer AS (
    SELECT
        c.name AS customer_name,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item
    FROM {{ source('semantic_layer', 'dim_customers') }} AS c
    -- Join orders to link customers with their order items
    INNER JOIN {{ source('semantic_layer', 'fct_orders') }} AS o 
        ON c.customer_id = o.customer_id
    -- Join order items to get item details
    INNER JOIN {{ source('semantic_layer', 'order_items') }} AS oi 
        ON o.order_id = oi.order_id
    -- Filter only food or drink items to reduce data volume early
    WHERE oi.is_food_item = 1 
       OR oi.is_drink_item = 1
)

-- Final aggregation: Use CASE statements to pivot data instead of UNION ALL
SELECT
    customer_name AS name,
    -- Count food items
    SUM(CASE WHEN is_food_item = 1 THEN 1 ELSE 0 END) AS food_item_count,
    -- Count drink items
    SUM(CASE WHEN is_drink_item = 1 THEN 1 ELSE 0 END) AS drink_item_count
FROM order_items_with_customer
GROUP BY customer_name
-- Only include customers who have ordered at least one item
HAVING food_item_count > 0 OR drink_item_count > 0
ORDER BY customer_name
