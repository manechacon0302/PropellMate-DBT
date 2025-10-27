/*
  Model: customerswithorders
  Description: Enriches order data with customer information
  Optimization:
    - Replaced correlated subquery with efficient JOIN operation
    - Uses dynamic source references via {{ source() }} macro
    - Reduces query execution time by eliminating row-by-row subquery execution
    - Better query plan and indexing utilization
*/

-- Materialize as view for real-time data without storage overhead
{{ config(materialized='table') }}

WITH 
-- Base CTE: Get all orders from the fact table
orders AS (
    SELECT 
        order_id,
        customer_id,
        order_total,
        ordered_at
    FROM {{ source('semantic_layer', 'fct_orders') }}
),

-- Base CTE: Get customer dimension data
customers AS (
    SELECT 
        customer_id,
        name
    FROM {{ source('semantic_layer', 'dim_customers') }}
)

-- Final SELECT: Join orders with customer data in a single pass
-- This eliminates the inefficient correlated subquery pattern
SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,  -- Direct join replaces correlated subquery
    o.order_total,
    o.ordered_at
FROM orders AS o
-- LEFT JOIN ensures we keep orders even if customer data is missing
LEFT JOIN customers AS c 
    ON o.customer_id = c.customer_id
-- Order by order_id for consistent result sets
ORDER BY o.order_id
