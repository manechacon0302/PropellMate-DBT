/*
  Model: totalordercustomername
  Description: Calculates total spending per customer, ordered by highest spenders
  Optimization:
    - Replaced correlated subquery with efficient JOIN operation
    - Uses dynamic source references via {{ source() }} macro
    - Eliminates redundant table scans by joining tables once
    - Improved query execution by moving customer lookup to JOIN instead of SELECT
*/

-- Materialize as view for real-time analytics without storage costs
{{ config(materialized='table') }}

WITH 
-- Base CTE: Get all orders from the fact table
orders AS (
    SELECT 
        customer_id,
        order_total
    FROM {{ source('semantic_layer', 'fct_orders') }}
),

-- Base CTE: Get customer dimension data
customers AS (
    SELECT 
        customer_id,
        name
    FROM {{ source('semantic_layer', 'dim_customers') }}
),

-- Aggregation CTE: Calculate total spending per customer
customer_spending AS (
    SELECT
        o.customer_id,
        SUM(o.order_total) AS total_spent
    FROM orders AS o
    GROUP BY o.customer_id
)

-- Final SELECT: Join aggregated spending with customer names
-- This replaces the inefficient correlated subquery in the original SELECT clause
SELECT
    c.name AS customer_name,
    cs.total_spent
FROM customer_spending AS cs
-- INNER JOIN ensures we only show customers who have placed orders
INNER JOIN customers AS c 
    ON cs.customer_id = c.customer_id
-- Order by highest spenders first for business insights
ORDER BY cs.total_spent DESC
