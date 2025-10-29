-- This model computes total spending by each customer.
-- Refactored to remove redundant subquery and use direct joins with sources.

SELECT
  c.name AS customer_name,
  SUM(o.order_total) AS total_spent
FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c ON o.customer_id = c.customer_id
GROUP BY c.name
ORDER BY total_spent DESC
