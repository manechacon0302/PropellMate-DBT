-- Sums order total per customer and displays customer name
-- Replaces inefficient correlated subquery with a JOIN for better performance and cost
-- All sources referenced via dbt 'source' macro

SELECT
  c.name AS customer_name,
  SUM(o.order_total) AS total_spent
FROM
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
  JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c ON o.customer_id = c.customer_id
GROUP BY
  c.name
ORDER BY
  total_spent DESC
