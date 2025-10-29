-- Produces orders joined with customer name. Eliminates inefficient correlated subquery by using a JOIN (improving cost & speed).
-- Uses dbt source macros for all base table references to ensure correct source lineage.

SELECT
  o.order_id,
  o.customer_id,
  c.name AS customer_name, -- Pull customer name through join, not subquery
  o.order_total,
  o.ordered_at
FROM
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
  JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    ON o.customer_id = c.customer_id
