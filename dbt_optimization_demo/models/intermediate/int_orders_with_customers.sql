{{
  config(
    materialized='ephemeral'
  )
}}

SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    o.order_total,
    o.ordered_at
FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
INNER JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    ON o.customer_id = c.customer_id
