SELECT
    order_id,
    customer_id,
    order_total,
    ordered_at
FROM {{ source('dbt_semantic_layer', 'fct_orders') }}