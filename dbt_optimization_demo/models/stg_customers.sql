SELECT
    customer_id,
    name
FROM {{ source('dbt_semantic_layer', 'dim_customers') }}