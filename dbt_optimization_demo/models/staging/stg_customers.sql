{{ config(
    materialized='view'
) }}

SELECT
    customer_id,
    name
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
