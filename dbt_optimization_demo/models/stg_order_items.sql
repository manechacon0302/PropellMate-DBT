SELECT
    order_item_id,
    order_id,
    is_food_item,
    is_drink_item
FROM {{ source('dbt_semantic_layer', 'order_items') }}