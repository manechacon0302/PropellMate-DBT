SELECT 
    IdTown,
    IdAlias,
    Alias

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_TownAlias') }}
WHERE CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')