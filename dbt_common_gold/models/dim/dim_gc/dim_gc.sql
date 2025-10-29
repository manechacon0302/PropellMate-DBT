SELECT 
  silver.IdCg,
  silver.Cg,
  silver.IdZu,
  silver.DateCgStart,
  silver.DateCgEnd,
  silver.IdCommercialSpeed,
  

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_ContractGroup') }} AS silver

WHERE {{ var("version_date", get_config_value_v4("version_date")) }}
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')

