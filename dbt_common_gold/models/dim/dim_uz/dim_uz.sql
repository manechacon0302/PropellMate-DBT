SELECT 
IdZu,
Zu,
IdDivision,
DateZuStart,
DateZuEnd

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_ZonalUnit') }}
WHERE {{ var("version_date", get_config_value_v4("version_date")) }}
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')