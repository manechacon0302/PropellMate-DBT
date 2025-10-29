SELECT
COALESCE(ora.DateHour, gcp.DateHour)       AS DateHour,
COALESCE(ora.IdSystem, gcp.IdSystem)       AS IdSystem,
obt.Description                            AS DescSystem,
ora.TotalPositions                         AS TotalPositionsORA,
gcp.TotalPositions                         AS TotalPositionsGCP,


 
FROM {{ source('bq_fact_silver_com', 'Al_T_Agg_PositionsControlORA') }} ora

FULL JOIN {{ source('bq_fact_silver_com', 'Al_T_Agg_PositionsControlGCP') }} gcp
ON ora.DateHour = gcp.DateHour and ora.IdSystem = gcp.IdSystem
AND {{ var("version_date", get_config_value_v4("version_date")) }} BETWEEN gcp.ValidStartDate AND COALESCE(gcp.ValidEndDate, '3000-1-1')

LEFT JOIN {{ source('bq_fact_silver_com', 'Al_T_Dim_Systems') }} obt
ON IdExplot = COALESCE(ora.IdSystem, gcp.IdSystem)
AND {{ var("version_date", get_config_value_v4("version_date")) }} BETWEEN obt.ValidStartDate AND COALESCE(obt.ValidEndDate, '3000-1-1')

/* Historic Condition */
WHERE {{ var("version_date", get_config_value_v4("version_date")) }} BETWEEN ora.ValidStartDate AND COALESCE(ora.ValidEndDate, '3000-1-1')

/* TO FULL-REFRESH COMMENT THIS PART AND DELETE_WHERE_CLAUSE IN SCHEMA */
AND COALESCE(ora.DateHour, gcp.DateHour)   >= {{ var("modif_date", get_config_value_v4("modif_date")) }}