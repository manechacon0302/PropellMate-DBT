SELECT
    CAST(Null AS STRING) AS IdCluster,
    IdOrigin,
    OriginName,
    IdDestination,
    DestinationName,
    Url,
    -- AUDITORIA
    CAST(NULL AS STRING) as IdControl,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) as CreateDate,
    'DBT' as CreateUser,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) as ModifDate,
    'DBT' as ModifUser,
    CAST(NULL AS TIMESTAMP) as DeleteDate,
    CAST(NULL AS STRING) as DeleteUser,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) as ValidStartDate,
    CAST(NULL AS TIMESTAMP) as ValidEndDate

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_RutasAlsaSEO') }}