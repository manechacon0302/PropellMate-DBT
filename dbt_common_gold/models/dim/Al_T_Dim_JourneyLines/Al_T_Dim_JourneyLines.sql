SELECT 
    T.IdJourney,
    T.IdOrigin,
    T.OriginTown,
    T.IdOriginAlias,
    T.OriginAlias,
    T.IdDestination,
    T.DestinationTown,
    T.IdDestinationAlias,
    T.DestinationAlias,
    T.Journey,
    T.IdLine,
    T.LineType,
    T.Distance,
    R.IdRoute,
    R.Route

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_Journey') }} T
LEFT JOIN {{ source('bq_dim_silver_com', 'Al_T_Dim_Route') }} R
    ON (T.IdOrigin = R.Town1 AND T.IdDestination = R.Town2) OR (T.IdOrigin = R.Town2 AND T.IdDestination = R.Town1)
WHERE CURRENT_TIMESTAMP() BETWEEN T.ValidStartDate AND COALESCE(T.ValidEndDate, '3000-1-1')
AND
    (
        CURRENT_TIMESTAMP() BETWEEN R.ValidStartDate AND COALESCE(R.ValidEndDate, '3000-1-1') OR
        R.Town1 IS NULL
    )
