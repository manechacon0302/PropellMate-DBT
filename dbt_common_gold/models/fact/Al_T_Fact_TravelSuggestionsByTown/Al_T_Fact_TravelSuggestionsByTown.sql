WITH RouteJourneys AS(
  SELECT DISTINCT IdJourney, IdOrigin, IdOriginAlias, IdDestination, IdDestinationAlias, Distance
  FROM {{ ref( "Al_T_Dim_RouteJourneys") }}
  WHERE CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
)
, Validas AS(
  SELECT IdOrigin, IdDestination, Url
  FROM {{ ref( "Al_T_Dim_RutasAlsaSEO") }}
  WHERE CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
)
, tags AS (
SELECT IdTown, MIN(TagOrder) AS TagOrder FROM(
  SELECT mun.IdTown
    ,CASE 
      WHEN dim.Tag='IsCoastal' THEN 1 
      WHEN dim.Tag='HasRelevantFestivals' THEN 2
      WHEN dim.Tag='IsTouristDestination' THEN 3
      ELSE 0
    END AS TagOrder
  FROM {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTowns') }} mun
  INNER JOIN {{ source('bq_fact_gld_gemini', 'Al_T_Fact_TaggedMunicipalities') }} tag
    ON mun.IdMunicipality=tag.IdMunicipality
  INNER JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTags') }} dim
    ON dim.IdMunicipalityTag=tag.IdTag
  WHERE dim.Tag IN(
    'IsCoastal'
    ,'HasRelevantFestivals'
    ,'IsTouristDestination'
  )
    AND UPPER(tag.TagValue)='SÍ'
    AND mun.ValidEndDate IS NULL 
    AND tag.ValidEndDate IS NULL 
    AND dim.ValidEndDate IS NULL 
  )
  GROUP BY IdTown
)
--Tags
SELECT
  CAST(Null AS STRING) as IdCluster,
  t.IdTown AS IdOrigin,
  t.Town AS OriginTown,
  Rut.IdDestination,
  SUM(TotalPassengers) AS Ventas,
  CASE
    WHEN TagOrder=1 THEN 'IsCoastal'
    WHEN TagOrder=2 THEN 'HasRelevantFestivals'
    WHEN TagOrder=3 THEN 'IsTouristDestination'
    ELSE ''
  END AS Tag
  ,TagOrder
  ,Validas.Url
  ,MIN(Distance) as Distance
  ,CASE
    WHEN Validas.IdOrigin IS NULL THEN FALSE
    ELSE TRUE
  END AS isValidurl,

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

FROM {{ ref( "dim_pueblo") }} t 
INNER JOIN (
  SELECT DISTINCT IdJourney, IdOrigin, IdDestination, Distance
  FROM RouteJourneys

  UNION ALL
  SELECT DISTINCT IdJourney, IdOriginAlias, IdDestination, Distance
  FROM RouteJourneys
  WHERE IdOriginAlias IS NOT NULL

  UNION ALL
  
  SELECT DISTINCT IdJourney, IdOrigin, IdDestinationAlias, Distance
  FROM RouteJourneys
  WHERE IdDestinationAlias IS NOT NULL

  UNION ALL
  
  SELECT DISTINCT IdJourney, IdOriginAlias, IdDestinationAlias, Distance
  FROM RouteJourneys
  WHERE IdDestinationAlias IS NOT NULL AND IdOriginAlias IS NOT NULL
  ) AS Rut
  ON Rut.IdOrigin = t.IdTown 
LEFT JOIN tags v
  ON v.IdTown=t.IdTown
LEFT JOIN Validas
  ON Validas.IdOrigin=Rut.IdOrigin AND Validas.IdDestination=Rut.IdDestination
LEFT JOIN (
  SELECT IdJourney, SUM(TotalPassengers) TotalPassengers
  FROM {{ ref( "Al_T_Fact_SalesSearches") }} v 
  WHERE PARSE_DATE('%Y%m', SAFE_CAST(TravelMonthDate AS STRING)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    AND CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
  GROUP BY IdJourney
) Ventas
  ON Ventas.IdJourney = Rut.IdJourney
WHERE t.ValidEndDate IS NULL
GROUP BY ALL