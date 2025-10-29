WITH RouteJourneys AS(
  SELECT DISTINCT IdJourney, Journey, IdRoute, IdOrigin, OriginTown, IdDestination, DestinationTown, ValidStartDate, ValidEndDate
  FROM {{ source('bq_dim_gld_com', 'Al_T_Dim_RouteJourneys') }}
  WHERE CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
) 
, SalesSearches AS(
  SELECT IdJourney, IdRoute, SUM(TotalPassengers) TotalPassengers, AVG(Distance) as Distance
  FROM {{ source('bq_fact_gld_com', 'Al_T_Fact_SalesSearches') }}
  WHERE CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
    AND PARSE_DATE('%Y%m', SAFE_CAST(TravelMonthDate AS STRING)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY IdJourney, IdRoute
)
, DimLine AS(
  SELECT IdLine, Line
  FROM {{ source('bq_dim_gld_com', 'Al_T_Dim_Line') }}
  WHERE CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
)
, Lines AS(
  SELECT IdOrigin AS IdOriginL, IdDestination AS IdDestinationL, IdLine
  FROM {{ source('bq_dim_gld_com', 'Al_T_Dim_JourneyLines') }}
  WHERE CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
)
, TownAlias AS(
  SELECT IdTown, Alias
  FROM {{ source('bq_dim_gld_com', 'Al_T_Dim_TownAlias') }}
  WHERE CURRENT_TIMESTAMP() BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
)
, AllTags AS(
  SELECT TM.IdMunicipality, tow.IdTown, tow.AutCom,
    CASE 
      WHEN TA.tag = 'HasRelevantFestivals' THEN 'Festival'
      WHEN TA.tag = 'TouristScore' THEN 'Touristic'
      WHEN TA.tag = 'HabitatType' THEN 'IsBigCity'
      ELSE TA.tag
    END AS Tag,
    TM.TagValue
  FROM {{ source('bq_fact_gld_gemini', 'Al_T_Fact_TaggedMunicipalities') }} tm
  INNER JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTags') }} ta
    ON ta.IdMunicipalityTag = tm.IdTag
    AND CURRENT_TIMESTAMP() BETWEEN TM.ValidStartDate AND COALESCE(TM.ValidEndDate, '3000-1-1')
    AND CURRENT_TIMESTAMP() BETWEEN TA.ValidStartDate AND COALESCE(TA.ValidEndDate, '3000-1-1')
    AND ( (tag, upper(tagvalue)) IN (
      ('IsCoastal', upper('Sí')),
      ('HasRelevantFestivals',upper('Sí')),
      ('IsTouristDestination', upper('Sí')),
      ('IsProvinceCapital', upper('Sí')),
      ('HabitatType', upper('Grande'))
    )
      OR Tag = 'TouristScore' )
  LEFT JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTowns') }} tow
    ON tm.IdMunicipality = tow.IdMunicipality
  WHERE 
    (
      CURRENT_TIMESTAMP() BETWEEN tow.ValidStartDate AND COALESCE(tow.ValidEndDate, '3000-1-1') OR
      tow.IdMunicipality IS NULL
    )
) 
, TouristTag AS(
  SELECT IdTown, Tag, TagValue AS TouristScore
  FROM AllTags
  WHERE Tag = 'Touristic' AND IdMunicipality IN(
    SELECT IdMunicipality
    FROM AllTags
    WHERE Tag = 'IsTouristDestination'
  )
)
,Main AS(
  SELECT
          -- Requested Journey
          tr.IdJourney,
          tr.Journey,

          -- Origin
          tr.IdOrigin           as IdOrigin,
          twno.Country          as OriginCountry,
          twno.AutCom           as OriginCA,
          twno.Province         as OriginProvince,
          twno.IdMunicipality   as IdOriginMunicipality,
          twno.MunicipalityName as OriginMunicipalityName,
          tr.OriginTown         as OriginTown,

          -- Destination
          tr.IdDestination      as IdDestination,
          twnd.AutCom           as DestinationCA,
          twnd.Province         as DestinationProvince,
          twnd.IdMunicipality   as IdDestinationMunicipality,
          twnd.MunicipalityName as MunicipalityDestination,
          tr.DestinationTown    as DestinationTown,

          -- Suggestion
          trd.IdJourney         as IdJourneySuggested,
          trd.Journey           as JourneySuggested,
          twns.idtown           as IdDestinationTownSuggested,
          twns.Country          as CountrySuggested,
          twns.Province         as ProvinceSuggested,
          twns.MunicipalityName as MunicipalityNameSuggested,
          trd.DestinationTown   as DestinationTownSuggested,

          sum(vb.TotalPassengers)   as TotalPassengers,
          avg(vb.Distance)          as Distance,

          tr.ValidStartDate,
          tr.ValidEndDate
          
  FROM RouteJourneys tr
  INNER JOIN RouteJourneys trd
    ON tr.IdDestination = trd.IdOrigin
  INNER JOIN SalesSearches vb
    ON trd.IdJourney = vb.idJourney AND trd.IdRoute = vb.IdRoute --AND vb.Distance <= 250000
  LEFT JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTowns') }} twno
    ON twno.idtown = tr.IdOrigin
  LEFT JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTowns') }} twnd
    ON twnd.idtown = tr.IdDestination
  LEFT JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTowns') }} twns
    ON twns.idtown = trd.idDestination

  WHERE 
        (
          CURRENT_TIMESTAMP() BETWEEN twno.ValidStartDate AND COALESCE(twno.ValidEndDate, '3000-1-1') OR
          twno.idtown IS NULL
        )
    AND
        (
          CURRENT_TIMESTAMP() BETWEEN twnd.ValidStartDate AND COALESCE(twnd.ValidEndDate, '3000-1-1') OR
          twnd.idtown IS NULL
        )
    AND
        (
          CURRENT_TIMESTAMP() BETWEEN twns.ValidStartDate AND COALESCE(twns.ValidEndDate, '3000-1-1') OR
          twns.idtown IS NULL
        )
    AND (twns.idmunicipality) <> (twno.idmunicipality) AND (twns.idmunicipality) <> (twnd.idmunicipality)

  GROUP BY 
        tr.IdJourney,
        tr.Journey,
        trd.IdJourney,
        trd.Journey,
        tr.OriginTown,
        tr.IdOrigin,
        tr.DestinationTown,
        tr.IdDestination,
        trd.DestinationTown,
        twnd.AutCom,
        twnd.Province,
        twnd.IdMunicipality,
        twnd.MunicipalityName,
        twno.IdMunicipality,
        twno.MunicipalityName,
        twno.Country,
        twno.AutCom,
        twno.Province,
        twns.idtown,
        twns.Country,
        twns.Province,
        twns.MunicipalityName,
        tr.ValidStartDate,
        tr.ValidEndDate
  
  HAVING SUM(vb.TotalPassengers)>0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tr.Journey, twns.MunicipalityName ORDER BY SUM(vb.TotalPassengers) DESC) = 1
)

, OriginMain AS(
  SELECT
          -- Requested Journey
          tr.IdJourney,
          tr.Journey,

          -- Origin
          tr.IdOrigin           as IdOrigin,
          twno.Country          as OriginCountry,
          twno.AutCom           as OriginCA,
          twno.Province         as OriginProvince,
          twno.IdMunicipality   as IdOriginMunicipality,
          twno.MunicipalityName as OriginMunicipalityName,
          tr.OriginTown         as OriginTown,

          -- Destination
          tr.IdDestination      as IdDestination,
          twnd.AutCom           as DestinationCA,
          twnd.Province         as DestinationProvince,
          twnd.IdMunicipality   as IdDestinationMunicipality,
          twnd.MunicipalityName as MunicipalityDestination,
          tr.DestinationTown    as DestinationTown,

          -- Suggestion
          trd.IdJourney         as IdJourneySuggested,
          trd.Journey           as JourneySuggested,
          twns.idtown           as IdDestinationTownSuggested,
          twns.Country          as CountrySuggested,
          twns.Province         as ProvinceSuggested,
          twns.MunicipalityName as MunicipalityNameSuggested,
          trd.DestinationTown   as DestinationTownSuggested,

          sum(vb.TotalPassengers)   as TotalPassengers,
          avg(vb.Distance)          as Distance,

          tr.ValidStartDate,
          tr.ValidEndDate

  FROM RouteJourneys tr
  INNER JOIN RouteJourneys trd
    ON tr.IdOrigin = trd.IdOrigin AND tr.IdDestination <> trd.IdDestination
  INNER JOIN SalesSearches vb
    ON trd.IdJourney = vb.idJourney AND trd.IdRoute = vb.IdRoute
  LEFT JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTowns') }} twno
    ON twno.idtown = tr.IdOrigin
  LEFT JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTowns') }} twnd
    ON twnd.idtown = tr.IdDestination
  LEFT JOIN {{ source('bq_dim_gld_gemini', 'Al_T_Dim_MunicipalityTowns') }} twns
    ON twns.idtown = trd.idDestination

  WHERE 
        (
          CURRENT_TIMESTAMP() BETWEEN twno.ValidStartDate AND COALESCE(twno.ValidEndDate, '3000-1-1') OR
          twno.idtown IS NULL
        )
    AND
        (
          CURRENT_TIMESTAMP() BETWEEN twnd.ValidStartDate AND COALESCE(twnd.ValidEndDate, '3000-1-1') OR
          twnd.idtown IS NULL
        )
    AND
        (
          CURRENT_TIMESTAMP() BETWEEN twns.ValidStartDate AND COALESCE(twns.ValidEndDate, '3000-1-1') OR
          twns.idtown IS NULL
        )
    AND (twns.idmunicipality) <> (twno.idmunicipality) AND (twns.idmunicipality) <> (twnd.idmunicipality)

  GROUP BY 
        tr.IdJourney,
        tr.Journey,
        trd.IdJourney,
        trd.Journey,
        tr.OriginTown,
        tr.IdOrigin,
        tr.DestinationTown,
        tr.IdDestination,
        trd.DestinationTown,
        twnd.AutCom,
        twnd.Province,
        twnd.IdMunicipality,
        twnd.MunicipalityName,
        twno.IdMunicipality,
        twno.MunicipalityName,
        twno.Country,
        twno.AutCom,
        twno.Province,
        twns.idtown,
        twns.Country,
        twns.Province,
        twns.MunicipalityName,
        tr.ValidStartDate,
        tr.ValidEndDate
  
  HAVING SUM(vb.TotalPassengers)>0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tr.Journey, twns.MunicipalityName ORDER BY SUM(vb.TotalPassengers) DESC, AVG(vb.Distance), twns.IdTown) = 1
  ORDER BY 1
)

, OriginSug AS(
  -- Origin Suggestions by Sales and Long Distance
  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested,
    'Origin - Sales' AS TypeSuggestion, CAST(NULL AS STRING) AS Tag, TotalPassengers, Distance, ValidStartDate, ValidEndDate
  FROM OriginMain
  GROUP BY ALL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY IdJourney ORDER BY TotalPassengers DESC, MunicipalityNameSuggested) <= 6
)

, DestShortDist AS(
  -- Destination Suggestions by Sales and Short Distance
  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested, 
    'Destination - Short Distance Sales' AS TypeSuggestion, CAST(NULL AS STRING) AS Tag, TotalPassengers, Distance, ValidStartDate, ValidEndDate
  FROM Main m
  WHERE Distance <= 150000
  GROUP BY ALL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY IdJourney ORDER BY TotalPassengers DESC, MunicipalityNameSuggested) <= 6
)

, DestAnyDist AS(
  -- Destination Suggestions by Sales and Long Distance
  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested, 
    'Destination - Sales' AS TypeSuggestion, CAST(NULL AS STRING) AS Tag, TotalPassengers, Distance, ValidStartDate, ValidEndDate
  FROM Main
  GROUP BY ALL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY IdJourney ORDER BY TotalPassengers DESC, MunicipalityNameSuggested) <= 6
)

, DestTags AS(
  -- Destination Suggestions by Tags
  -- Coastal and Festivals
  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested,
    'Destination - Recommended destinations' AS TypeSuggestion, MAX(ta.Tag) AS Tag, TotalPassengers, Distance, ValidStartDate, ValidEndDate
  FROM Main m
  INNER JOIN AllTags ta
    ON ta.idtown = m.IdDestinationTownSuggested AND Tag IN('IsCoastal', 'Festival') AND Distance <= 150000
  GROUP BY ALL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY IdJourney ORDER BY TotalPassengers DESC, MunicipalityNameSuggested) <= 6

  UNION ALL

  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested, 
    'Destination - Recommended destinations' AS TypeSuggestion, ta.Tag AS Tag, TotalPassengers, Distance, ValidStartDate, ValidEndDate
  FROM Main m
  INNER JOIN TouristTag ta
    ON ta.idtown = m.IdDestinationTownSuggested AND Distance <= 150000
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28, ta.TouristScore
  QUALIFY ROW_NUMBER() OVER (PARTITION BY IdJourney ORDER BY TouristScore DESC, TotalPassengers DESC, MunicipalityNameSuggested) <= 6
)

, AllSuggestions AS(
  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested, 
      TypeSuggestion, Tag, TotalPassengers, Distance

  FROM OriginSug

  UNION ALL

  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested, 
      TypeSuggestion, Tag, TotalPassengers, Distance

  FROM DestShortDist

  UNION ALL

  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested, 
      TypeSuggestion, Tag, TotalPassengers, Distance

  FROM DestAnyDist

  UNION ALL
  
  SELECT 
    IdJourney, Journey, IdOrigin, OriginCountry, OriginCA, OriginProvince, IdOriginMunicipality, OriginMunicipalityName, OriginTown,
    IdDestination, DestinationCA, DestinationProvince, IdDestinationMunicipality, MunicipalityDestination, DestinationTown, 
    IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested, 
      TypeSuggestion, MAX(Tag) AS Tag, TotalPassengers, Distance

  FROM DestTags
  GROUP BY ALL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY IdJourney ORDER BY TotalPassengers DESC) <= 6
)
SELECT
  CAST(Null AS STRING) as IdCluster,
  IdJourney, Journey, L.IdLine, DL.Line, IdOrigin, OriginCountry, OriginCA, OriginProvince, OriginMunicipalityName, AO.Alias AS OriginAlias, OriginTown,
  IdDestination, DestinationCA, DestinationProvince, MunicipalityDestination, AD.Alias AS DestinationAlias, DestinationTown, 
  IdJourneySuggested, JourneySuggested, IdDestinationTownSuggested, CountrySuggested, ProvinceSuggested, MunicipalityNameSuggested, DestinationTownSuggested, 
  TypeSuggestion, S.Tag, TotalPassengers, Distance,
  CASE
    WHEN S.OriginCA IN ('Galicia', 'Principado de Asturias', 'Cantabria', 'País Vasco', 'Comunidad Foral de Navarra') THEN True
    ELSE False
  END AS OriginIsNorth,
  CASE
    WHEN OTC.IdTown IS NOT NULL THEN True
    ELSE False
  END AS OriginIsCoastal,
  CASE
    WHEN OTU.IdTown IS NOT NULL THEN True
    ELSE False
  END AS OriginIsUrban,
  CASE
    WHEN OTU.IdTown IS NULL THEN True
    ELSE False
  END AS OriginIsRural,
  CASE
    WHEN S.DestinationCA IN ('Galicia', 'Principado de Asturias', 'Cantabria', 'País Vasco', 'Comunidad Foral de Navarra') THEN True
    ELSE False
  END AS DestinationIsNorth,
  CASE
    WHEN DTC.IdTown IS NOT NULL THEN True
    ELSE False
  END AS DestinationIsCoastal,
  CASE
    WHEN DTU.IdTown IS NOT NULL THEN True
    ELSE False
  END AS DestinationIsUrban,
  CASE
    WHEN DTU.IdTown IS NULL THEN True
    ELSE False
  END AS DestinationIsRural,

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

FROM AllSuggestions S

-- Origin-Destination Lines
LEFT JOIN Lines L
  ON S.IdOrigin = L.IdOriginL AND S.IdDestination = L.IdDestinationL
LEFT JOIN DimLine DL
  ON DL.IdLine = L.IdLine
-- Origin-Destination Alias
LEFT JOIN TownAlias AO
  ON AO.IdTown = S.IdOrigin
LEFT JOIN TownAlias AD
  ON AD.IdTown = S.IdDestination

-- Origin Tags
LEFT JOIN ( SELECT IdTown FROM AllTags WHERE Tag = 'IsCoastal' ) OTC
  ON S.IdOrigin = OTC.IdTown
LEFT JOIN ( SELECT DISTINCT IdTown FROM AllTags WHERE Tag IN ('IsProvinceCapital', 'IsBigCity') ) OTU
  ON S.IdOrigin = OTU.IdTown

-- Destination Tags
LEFT JOIN ( SELECT IdTown FROM AllTags WHERE Tag = 'IsCoastal' ) DTC
  ON S.IdDestination = DTC.IdTown
LEFT JOIN ( SELECT DISTINCT IdTown FROM AllTags WHERE Tag IN ('IsProvinceCapital', 'IsBigCity') ) DTU
  ON S.IdDestination = DTU.IdTown