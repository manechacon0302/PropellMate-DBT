WITH AggSales AS (
  SELECT
    TravelMonthDate,
    IdJourney,
    SUM(COALESCE(Passengers, 0)) AS TotalPassengers
  FROM {{ source('bq_fact_silver_com', 'Al_T_Fact_JourneySales') }}
  WHERE {{ var("version_date", get_config_value_v4("version_date")) }} 
    BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
  AND TravelMonthDate = SAFE_CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()-1) AS INT64)
  GROUP BY ALL
)
, UpdSales AS(
  SELECT
    TravelMonthDate,
    IdJourney,
    TotalPassengers
  FROM AggSales
)
, RouteJourneys AS(
  SELECT DISTINCT
    IdJourney,
    IdRoute,
    Distance
  FROM {{ ref( "Al_T_Dim_RouteJourneys") }}
  WHERE {{ var("version_date", get_config_value_v4("version_date")) }}
    BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
)
, JourneySearches AS(
  SELECT TravelMonthDate, IdJourney, COALESCE(WebSearches, 0) AS WebSearches
  FROM {{ source('bq_fact_silver_com', 'Al_T_Fact_JourneySearches') }}
  WHERE CURRENT_TIMESTAMP()
    BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
)
SELECT
  COALESCE(S.TravelMonthDate, SAFE_CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()-1) AS INT64)) AS TravelMonthDate,
  RJ.IdJourney,
  RJ.IdRoute,
  RJ.Distance,
  COALESCE(S.TotalPassengers, 0) AS TotalPassengers,
  SUM(COALESCE(WebSearches, 0)) AS TotalOnlineSearches,
FROM RouteJourneys RJ
LEFT JOIN UpdSales S
    ON S.IdJourney = RJ.IdJourney
LEFT JOIN JourneySearches B
  ON S.TravelMonthDate = B.TravelMonthDate AND S.IdJourney = B.IdJourney
GROUP BY ALL