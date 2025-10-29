SELECT
  Line,
  TripID,
  TravelDate,
  BusNumber,
  OriginCode,
  OriginName,
  DestinationCode,
  DestinationName,
  SaleDate,
  DaysInAdvance,
  SeatsOffered,
  OccupiedOriginDest,
  OccupiedInitialFinal,
  AvailableOriginDest,
  AvailableInitialFinal,
  InfoDateFrom,
  InfoDateTo,
  StopDate
FROM {{ source('bq_dim_silver_com', 'Al_T_Fact_VehicleOccupancy') }}
WHERE {{ var("version_date", get_config_value_v4("version_date")) }}
  BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
AND DATE(modifdate) >= {{ var("modif_date", get_config_value_v4("modif_date")) }}