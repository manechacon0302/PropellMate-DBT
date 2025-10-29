SELECT
  Date,
  DateTravel,
  DateDeparture,
  IdDivision,
  IdCg,
  IdZu,
  IdZone,
  IdDriver,
  IdEmployee,
  IdVehicle,
  LicensePlate,
  IdVehicleCompany,
  TotalKilometers,
  TotalHours,
  ActivityDate
FROM (
  SELECT * FROM  {{ source("bq_agg_silver_com", "Al_T_Agg_Activity_Date") }}
  WHERE
    CURRENT_TIMESTAMP() BETWEEN ValidStartDate
    AND COALESCE(ValidEndDate, '3000-1-1') 
  
     ) 