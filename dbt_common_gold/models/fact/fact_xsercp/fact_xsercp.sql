SELECT 
      -- estructura operativa
      IdCountry, 
      IdZone, 
      IdDivision, 
      IdUZ, 
      IdAxis,
      IDZoneManagement,
      -- horas viaje
      TravelDate,
      TravelDateINT,
      DepartureDate, 
      ArrivalDate,
      DepartureDateUTC, 
      ArrivalDateUTC, 
      -- empresa
      IdCompanyLine,
      -- servicio
      Registration,
      LineCategory,
      IdLine,     
      IdExpedition,
      IdItinerary,
      Discretion,
      Source,
      Destination,
      DepartureTime, 
      ArrivalTime ,
      Service,
      NumberDays,
      Outbound,
      Time,
      KMS,
      -- coche
      Vehicle,
      LicensePlate,  
      CompanyVehicle,
      Reinforcement,
      Coach,
      CoachShift,
      -- conductor 1
      IdDriver,
      IdCompanyDriver,
      DriverShift,     
      -- conductor 2
      IdDriver2,
      IdCompanyDriver2

-- CAMPOS ODS  
, ValidStartDate 
, ValidEndDate
-- AUDITORIA
      , CURRENT_TIMESTAMP() as CreateDate
      , 'DBT' CreateUser
      , null ModifDate
      , null ModifUser
      , null DeleteDate
      , null DeleteUser
FROM {{ source('bq_fact_silver_com', 'Al_T_Fact_Planner') }} 