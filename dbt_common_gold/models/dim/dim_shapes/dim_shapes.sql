SELECT 
IdCompany,
IdLine,
IdItinerary, 
Latitude,
Longitude,
IdSequence,
DistTraveled,
Id,
Geom,
Valid,
CreateDateOrigin
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
FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_Shapes') }} 