 
SELECT  
Date,
DateInt,
Year,
YearMonth,
Month,
--NOM_MES         as MonthName,
Day,
--NOM_DIA         as DayName,
DayWeek,
Semester,
Quarter,
Week,
WeekStart,
WeekEnd

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
FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_Calendar') }} 