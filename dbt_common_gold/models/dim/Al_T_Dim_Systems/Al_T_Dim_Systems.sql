SELECT 

IdExplot               as IdExplot,
Description            as Description,
IdCompany              as IdCompany,
IdZone                 as IdZone,
IdOfficialConcession   as IdOfficialConcession,
ObtTablet              as ObtTablet,
Latitude               as Latitude,
Longitude              as Longitude,
Strategy               as Strategy,
TimeZone               as TimeZone,
Rti                    as Rti,
Tla                    as Tla,
Cadence                as Cadence

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_Systems') }} 

/* Historic Condition */
WHERE {{ var("version_date", get_config_value_v4("version_date")) }} BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
