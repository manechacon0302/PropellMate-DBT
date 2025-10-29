select 
IdZone,
IdZu,
IdDivision,
IdCompany,
IdVehicle,
IdAxis,
IdCg,
IdTypeOrigin,
DateVehicleStart,
DateVehicleEnd,
LicensePlate,
Class,
SystemPrincipal,
SystemSecundary,
SystemMonitoring,
Cam,
CCTV,
Tachograph,
GPS,
ABC,
IdCompanyRenting,
IdCompanyOwner,
BatteryType,
Owned


FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_VehicleEvol') }} 


WHERE {{ var("version_date", get_config_value_v4("version_date")) }}
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')

