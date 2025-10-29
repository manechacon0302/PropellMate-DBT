SELECT 
IdVehicle,
IdTypeOrigin,
LicensePlate,
DateVehicleStart,
DateVehicleEnd,
Owned,
VehicleType,
IdBrand,
IdModel,
Seats,
SerialNumber,
Size,
RegistrationDate,
IdFuel,
VehicleClasification

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_Vehicle') }} 

{% if is_incremental() %}
   WHERE modifdate >= {{ var("modif_date", get_config_value_v4("modif_date")) }}
{% else %}
  WHERE 1=1
{% endif %}
AND {{ var("version_date", get_config_value_v4("version_date")) }}
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')

