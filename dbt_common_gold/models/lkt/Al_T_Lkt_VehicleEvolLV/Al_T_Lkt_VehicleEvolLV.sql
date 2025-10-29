  SELECT 
    IdZone,
    IdZu, 
    Zu,
    IdDivision, 
    Division,
    IdVehicle, 
    IdCompany,
    IdCg, 
    Cg,
    DateVehicleStart, 
    DateVehicleEnd, 
    LicensePlate,
    Class,
    SystemPrincipal,
    SystemSecundary,
    Cam,
    CCTV,
    Tachograph,
    GPS,
    ABC,
    IdCompanyRenting,
    IdCompanyOwner
FROM {{ source('bq_com_slv', 'Al_T_Lkt_VehicleEvolLV_Stg1') }} main
-- {% if is_incremental() %}
--   WHERE 1=1
-- {% else %}
--    WHERE modifdate >= {{ var("modif_date", get_config_value_v4("modif_date")) }}
  
-- {% endif %}
where {{ var("version_date", get_config_value_v4("version_date")) }}
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')

