SELECT 
      IdVehicle, 
      LicensePlate, 
      System, 
      Registers, 
      TimestampInit, 
      TimestampEnd
FROM {{ source('bq_com_slv', 'Al_T_Lkt_SecondaryVehicleProvider') }}
{% if is_incremental() %}
   WHERE modifdate >= {{ var("modif_date", get_config_value_v4("modif_date")) }}
{% else %}
  WHERE 1=1
{% endif %}
AND {{ var("version_date", get_config_value_v4("version_date")) }}
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')