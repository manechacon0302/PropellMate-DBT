SELECT 
IdZone,
IdDivision,
IdZu,
IdCg,
IdAxis,
IdCompany,
IdEmployee,
IdDriver,
IdNational,
Name,
Surname1,
Surname2,
MaritalStatus,
Sex,
ContractDate,
DateDriverStart,
DateDriverEnd,
DateBirth

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_Driver') }} 

WHERE {{ var("version_date", get_config_value_v4("version_date")) }}
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')

