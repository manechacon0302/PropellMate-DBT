SELECT
  ThirdPartyCode,
  CompanyCode,
  ThirdPartyName,
  NIF,
  LongDescription,
  AcronymCode,
  Address,
  Number,
  PostalCode,
  GroupCode,
  CountryCode,
  ProvinceCode,
  MunicipalityCode,
  Phone1,
  Phone2,
  Phone3,
  Fax,
  Email,
  ZoneCode,
  UnifiedNIF,
  GlobalNIF,
  StartDate,
  UpdateDate

FROM {{ source('bq_dim_silver_com', 'Al_T_Dim_AlsaProviders') }}
WHERE {{ var("version_date", get_config_value_v4("version_date")) }}
  BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')
AND DATE(modifdate) >= {{ var("modif_date", get_config_value_v4("modif_date")) }}