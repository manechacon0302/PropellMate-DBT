SELECT 
    DateTravel_PARTITION,
    HashedKey,
    IdService,
    IdExpedition,
    IdItinerary,
    FlgDrivingWork,
    FlgOwnVehicle,
    FlgThirdPartyVehicle,
    FlgOverLap,
    SubSituation,
    IdLine,
    IdVehicleCompany,
    IdDriverCompany,
    IdVehicle,
    LicensePlate,
    IdDriver,
    Year,
    DateTravel,
    DateDeparture,
    NumKilometers,
    OriginData,
    FlgOwnService,
    ReasonDesc,
    ActivityDate

FROM {{ source("bq_fact_silver_com", "Al_T_Fact_ServiceActivity_PW") }}

/*Hisotirc Condition*/
WHERE {{ var("version_date", get_config_value_v4("version_date")) }}
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1') 

/*TO FULL-REFRESH COMMENT THIS PART AND DELETE_WHERE_CLAUSE IN SCHEMA*/
and DateTravel_PARTITION >= {{ var("modif_date", get_config_value_v4("modif_date")) }}    
