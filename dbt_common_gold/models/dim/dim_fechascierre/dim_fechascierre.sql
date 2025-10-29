SELECT 
  COALESCE(T2.Period, T1.Period) AS Period,    
  COALESCE(T2.PeriodNumber, T1.PeriodNumber) AS PeriodNumber,
  COALESCE(T2.Year, T1.Year) AS Year,    
  CASE 
    WHEN COALESCE(T2.MonthStartDate, T1.MonthStartDate) = 0 THEN 19000101 
    ELSE COALESCE(T2.MonthStartDate, T1.MonthStartDate) 
  END AS MonthStartDate,    
  CASE 
    WHEN COALESCE(T2.MonthEndDate, T1.MonthEndDate) = 0 THEN 19000131 
    ELSE COALESCE(T2.MonthEndDate, T1.MonthEndDate) 
  END AS MonthEndDate, 
  COALESCE(T2.ClosingDate, T1.ClosingDate) AS ClosingDate,
  COALESCE(T2.IdReport, T1.IdReport) AS IdReport
FROM `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_ClosingDates` AS T1
LEFT JOIN `prj-gld-dev-westeu-01.bq_com_test_westeu_01.Al_T_Dim_ClosingDates_Manual` AS T2
  ON T1.PeriodNumber = T2.PeriodNumber AND T1.IdReport = T2.IdReport

WHERE CURRENT_TIMESTAMP() 
BETWEEN ValidStartDate AND COALESCE(ValidEndDate, '3000-1-1')