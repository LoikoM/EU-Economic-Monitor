SELECT 
    geo AS Country,
    -- FORMAT DATE FOR TABLEAU:
    -- Ensures consistency with other datasets by using the 'YYYY-MM-01' format
    TIME_PERIOD || '-01' AS Date,
    -- Unemployment rate value
    OBS_VALUE AS Unemployment_Rate
 
FROM une
WHERE TIME_PERIOD >= '2021-01'
  -- DATA CLEANING:
  -- Filter by a specific age group to prevent duplicate entries for the same month/country
  AND age = 'From 25 to 74 years' 
   
ORDER BY Country, TIME_PERIOD;