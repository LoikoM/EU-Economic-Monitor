SELECT 
    geo AS Country,
    -- FORMAT DATE FOR TABLEAU:
    -- Appends '-01' to convert 'YYYY-MM' into a standard 'YYYY-MM-01' date format
    TIME_PERIOD || '-01' AS Date,
    -- The value is already expressed as an annual percentage change (Inflation Rate)
    OBS_VALUE AS Inflation_Rate
FROM hicp
WHERE TIME_PERIOD >= '2021-01'
ORDER BY Country, TIME_PERIOD;