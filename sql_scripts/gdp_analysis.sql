SELECT 
    geo AS Country,
    -- GENERATE TABLEAU-READY DATE FORMAT:
    -- Converts quarterly format (e.g., '2021-Q1') into a standard date ('2021-01-01')
    -- to ensure proper time-series visualization in Tableau.
    CASE 
        WHEN SUBSTR(TIME_PERIOD, 6, 2) = 'Q1' THEN SUBSTR(TIME_PERIOD, 1, 4) || '-01-01'
        WHEN SUBSTR(TIME_PERIOD, 6, 2) = 'Q2' THEN SUBSTR(TIME_PERIOD, 1, 4) || '-04-01'
        WHEN SUBSTR(TIME_PERIOD, 6, 2) = 'Q3' THEN SUBSTR(TIME_PERIOD, 1, 4) || '-07-01'
        WHEN SUBSTR(TIME_PERIOD, 6, 2) = 'Q4' THEN SUBSTR(TIME_PERIOD, 1, 4) || '-10-01'
    END AS Date,
    obs_value AS obs_value,
    -- CALCULATE YEAR-OVER-YEAR (YoY) GDP GROWTH:
    -- Formula: ((Current Quarter Value / Same Quarter Previous Year Value) - 1) * 100
    ((OBS_VALUE / prev_value_year - 1) * 100) AS GDP_Growth_YoY
FROM (
    SELECT 
        geo, 
        TIME_PERIOD, 
        OBS_VALUE,
        -- Window Function: Look back 4 quarters to compare with the same period last year
        LAG(OBS_VALUE, 4) OVER (PARTITION BY geo ORDER BY TIME_PERIOD) as prev_value_year
    FROM gdp
)
--Filter out initial records where the 1-year lookback value (LAG) is not available
WHERE prev_value_year IS NOT NULL
ORDER BY Country, TIME_PERIOD;
