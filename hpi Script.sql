
SELECT 
    geo AS Country,
    -- GENERATE TABLEAU-READY DATE FORMAT:
    -- Convert 'YYYY-Q1' to 'YYYY-01-01'
    -- Convert 'YYYY-Q2' to 'YYYY-04-01', and so on.
    CASE 
        WHEN SUBSTR(time_period, 6, 2) = 'Q1' THEN SUBSTR(time_period, 1, 4) || '-01-01'
        WHEN SUBSTR(time_period, 6, 2) = 'Q2' THEN SUBSTR(time_period, 1, 4) || '-04-01'
        WHEN SUBSTR(time_period, 6, 2) = 'Q3' THEN SUBSTR(time_period, 1, 4) || '-07-01'
        WHEN SUBSTR(time_period, 6, 2) = 'Q4' THEN SUBSTR(time_period, 1, 4) || '-10-01'
    END AS Date,
    obs_value as obs_value,
    -- CALCULATE YEAR-OVER-YEAR (YoY) GROWTH RATE:
    -- Formula: ((Current Value / Previous Year Value) - 1) * 100
    ((obs_value / prev_value - 1) * 100) AS Growth_YoY
FROM (
    SELECT 
        geo, 
        time_period, 
        obs_value,
        LAG(obs_value, 4) OVER (PARTITION BY geo ORDER BY time_period) as prev_value
    FROM hpi
)
WHERE prev_value IS NOT NULL
ORDER BY Country, time_period;