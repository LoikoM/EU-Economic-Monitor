SELECT
    country_name,
    TO_DATE(
        SPLIT_PART(time_period, '-Q', 1) || '-' ||
        CASE SPLIT_PART(time_period, '-Q', 2)
            WHEN '1' THEN '01'
            WHEN '2' THEN '04'
            WHEN '3' THEN '07'
            WHEN '4' THEN '10'
        END || '-01', 'YYYY-MM-DD'
    ) AS date,
    obs_value,
    ROUND(
        (obs_value - LAG(obs_value, 4) OVER (PARTITION BY country_code ORDER BY date)) 
        / LAG(obs_value, 4) OVER (PARTITION BY country_code ORDER BY date) * 100
    , 2) AS hpi_growth_yoy
FROM {{ ref('stg_hpi') }}