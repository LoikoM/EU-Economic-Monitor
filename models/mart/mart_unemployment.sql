SELECT
    country_name,
    TO_DATE(time_period || '-01', 'YYYY-MM-DD') AS date,
    obs_value AS unemployment_rate
FROM {{ ref('stg_unemployment') }}