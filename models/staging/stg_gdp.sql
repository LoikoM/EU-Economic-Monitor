WITH raw AS (
    SELECT json_content
    from {{ source('eurostat_raw', 'EUROSTAT_RAW') }}
    WHERE dataset_name = 'gdp'
),

geo_flat AS (
    SELECT
        key AS country_code,
        value::INT AS country_idx
    FROM raw,
    LATERAL FLATTEN(input => json_content:dimension:geo:category:index)
),

time_flat AS (
    SELECT
        key AS time_period,
        value::INT AS time_idx
    FROM raw,
    LATERAL FLATTEN(input => json_content:dimension:time:category:index)
),

num_periods AS (
    SELECT COUNT(*) AS n
    FROM time_flat
)

SELECT
    g.country_code,
    r.json_content:dimension:geo:category:label[g.country_code]::STRING AS country_name,
    t.time_period,
    r.json_content:value[CAST(g.country_idx * p.n + t.time_idx AS STRING)]::FLOAT AS obs_value
FROM raw r
CROSS JOIN geo_flat g
CROSS JOIN time_flat t
CROSS JOIN num_periods p
WHERE r.json_content:value[CAST(g.country_idx * p.n + t.time_idx AS STRING)] IS NOT NULL