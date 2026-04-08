{{ config(materialized='table') }}

WITH fips_cases AS (
    SELECT
        resident_state,
        resident_county,
        county_fips,
        COUNT(*) AS total_recorded_cases,
        COUNT(CASE WHEN hospitalization_status = 'Yes' THEN 1 END)
            AS hospitalization_count,
        COUNT(CASE WHEN death_status = 'Yes' THEN 1 END) AS death_count
    FROM {{ ref('stg_cdc_cases') }}
    WHERE county_fips IS NOT NULL
    GROUP BY resident_state, resident_county, county_fips
),
joined AS (
    SELECT
        fips_cases.resident_state,
        fips_cases.resident_county,
        fips_cases.county_fips,
        fips_cases.total_recorded_cases,
        fips_cases.hospitalization_count,
        fips_cases.death_count,
        census.total_population
    FROM fips_cases
    LEFT JOIN {{ ref('stg_census') }} AS census
        ON fips_cases.county_fips = census.fips_code
)
SELECT
    resident_state,
    resident_county,
    county_fips,
    total_recorded_cases,
    hospitalization_count,
    death_count,
    total_population AS population,
    ROUND(total_recorded_cases * 100.0 / NULLIF(total_population, 0), 4)
        AS cases_per_population_pct,
    ROUND(hospitalization_count * 100.0 / NULLIF(total_population, 0), 4)
        AS hospitalizations_per_population_pct,
    ROUND(death_count * 100.0 / NULLIF(total_population, 0), 4)
        AS deaths_per_population_pct
FROM joined