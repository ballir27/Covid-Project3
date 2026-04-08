WITH fips_cases AS (
    SELECT
        RESIDENT_STATE,
        RESIDENT_COUNTY,
        COUNTY_FIPS,
        COUNT(*) AS Total_Recorded_Cases,
        NULLIF(COUNT(CASE WHEN HOSPITALIZATION_YN = 'Yes' THEN 1 END), 0) AS Hospitalization_Count,
        NULLIF(COUNT(CASE WHEN DEATH_YN = 'Yes' THEN 1 END), 0) AS Death_Count,
    FROM {{ ref('stg_cdc_cases') }}
    GROUP BY RESIDENT_STATE, RESIDENT_COUNTY, COUNTY_FIPS
)
SELECT
    fips_cases.RESIDENT_STATE,
    SUM(fips_cases.Total_Recorded_Cases) AS Total_Recorded_Cases,
    SUM(fips_cases.Hospitalization_Count) AS Hospitalization_Count,
    SUM(fips_cases.Death_Count) AS Death_Count,
    SUM(census.TOTAL_POPULATION) AS population,
    SUM(fips_cases.Total_Recorded_Cases) * 100.0 / SUM(census.TOTAL_POPULATION) AS Cases_Per_Population_Pct,
    SUM(fips_cases.Hospitalization_Count) * 100.0 / SUM(census.TOTAL_POPULATION) AS Hospitalizations_Per_Population_Pct,
    SUM(fips_cases.Death_Count) * 100.0 / SUM(census.TOTAL_POPULATION) AS Deaths_Per_Population_Pct

FROM fips_cases
JOIN {{ref('stg_census')}} AS census
    ON fips_cases.COUNTY_FIPS = census.FIPS_CODE
GROUP BY fips_cases.RESIDENT_STATE