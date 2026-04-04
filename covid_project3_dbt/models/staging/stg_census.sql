{{ config(materialized='table') }}

WITH cleaned AS (
    SELECT
        -- Base cleaned fields (cast once)
        CAST(NULLIF(TRIM("Total_Population"), '') AS BIGINT) AS total_population,
        CAST(NULLIF(TRIM("Male_Population"), '') AS BIGINT) AS male_population,
        CAST(NULLIF(TRIM("Female_Population"), '') AS BIGINT) AS female_population,

        CAST(NULLIF(TRIM("Under_5_Population"), '') AS BIGINT) AS pop_under_5,
        CAST(NULLIF(TRIM("5_To_9_Population"), '') AS BIGINT) AS pop_5_9,
        CAST(NULLIF(TRIM("10_To_14_Population"), '') AS BIGINT) AS pop_10_14,
        CAST(NULLIF(TRIM("15_To_19_Population"), '') AS BIGINT) AS pop_15_19,
        CAST(NULLIF(TRIM("20_To_24_Population"), '') AS BIGINT) AS pop_20_24,
        CAST(NULLIF(TRIM("25_To_34_Population"), '') AS BIGINT) AS pop_25_34,
        CAST(NULLIF(TRIM("35_To_44_Population"), '') AS BIGINT) AS pop_35_44,
        CAST(NULLIF(TRIM("45_To_54_Population"), '') AS BIGINT) AS pop_45_54,
        CAST(NULLIF(TRIM("55_To_59_Population"), '') AS BIGINT) AS pop_55_59,
        CAST(NULLIF(TRIM("60_To_64_Population"), '') AS BIGINT) AS pop_60_64,
        CAST(NULLIF(TRIM("65_To_74_Population"), '') AS BIGINT) AS pop_65_74,
        CAST(NULLIF(TRIM("75_To_84_Population"), '') AS BIGINT) AS pop_75_84,
        CAST(NULLIF(TRIM("85_Plus_Population"), '') AS BIGINT) AS pop_85_plus,

        CAST(NULLIF(TRIM("White_Population"), '') AS BIGINT) AS pop_white,
        CAST(NULLIF(TRIM("Black_Or_African_American_Population"), '') AS BIGINT) AS pop_black,
        CAST(NULLIF(TRIM("American_Indian_And_Alaska_Native_Population"), '') AS BIGINT) AS pop_native,
        CAST(NULLIF(TRIM("Asian_Population"), '') AS BIGINT) AS pop_asian,
        CAST(NULLIF(TRIM("Native_Hawaiian_And_Other_Pacific_Islander_Population"), '') AS BIGINT) AS pop_pacific,
        CAST(NULLIF(TRIM("Some_Other_Race_Population"), '') AS BIGINT) AS pop_other,

        TRY_CAST(NULLIF(TRIM("state"), '') AS INT) AS state_code,
        TRY_CAST(NULLIF(TRIM("county"), '') AS INT) AS county_code,
        TRY_CAST(NULLIF(TRIM("fips_code"), '') AS INT) AS fips_code

    FROM {{ source('COVID19_DB', 'US_CENSUS_2023') }}
)

SELECT
    *,

    -- Aggregations (clean & readable)
    (pop_under_5 + pop_5_9 + pop_10_14 + pop_15_19) AS pop_0_19,

    (pop_20_24 + pop_25_34 + pop_35_44 + pop_45_54) AS pop_20_54,

    (pop_55_59 + pop_60_64) AS pop_55_64,

    (pop_65_74 + pop_75_84 + pop_85_plus) AS pop_65_plus

FROM cleaned