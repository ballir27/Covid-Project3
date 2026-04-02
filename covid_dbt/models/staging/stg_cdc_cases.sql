SELECT
    CAST(NULLIF(TRIM("CASE_MONTH"), '') || '-01' AS DATE) AS Case_Month,
    NULLIF(TRIM("RES_STATE"), '') AS Resident_State,
    TRY_CAST(NULLIF(TRIM("STATE_FIPS_CODE"), '') AS INT) AS State_FIPS,
    NULLIF(TRIM("RES_COUNTY"), '') AS Resident_County,
    TRY_CAST(NULLIF(TRIM("COUNTY_FIPS_CODE"), '') AS INT) AS County_FIPS,
    NULLIF(TRIM("AGE_GROUP"), '') AS Age_Group,
    NULLIF(TRIM("SEX"), '') AS Gender,
    NULLIF(TRIM("RACE"), '') AS Race,
    NULLIF(TRIM("ETHNICITY"), '') AS Ethnicity,
    NULLIF(TRIM("CASE_ONSET_INTERVAL"), '') AS Case_Onset_Interval,
    NULLIF(TRIM("PROCESS"), '') AS Process_Type,
    CASE
        WHEN NULLIF(TRIM("EXPOSURE_YN"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("EXPOSURE_YN"), '') = 'No' THEN 'No'
        ELSE 'Missing or Unknown'
    END AS Exposure_YN,
    NULLIF(TRIM("CURRENT_STATUS"), '') AS Case_Status,
    NULLIF(TRIM("SYMPTOM_STATUS"), '') AS Symptom_Status,
    CASE
        WHEN NULLIF(TRIM("HOSP_YN"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("HOSP_YN"), '') = 'No' THEN 'No'
        ELSE 'Missing or Unknown'
    END AS Hospitalization_YN,
    CASE
        WHEN NULLIF(TRIM("ICU_YN"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("ICU_YN"), '') = 'No' THEN 'No'
        ELSE 'Missing or Unknown'
    END AS ICU_YN,
    CASE
        WHEN NULLIF(TRIM("DEATH_YN"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("DEATH_YN"), '') = 'No' THEN 'No'
        ELSE 'Missing or Unknown'
    END AS Death_YN,
    NULLIF(TRIM("CASE_POSITIVE_SPECIMEN"), '') AS Positive_Specimen_Interval,
    CASE
        WHEN NULLIF(TRIM('UNDERLYING_CONDITIONS_YN'), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM('UNDERLYING_CONDITIONS_YN'), '') = 'No' THEN 'No'
        ELSE NULL
    END AS Underlying_Conditions_YN,
    NULLIF(TRIM("UNIQUE_KEY"), '') AS Unique_Key
FROM {{ source('COVID19_DB', 'CDC_RAW_CASES_1') }}