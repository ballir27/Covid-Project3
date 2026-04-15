SELECT
    CAST(NULLIF(TRIM("case_month"), '') || '-01' AS DATE) AS Case_Month,
    NULLIF(TRIM("res_state"), '') AS Resident_State,
    TRY_CAST(NULLIF(TRIM("state_fips_code"), '') AS INT) AS State_FIPS,
    NULLIF(TRIM("res_county"), '') AS Resident_County,
    TRY_CAST(NULLIF(TRIM("county_fips_code"), '') AS INT) AS County_FIPS,
    NULLIF(TRIM("age_group"), '') AS Age_Group,
    NULLIF(TRIM("sex"), '') AS Gender,
    NULLIF(TRIM("race"), '') AS Race,
    NULLIF(TRIM("ethnicity"), '') AS Ethnicity,
    NULLIF(TRIM("case_onset_interval"), '') AS Case_Onset_Interval,
    NULLIF(TRIM("process"), '') AS Process_Type,
    CASE
        WHEN NULLIF(TRIM("exposure_yn"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("exposure_yn"), '') = 'No' THEN 'No'
        ELSE 'Missing or Unknown'
    END AS Exposure_YN,
    NULLIF(TRIM("current_status"), '') AS Case_Status,
    NULLIF(TRIM("symptom_status"), '') AS Symptom_Status,
    CASE
        WHEN NULLIF(TRIM("hosp_yn"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("hosp_yn"), '') = 'No' THEN 'No'
        ELSE 'Missing or Unknown'
    END AS Hospitalization_YN,
    CASE
        WHEN NULLIF(TRIM("icu_yn"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("icu_yn"), '') = 'No' THEN 'No'
        ELSE 'Missing or Unknown'
    END AS ICU_YN,
    CASE
        WHEN NULLIF(TRIM("death_yn"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("death_yn"), '') = 'No' THEN 'No'
        ELSE 'Missing or Unknown'
    END AS Death_YN,
    NULLIF(TRIM("case_positive_specimen"), '') AS Positive_Specimen_Interval,
    CASE
        WHEN NULLIF(TRIM("underlying_conditions_yn"), '') = 'Yes' THEN 'Yes'
        WHEN NULLIF(TRIM("underlying_conditions_yn"), '') = 'No' THEN 'No'
        ELSE NULL
    END AS Underlying_Conditions_YN,
    NULLIF(TRIM("unique_key"), '') AS Unique_Key
FROM {{ source('COVID19_DB', 'CDC_RAW_CASES_1') }}