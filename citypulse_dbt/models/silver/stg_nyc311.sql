
{{ config(materialized='table') }}

SELECT
    unique_key,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(closed_date AS TIMESTAMP) AS closed_date,
    TRIM(agency) AS agency,
    TRIM(complaint_type) AS complaint_type,
    UPPER(TRIM(borough)) AS borough,
    status
FROM read_files(
    'r2://citypulse-bronze@72f1f0ca3c53f9db09ed9595bd8f3b62.r2.cloudflarestorage.com/bronze/nyc311/
nyc311_export.parquet'
)
WHERE unique_key IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY unique_key
    ORDER BY created_date DESC
) = 1
