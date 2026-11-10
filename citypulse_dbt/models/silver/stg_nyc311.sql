
{{ config(materialized='table') }}

SELECT
    CAST(`Unique Key` AS STRING) AS unique_key,
    TO_TIMESTAMP(`Created Date`, 'MM/dd/yyyy hh:mm:ss a') AS created_date,
    TO_TIMESTAMP(`Closed Date`, 'MM/dd/yyyy hh:mm:ss a') AS closed_date,
    TRIM(Agency) AS agency,
    TRIM(`Problem (formerly Complaint Type)`) AS complaint_type,
    UPPER(TRIM(Borough)) AS borough,
    Status AS status
FROM read_files(
    'r2://citypulse-bronze@72f1f0ca3c53f9db09ed9595bd8f3b62.r2.cloudflarestorage.com/bronze/nyc311/',
    format => 'parquet'
)
WHERE `Unique Key` IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY `Unique Key`
    ORDER BY `Created Date` DESC
) = 1
