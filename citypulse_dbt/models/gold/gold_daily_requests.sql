
{{ config(materialized='table') }}

SELECT
    CAST(created_date AS DATE) AS request_date,
    borough,
    COUNT(*) AS total_request,
    COUNT_IF(closed_date IS NOT NULL) AS closed_requests,
    COUNT_IF(closed_date IS NULL) AS open_requests
FROM {{ ref('stg_nyc311') }}
GROUP BY
    CAST(created_date AS DATE),
    borough
