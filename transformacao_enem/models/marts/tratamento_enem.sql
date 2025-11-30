{{ config(
    materialized='view'
) }}

with enem AS (
    SELECT * FROM {{ ref('enem_unificado') }}
)

SELECT *
FROM enem