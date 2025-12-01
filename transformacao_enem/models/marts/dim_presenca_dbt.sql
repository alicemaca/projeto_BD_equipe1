{{ config(materialized='table')}}

WITH base AS(
    SELECT * FROM {{ ref('tratamento_enem')}} 
),

tipos_presenca_unicos AS (
    SELECT DISTINCT
        presenca_1d,
        presenca_2d
    FROM base
)

SELECT
    ROW_NUMBER() OVER (ORDER BY presenca_1d, presenca_2d) AS id_presenca_sk,
    *
FROM tipos_presenca_unicos