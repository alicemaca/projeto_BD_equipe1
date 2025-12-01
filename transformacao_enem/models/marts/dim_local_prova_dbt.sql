{{ config(materialized='table')}}

WITH base AS(
    SELECT * FROM {{ ref('tratamento_enem')}} 
),

locais_unicos AS (
    SELECT DISTINCT
        municipio_prova,
        uf_prova
    FROM base
)

SELECT
    ROW_NUMBER() OVER (ORDER BY municipio_prova, uf_prova) AS id_local_prova_sk,
    *
FROM locais_unicos