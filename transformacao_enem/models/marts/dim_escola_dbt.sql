{{ config(materialized='table')}}

WITH base AS(
    SELECT * FROM {{ ref('tratamento_enem')}} 
),

escolas_unicas AS (
    SELECT DISTINCT
        escola,
        municipio_escola,
        uf_escola,
        dependencia_adm_escola
    FROM base
)

SELECT
    ROW_NUMBER() OVER (ORDER BY municipio_escola, uf_escola) AS id_escola_sk,
    *
FROM escolas_unicas