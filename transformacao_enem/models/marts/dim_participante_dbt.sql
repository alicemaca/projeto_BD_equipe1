{{ config(materialized='table')}}

WITH base AS(
    SELECT * FROM {{ ref('tratamento_enem')}} 
),

perfis_unicos AS (
    SELECT DISTINCT
        inscricao,
        idade,
        sexo,
        estado_civil,
        cor_raca,
        nacionalidade
    FROM base
)

SELECT
    ROW_NUMBER() OVER (ORDER BY inscricao),
    *
FROM perfis_unicos