{{ config(materialized='table')}}

WITH base AS(
    SELECT * FROM {{ ref('tratamento_enem')}} 
),

tipos_unicos AS (
    SELECT DISTINCT
        status_conclusao,
        ano_conclusao,
        ensino,
        eh_treineiro
    FROM base
)

SELECT
    ROW_NUMBER() OVER (ORDER BY status_conclusao, ano_conclusao) AS id_conclusao_sk,
    *
FROM tipos_unicos