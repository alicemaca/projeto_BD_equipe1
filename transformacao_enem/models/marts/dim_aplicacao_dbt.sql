{{ config(materialized='table')}}

WITH base AS(
    SELECT * FROM {{ ref('tratamento_enem')}} 
),

tipos_aplicacao_unicos AS (
    SELECT DISTINCT
        tp_aplicacao_dia_1, 
        tp_aplicacao_dia_2
    FROM base
)

SELECT
    ROW_NUMBER() OVER (ORDER BY tp_aplicacao_dia_1, tp_aplicacao_dia_2) AS id_aplicacao_sk,
    *
FROM tipos_aplicacao_unicos