{{ config(
    materialized='view'
) }}

with enem AS (
    SELECT * FROM {{ ref('enem_unificado') }}
)

SELECT 
    "NU_INSCRICAO" AS inscricao,
    "NU_ANO" AS ano,
    "TP_SEXO" AS sexo,
    "NO_MUNICIPIO_PROVA" AS municipio_prova,
    "SG_UF_PROVA" AS uf_prova,

    CASE "TP_NACIONALIDADE"
        WHEN 0 THEN 'não informado'
        WHEN 1 THEN 'brasileiro'
        WHEN 2 THEN 'naturalizado'
        WHEN 3 THEN 'estrangeiro'
        WHEN 4 THEN 'brasileiro nato'
    END as nacionalidade,

    CASE "TP_ST_CONCLUSAO"
        WHEN 1 THEN 'concluido'
        WHEN 2 THEN 'cursando'
        WHEN 3 THEN 'cursando'
        WHEN 4 THEN 'não inciado'
    END as status_conclusao,

    CASE "Q002"
        WHEN 'A' THEN 'Nunca estudou'
        WHEN 'B' THEN 'Não completou o ensino fundamental 1'
        WHEN 'C' THEN 'Não completou o ensino fundamental 2'
        WHEN 'D' THEN 'Não completou o ensino médio'
        WHEN 'E' THEN 'Não completou a faculdade'
        WHEN 'F' THEN 'Não completou a pós-graduação'
        WHEN 'G' THEN 'Completou a pós-graduação'
        WHEN 'H' THEN 'Não sei'
        ELSE 'Não informado'
    END as escolaridade_mae

FROM enem
