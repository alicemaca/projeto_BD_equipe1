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

    CASE "TP_ESTADO_CIVIL"
        WHEN 0 THEN 'Não Informado'
        WHEN 1 THEN 'Solteiro'
        WHEN 2 THEN 'União Estável'
        WHEN 3 THEN 'Divorciado'
        WHEN 4 THEN 'Viúvo'
    END as estado_civil,

    CASE "TP_COR_RACA"
        WHEN 0 THEN 'Não declarado'
        WHEN 1 THEN 'Branca'
        WHEN 2 THEN 'Preta'
        WHEN 3 THEN 'Parda'
        WHEN 4 THEN 'Amarela'
        WHEN 5 THEN 'Indígena'
        WHEN 6 THEN 'Não dispõe da informação'
    END as cor_raca,

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

    CASE "TP_ESCOLA"
        WHEN 1 THEN 'Não respondeu'
        WHEN 2 THEN 'Pública'
        WHEN 3 THEN 'Privada'
    END as escola,

    CASE "IN_TREINEIRO"
        WHEN 1 THEN 'Sim'
        WHEN 0 THEN 'Não'
    END as eh_treineiro,

    COALESCE("NO_MUNICIPIO_ESC", 'NÃO INFORMADO') AS municipio_escola,

    COALESCE("SG_UF_ESC", 'NÃO INFORMADO') AS uf_escola,

    CASE CAST(COALESCE("TP_DEPENDENCIA_ADM_ESC", 0) AS integer) 
        WHEN 1 THEN 'Federal'
        WHEN 2 THEN 'Estadual'
        WHEN 3 THEN 'Municipal'
        WHEN 4 THEN 'Privada'
        ELSE 'não informado' 
    END as dependencia_adm_escola,

    "NO_MUNICIPIO_PROVA" AS municipio_prova,

    "SG_UF_PROVA" AS uf_prova,

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
