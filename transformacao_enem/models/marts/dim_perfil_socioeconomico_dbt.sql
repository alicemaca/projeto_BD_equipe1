{{ config(materialized='table')}}

WITH base AS(
    SELECT * FROM {{ ref('tratamento_enem')}} 
),

perfis_socioeconomicos_unicos AS (
    SELECT DISTINCT
        escolaridade_pai,
        escolaridade_mae,
        qtde_pessoas_casa,
        renda_mensal,
        tem_empregado_domestico,
        qtd_carro,
        tem_motocicleta,
        tem_celular,
        qtd_computador,
        tem_internet
    FROM base
)

SELECT
    ROW_NUMBER() OVER (ORDER BY renda_mensal) AS id_perfil_socioeconomico_sk,
    *
FROM perfis_socioeconomicos_unicos