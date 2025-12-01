{{ config(materialized='table')}}

WITH perfil AS(
    SELECT * FROM {{ ref('tratamento_enem')}} 
),

participante AS (SELECT * FROM {{ ref('dim_participante_dbt')}}),
escola AS (SELECT * FROM {{ ref('dim_escola_dbt')}}),
local_prova AS (SELECT * FROM {{ ref('dim_local_prova_dbt')}}),
conclusao AS (SELECT * FROM {{ ref('dim_conclusao_dbt')}}),
aplicacao AS (SELECT * FROM {{ ref('dim_aplicacao_dbt')}}),
perfil_socioeconomico AS (SELECT * FROM {{ ref('dim_perfil_socioeconomico_dbt')}}),
presenca AS (SELECT * FROM {{ ref('dim_presenca_dbt')}})

SELECT

    p.inscricao,
    e.id_escola_sk,
    l.id_local_prova_sk,
    c.id_conclusao_sk,
    a.id_aplicacao_sk,
    ps.id_perfil_socioeconomico_sk,
    pe.id_presenca_sk,
    base.ano,
    base.nota_cn,
    base.nota_ch,
    base.nota_lc,
    base.nota_mt,
    base.nota_redacao 

FROM perfil AS base

LEFT JOIN participante p
    ON base.inscricao = p.inscricao
    AND base.idade = p.idade
    AND base.sexo = p.sexo
    AND base.estado_civil = p.estado_civil
    AND base.cor_raca = p.cor_raca
    AND base.nacionalidade = p.nacionalidade

LEFT JOIN escola e
    ON base.municipio_escola = e.municipio_escola
    AND base.escola = e.escola
    AND base.uf_escola = e.uf_escola
    AND base.dependencia_adm_escola = e.dependencia_adm_escola

LEFT JOIN local_prova l
    ON base.municipio_prova = l.municipio_prova
    AND base.uf_prova = l.uf_prova

LEFT JOIN conclusao c
    ON base.status_conclusao = c.status_conclusao
    AND base.ano_conclusao = c.ano_conclusao
    AND base.ensino = c.ensino
    AND base.eh_treineiro = c.eh_treineiro

LEFT JOIN aplicacao a
    ON base.tp_aplicacao_dia_1 = a.tp_aplicacao_dia_1 
    AND base.tp_aplicacao_dia_2 = a.tp_aplicacao_dia_2

LEFT JOIN perfil_socioeconomico ps
    ON base.escolaridade_pai = ps.escolaridade_pai
    AND base.escolaridade_mae = ps.escolaridade_mae
    AND base.qtde_pessoas_casa = ps.qtde_pessoas_casa
    AND base.renda_mensal = ps.renda_mensal
    AND base.tem_empregado_domestico = ps.tem_empregado_domestico
    AND base.qtd_carro = ps.qtd_carro
    AND base.tem_motocicleta = ps.tem_motocicleta
    AND base.tem_celular = ps.tem_celular
    AND base.qtd_computador = ps.qtd_computador
    AND base.tem_internet = ps.tem_internet

LEFT JOIN presenca pe
    ON base.presenca_1d = pe.presenca_1d
    AND base.presenca_2d = pe.presenca_2d

