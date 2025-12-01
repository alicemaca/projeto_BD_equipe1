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

    CASE "TP_ENSINO" 
        WHEN 1 THEN 'Ensino Regular'
        WHEN 2 THEN 'Educação Especial - Modalidade Substitutiva'
        ELSE 'Não informado'
    END as ensino,

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

    CASE 
        WHEN "CO_PROVA_LC" IN (889, 890, 891, 892, 1065, 1066, 1067, 1068, 1201, 1202, 1203, 1204) THEN 'Regular'
        WHEN "CO_PROVA_LC" IN (895, 1207) THEN 'Braile'
        WHEN "CO_PROVA_LC" IN (896, 1072, 1208) THEN 'Ledor'
        WHEN "CO_PROVA_LC" IN (897, 1073, 1209) THEN 'Libras'
        WHEN "CO_PROVA_LC" IN (969, 970, 971, 972, 976, 1145, 1146, 1147, 1148, 1281, 1282, 1283, 1284) THEN 'Reaplicação'
        WHEN "CO_PROVA_LC" IN (1003, 1004, 1005, 1006, 1179, 1180, 1181, 1182) THEN 'Digital'
        WHEN "CO_PROVA_LC" IN (1025, 1026, 1027, 1028, 1032) THEN 'Segunda oportunidade'
        WHEN "CO_PROVA_LC" IN (1205, 1206) THEN 'Ampliada'
        WHEN "CO_PROVA_LC" = 0 THEN 'Não entregue' 
        
    END AS tp_aplicacao_dia_1,

    CASE 
        WHEN "CO_PROVA_MT" IN (899, 900, 901, 902, 1075, 1076, 1077, 1078, 1211, 1212, 1213, 1214) THEN 'Regular'
        WHEN "CO_PROVA_MT" IN (905, 1217) THEN 'Braile'
        WHEN "CO_PROVA_MT" IN (906, 1082, 1218) THEN 'Ledor'
        WHEN "CO_PROVA_MT" IN (907, 1083, 1219) THEN 'Libras'
        WHEN "CO_PROVA_MT" IN (979, 980, 981, 982, 986, 1155, 1156, 1157, 1158, 1291, 1292, 1293, 1294) THEN 'Reaplicação'
        WHEN "CO_PROVA_MT" IN (1007, 1008, 1009, 1010, 1183, 1184, 1185, 1186) THEN 'Digital'
        WHEN "CO_PROVA_MT" IN (1035, 1036, 1037, 1038, 1042) THEN 'Segunda oportunidade'
        WHEN "CO_PROVA_MT" IN (1215, 1216) THEN 'Ampliada'
        WHEN "CO_PROVA_MT" = 0 THEN 'Não entregue' 
        
    END AS tp_aplicacao_dia_2,

    COALESCE("NU_NOTA_CN", 0) AS nota_cn,
    COALESCE("NU_NOTA_CH", 0) AS nota_ch,
    COALESCE("NU_NOTA_LC", 0) AS nota_lc,
    COALESCE("NU_NOTA_MT", 0) AS nota_mt,
    COALESCE("NU_NOTA_REDACAO", 0) AS nota_redacao,
    
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
    END as escolaridade_mae,

    CASE "Q006"
        WHEN 'A' THEN 'Nenhuma renda'
        WHEN 'B' THEN 'Até R$ 1.320,00'
        WHEN 'C' THEN 'De R$ 1.320,01 até R$ 3.400,00'
        WHEN 'D' THEN 'De R$ 1.320,01 até R$ 3.400,00'
        WHEN 'E' THEN 'De R$ 1.320,01 até R$ 3.400,00'
        WHEN 'F' THEN 'De R$ 1.320,01 até R$ 3.400,00'
        WHEN 'G' THEN 'De R$ 3.400,01 até R$ 8.100,00'
        WHEN 'H' THEN 'De R$ 3.400,01 até R$ 8.100,00'
        WHEN 'I' THEN 'De R$ 3.400,01 até R$ 8.100,00'
        WHEN 'J' THEN 'De R$ 3.400,01 até R$ 8.100,00'
        WHEN 'K' THEN 'De R$ 8.100,01 até R$ 25.200,00'
        WHEN 'L' THEN 'De R$ 8.100,01 até R$ 25.200,00'
        WHEN 'M' THEN 'De R$ 8.100,01 até R$ 25.200,00'
        WHEN 'N' THEN 'De R$ 8.100,01 até R$ 25.200,00'
        WHEN 'O' THEN 'De R$ 8.100,01 até R$ 25.200,00'
        WHEN 'P' THEN 'De R$ 8.100,01 até R$ 25.200,00'
        WHEN 'Q' THEN 'Acima de R$ 25.200,01'

    END as renda_mensal,

    CASE "Q007"
        WHEN 'A' THEN 'Não'
        WHEN 'B' THEN 'Sim, um ou dois dias por semana'
        WHEN 'C' THEN 'Sim, três ou quatro dias por semana'
        WHEN 'D' THEN 'Sim, pelo menos cinco dias por semana'
    END as tem_empregado_domestico,

    CASE "Q010"
        WHEN 'A' THEN 'Não'
        WHEN 'B' THEN 'Sim, um'
        WHEN 'C' THEN 'Sim, dois'
        WHEN 'D' THEN 'Sim, três'
        WHEN 'E' THEN 'Sim, quatro ou mais'
    END as qtd_carro,

    CASE "Q022"
        WHEN 'A' THEN 'Não'
        WHEN 'B' THEN 'Sim, um'
        WHEN 'C' THEN 'Sim, dois'
        WHEN 'D' THEN 'Sim, três'
        WHEN 'E' THEN 'Sim, quatro ou mais'
    END as tem_celular,

    CASE "Q024"
        WHEN 'A' THEN 'Não.'
        WHEN 'B' THEN 'Sim, um.'
        WHEN 'C' THEN 'Sim, dois.'
        WHEN 'D' THEN 'Sim, três.'
        WHEN 'E' THEN 'Sim, quatro ou mais.'
    END as qtd_computador

FROM enem
