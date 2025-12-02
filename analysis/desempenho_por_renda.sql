SELECT 
    renda_mensal,
    COUNT(inscricao) as total_candidatos,
    (AVG((nota_cn + nota_ch + nota_lc + nota_mt + nota_redacao) / 5), 2) as media_geral_notas,
    (AVG(nota_redacao), 2) as media_redacao,
    (AVG(nota_mt), 2) as media_matematica
FROM 
    fato_enem_dbt AS f
INNER JOIN
	dim_perfil_socioeconomico_dbt AS p
	ON f.id_perfil_socioeconomico_sk = p.id_perfil_socioeconomico_sk
GROUP BY 
    p.renda_mensal
ORDER BY 
    p.renda_mensal ASC;