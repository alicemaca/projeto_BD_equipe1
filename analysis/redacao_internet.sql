SELECT 
    tem_internet,
    qtd_computador,
    COUNT(inscricao) as qtd_inscritos,
    AVG((nota_cn + nota_ch + nota_lc + nota_mt + nota_redacao) / 5) as media_geral
FROM 
    fato_enem_dbt AS f
INNER JOIN
	dim_perfil_socioeconomico_dbt AS p
	ON f.id_perfil_socioeconomico_sk = p.id_perfil_socioeconomico_sk
GROUP BY 
    p.tem_internet, 
    p.qtd_computador
ORDER BY 
    media_geral DESC;