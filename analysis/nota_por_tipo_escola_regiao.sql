SELECT 
    uf_escola,
    escola,
    COUNT(inscricao) as total_alunos,
    AVG(nota_mt) as media_matematica,
    AVG(nota_lc) as media_linguagens
FROM 
    fato_enem_dbt AS f
INNER JOIN
    dim_escola_dbt AS d
    ON f.id_escola_sk = d.id_escola_sk
WHERE 
    d.uf_escola IS NOT NULL 
    AND d.escola IS NOT NULL
GROUP BY 
    d.uf_escola, 
    d.escola
ORDER BY 
    d.uf_escola, 
    media_matematica DESC;