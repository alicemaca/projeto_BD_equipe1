# 🎲Microdados do ENEM: 
> Pipeline de dados do ENEM dos anos 2021, 2022 e 2023

> Projeto de integração para a matéria Banco de Dados (2025.2) - CIn UFPE
***
## 🎯Objetivos gerais:
O objetivo do projeto foi integrar dados de 3 anos diferentes para permitir análises mais completas.
Esse projeto utiliza o ETL e ELT para processar, transformar e modelar os microdados do ENEM. Além disso, foi utilizado o esquema estrela para otimizar e facilitar o uso. Foram utilizados 200.000 linhas de cada ano para compor o projeto.
- Fonte: [GOV](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem)
- Dados Brutos: Arquivos CSV separados por ano (2021, 2022 e 2023) contendo registros dos alunos, perguntas socioeconômicas e resultados.
***
## 🏛️Arquitetura da solução:
### ETL:
- Extração: Carregamento das tabelas através da junção dos arquivos csv em um único dataframe.
- Transformação: Deleção e renomeação de colunas, preenchimento de valores nulos e substituição de valores pela legenda usando Pandas
- Carga: Inserção dos dados transformados no PostgreSQL usando SQLAlchemy.
### ELT:
- Extração e carga: Carregamento dos dados brutos no PostgreSQL com Python
- Transformação: Deleção e renomeação de colunas, preenchimento de valores nulos e substituição de valores pela legenda usando SQL diretamente no banco de dados.
***
## 💫Modelagem de dados:
Ao final do pipeline, os dados são organizados em um modelo dimensional, esquema estrela, para facilitar análises:
| Tabela | Tipo | Descrição |
| :--- | :--- | :--- |
| fato\_enem | Fato | Registro de cada participação no ENEM, contendo as notas por área de conhecimento (Métricas) e as chaves estrangeiras para as dimensões. |
| dim\_aplicacao | Dimensão | Detalhes sobre a Aplicação da Prova |
| dim\_conclusao | Dimensão | Detalhes sobre a Situação de Conclusão do Ensino Médio pelo participante |
| dim\_escola | Dimensão | Informações sobre a Escola de Conclusão do Ensino Médio |
| dim\_local\_prova | Dimensão | Localização geográfica onde a prova foi realizada |
| dim\_participante | Dimensão | Perfil Demográfico do aluno |
| dim\_perfil\_socioeconomico | Dimensão | Respostas detalhadas ao Questionário Socioeconômico do participante |
| dim\_presenca| Dimensão | Status de Presença/Ausência do participante em cada dia de prova |
***
## 💻Tecnologias utilizadas:
- Python 3.10+: Scripting e manipulação de dados (Pandas).
- PostgreSQL: Data Warehouse.
- dbt Core: Orquestração de transformações SQL e testes de dados.
- SQLAlchemy & Psycopg2: Conectores de banco de dados.
- Git/GitHub: Versionamento de código.
  ***
  ## 📁Estrutura do repositório:
```
├── logs/
├── notebooks/
│   ├── dados/
│   │   ├── MICRODADOS_ENEM_2021_CORTADOS.csv
│   │   ├── MICRODADOS_ENEM_2022_CORTADOS.csv
│   │   └── MICRODADOS_ENEM_2023_CORTADOS.csv
│   ├── ELT.ipynb
│   └── ELT.ipynb
├── seeds/
├── transformacao_enem/
│   ├── analyses/
│   ├── logs/
│   ├── macros/
│   ├── models/
│   │   ├── example/
│   │   ├── marts/
│   │   │   └── tratamento_enem.sql
│   │   └── staging/
│   │       ├── enem_unificado.sql
│   │       └── schema.yml
│   ├── seeds/
│   ├── snapshots/
│   ├── target/
│   ├── tests/
│   ├── .gitignore
│   ├── dbt_project.yml
│   └── README.md
├── venv/
└── .gitignore
```

## 👩🏽‍💻Como executar:
### Pré-requisitos:
- Instale Python e PostgreSQL.
- Instale o Git lfs:
  ```
  git lfs install
  ```
  *caso o import dos arquivos com o git lfs não funcione, em razão de falta de limite de memória do git lfs do repositório, pois possuimos a conta gratuita, baixe os arquivos nesse link: [arquivo](https://drive.google.com/drive/folders/1sh_EMdV9SkNrfatwNt32xAoAhtHJ3Fhs?usp=drive_link) e inclua eles na pasta "notebooks/dados" do seu clone.
- Clone este repositório.
- Instale as dependências citadas nos arquivos ELT.ipynb e ETL.ipynb
### Configuração de ambiente:
- Preencha o arquivo ELT.ipynb com suas credenciais do PostgreSQL
- Configure o dbt
- Crie um database no pgAdmin 4 chamada "transformacao_enem"
### Execução:
- Rode os arquivos ELT.ipynb e ETL.ipynb
- Execute o comando para construir o Data Warehouse:
```
dbt run
```
## 🏆Resultados:

## 👨‍👩‍👧‍👦Informações sobre os participantes:
- Arthur Jorge (ajbc)
- Davi Matoso (dmt2)
- Giovanna Bardi (gmcb)
- Maria Amorim (maca)
- Maria Eduarda Veloso (mevv)
- Raissa Figueiredo (rmf5)
- Sergio Tavares (stcml)

