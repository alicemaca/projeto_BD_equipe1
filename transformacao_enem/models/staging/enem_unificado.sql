select
    *
from {{ source('dados_brutos_enem', 'MICRODADOS_ENEM_2021') }}

union all

select
    *
from {{ source('dados_brutos_enem', 'MICRODADOS_ENEM_2022') }}

union all

select
    *
from {{ source('dados_brutos_enem', 'MICRODADOS_ENEM_2023') }}