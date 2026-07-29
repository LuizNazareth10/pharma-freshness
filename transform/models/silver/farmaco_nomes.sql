{#
  Universo de nomes de farmaco a normalizar.

  Grao: uma linha por `nome_normalizado` distinto observado em qualquer fonte.

  Este modelo existe para separar DUAS responsabilidades que costumam ser misturadas:
    1. descobrir quais nomes precisam de normalizacao (SQL, barato, deterministico);
    2. consultar o RxNorm para esses nomes (rede, lento, sujeito a falha).

  Com a separacao, o modelo seguinte consulta a API uma vez por nome distinto, e nao uma vez
  por linha de fato. Nesta base, isso e a diferenca entre algumas centenas de chamadas e
  dezenas de milhares.

  O `rxcui_openfda_sugerido` e apenas um indicio. O bloco `openfda` do FAERS lista os RxCUI de
  TODAS as apresentacoes do produto (dose, forma, marca), nao o ingrediente. Ele serve para
  auditar o resultado do RxNorm, nao para substitui-lo.
#}

with nomes_faers as (

    select
        nome_normalizado,
        'faers'                                     as fonte,
        try_cast(openfda_rxcui[1] as varchar)       as rxcui_openfda_sugerido
    from {{ ref('stg_faers_drugs') }}
    where nome_normalizado is not null

),

nomes_res as (

    select
        nome_normalizado,
        'res'                                       as fonte,
        try_cast(openfda_rxcui[1] as varchar)       as rxcui_openfda_sugerido
    from {{ ref('stg_res') }}
    where nome_normalizado is not null

),

nomes_dailymed as (

    select
        {{ nome_farmaco_normalizado('produto_nome') }} as nome_normalizado,
        'dailymed'                                  as fonte,
        cast(null as varchar)                       as rxcui_openfda_sugerido
    from {{ ref('stg_dailymed') }}
    where produto_nome is not null

),

unificado as (

    select * from nomes_faers
    union all
    select * from nomes_res
    union all
    select * from nomes_dailymed

),

agregado as (

    select
        nome_normalizado,
        count(*)                                            as ocorrencias,
        list_sort(list_distinct(list(fonte)))               as fontes,
        max(rxcui_openfda_sugerido)                         as rxcui_openfda_sugerido
    from unificado
    where nome_normalizado is not null
    group by nome_normalizado

)

select * from agregado
