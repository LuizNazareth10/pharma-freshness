{#
  Dimensao de fonte.

  Grao: um sistema de origem do pipeline.

  Vem de um seed (`seeds/fonte_referencia.csv`) porque estes atributos sao conhecimento do
  projeto, nao dado observado: mantenedor, cadencia esperada e URL da API nao chegam em
  nenhuma resposta das APIs. Versionar isso como CSV mantem a informacao sob revisao de codigo.

  `cadencia_esperada` e o que torna a Fase 4 possivel: sem uma expectativa declarada, nao ha
  como dizer que uma fonte esta atrasada -- so da para dizer quando ela chegou.
#}

with referencia as (

    select * from {{ ref('fonte_referencia') }}

),

observado as (

    select fonte, max(ingest_time) as ultimo_ingest_time, count(*) as registros_bronze
    from (
        select fonte, ingest_time from {{ ref('stg_dailymed') }}
        union all
        select fonte, ingest_time from {{ ref('stg_faers') }}
        union all
        select fonte, ingest_time from {{ ref('stg_res') }}
    )
    group by fonte

),

final as (

    select
        {{ chave_hash(['r.fonte']) }}                   as id_fonte,
        r.fonte,
        r.nome_exibicao,
        r.mantenedor,
        r.cadencia_esperada,
        r.url_api,
        r.descricao,

        -- Limiares de SLO declarados no seed. Ficam na dimensao para que qualquer consulta
        -- consiga comparar o observado com o prometido sem reabrir o CSV.
        r.sla_ingestao_horas,
        r.sla_frescor_fonte_horas,
        r.frescor_fonte_observacao,

        o.ultimo_ingest_time,
        coalesce(o.registros_bronze, 0)                 as registros_observados
    from referencia r
    left join observado o on o.fonte = r.fonte

)

select * from final
