{#
  ============================================================================================
  GRAO: uma linha representa UMA MEDICAO de frescor de UMA fonte, em um instante.

  Chave logica: (fonte, medicao_em) -> `id_medicao`.
  ============================================================================================

  Este e o modelo que transforma "frescor diario" de intencao em numero.

  Por que esta tabela e um LOG, e nao um estado
  ---------------------------------------------
  Todos os outros modelos da gold descrevem o estado atual: republicar sem dado novo nao muda
  nada e nao gera snapshot. Aqui e o oposto, de proposito. O documento de fundacao pede uma
  tabela que "para cada execucao do pipeline registra" o atraso -- ou seja, uma serie temporal
  de OBSERVACOES. Cada execucao acrescenta uma medicao nova, mesmo que os dados nao tenham
  mudado, porque "nada mudou desde ontem" tambem e uma informacao de frescor.

  E essa serie que permite responder perguntas que uma foto instantanea nao responde:
  o atraso do FAERS esta crescendo? O DailyMed degradou depois da mudanca de infraestrutura?
  Quantas horas, em media, ficamos sem dado novo?

  Todas as linhas de uma execucao compartilham `medicao_em = run_started_at`, o relogio do
  dbt. Usar `now()` daria timestamps ligeiramente diferentes por fonte e impediria comparar
  as tres na mesma medicao.

  Os TRES relogios, e por que nao basta um
  -----------------------------------------
  O exemplo do documento de fundacao calcula um unico gap:
      DATEDIFF('hour', MAX(event_time), MAX(ingest_time))
  Esse numero mistura duas causas de atraso que precisam ser separadas, porque uma e nossa e a
  outra nao:

    1. `atraso_da_fonte_horas`  = ultimo_ingest_time - event_time_mais_recente
       Quando capturamos, qual era a idade do dado mais novo que a fonte oferecia?
       Isso mede a FONTE. Nao ha codigo nosso capaz de melhorar esse numero.

    2. `atraso_do_pipeline_horas` = agora - ultimo_ingest_time
       Ha quanto tempo nao rodamos? Isso mede o PIPELINE, e e inteiramente nosso.

    3. `idade_do_dado_horas`    = agora - event_time_mais_recente
       Quantas horas tem o dado mais novo que conseguimos entregar AGORA?
       E a soma das duas causas -- e a unica que interessa a quem consome.

  Confundir 1 com 2 leva ao erro operacional classico: alguem ve o gap alto, conclui que o
  pipeline quebrou, investiga o codigo por horas e descobre que a FDA e que nao publicou nada.
  Ou o contrario: o pipeline esta parado ha tres dias e ninguem percebe, porque a fonte tambem
  esta lenta e o numero agregado parece "normal para essa fonte".

  Por que ler da silver, e nao do fato
  -------------------------------------
  O exemplo do documento agrupa `fato_evento_adverso` por `fonte`. Naquele fato, porem, `fonte`
  vale sempre 'faers' -- ele so contem FAERS. O GROUP BY devolveria uma unica linha e as outras
  duas fontes ficariam invisiveis justamente na tabela que existe para vigia-las.
  Os modelos `stg_*` cobrem as tres fontes no grao natural de cada uma.
#}

{{ config(
    materialized='incremental',
    unique_key='id_medicao',
    incremental_strategy='delete+insert'
) }}

with observado as (

    -- Uma linha por fonte, com os extremos dos dois relogios.
    select
        fonte,
        max(event_time)                                     as event_time_mais_recente,
        min(event_time)                                     as event_time_mais_antigo,
        max(ingest_time)                                    as ultimo_ingest_time,
        min(ingest_time)                                    as primeiro_ingest_time,
        count(*)                                            as registros
    from (
        select fonte, event_time, ingest_time from {{ ref('stg_dailymed') }}
        union all
        select fonte, event_time, ingest_time from {{ ref('stg_faers') }}
        union all
        select fonte, event_time, ingest_time from {{ ref('stg_res') }}
    )
    group by fonte

),

referencia as (

    select
        fonte,
        nome_exibicao,
        cadencia_esperada,
        sla_ingestao_horas,
        sla_frescor_fonte_horas
    from {{ ref('fonte_referencia') }}

),

medido as (

    select
        -- `run_started_at` e o instante em que ESTA execucao do dbt comecou. Compartilhado por
        -- todas as linhas da medicao, o que torna a comparacao entre fontes justa.
        cast('{{ run_started_at }}' as timestamp with time zone)    as medicao_em,

        o.fonte,
        r.nome_exibicao,
        r.cadencia_esperada,

        o.event_time_mais_recente,
        o.event_time_mais_antigo,
        o.ultimo_ingest_time,
        o.primeiro_ingest_time,
        o.registros,

        -- (1) atraso da FONTE: idade do dado no momento em que o capturamos.
        date_diff('hour', o.event_time_mais_recente, o.ultimo_ingest_time)
                                                                    as atraso_da_fonte_horas,

        -- (2) atraso do PIPELINE: ha quanto tempo nao capturamos nada.
        date_diff(
            'hour', o.ultimo_ingest_time, cast('{{ run_started_at }}' as timestamp with time zone)
        )                                                           as atraso_do_pipeline_horas,

        -- (3) idade do dado entregue agora: o que o consumidor realmente sente.
        date_diff(
            'hour',
            o.event_time_mais_recente,
            cast('{{ run_started_at }}' as timestamp with time zone)
        )                                                           as idade_do_dado_horas,

        r.sla_ingestao_horas,
        r.sla_frescor_fonte_horas
    from observado o
    inner join referencia r on r.fonte = o.fonte

),

final as (

    select
        {{ chave_hash(['fonte', 'medicao_em']) }}                   as id_medicao,
        {{ chave_hash(['fonte']) }}                                 as id_fonte,
        m.*,

        -- Violacao do SLO do PIPELINE: nos falhamos. Isso deve acordar alguem.
        m.atraso_do_pipeline_horas > m.sla_ingestao_horas           as violou_sla_pipeline,

        -- Violacao do SLO da FONTE: a origem esta mais lenta do que o esperado dela.
        -- Nao e defeito nosso, mas muda o que podemos prometer a quem consome.
        m.atraso_da_fonte_horas > m.sla_frescor_fonte_horas         as violou_sla_fonte,

        case
            when m.atraso_do_pipeline_horas > m.sla_ingestao_horas       then 'pipeline_atrasado'
            when m.atraso_da_fonte_horas    > m.sla_frescor_fonte_horas  then 'fonte_atrasada'
            else 'ok'
        end                                                         as situacao
    from medido m

)

select * from final
