{#
  ============================================================================================
  GRAO: uma linha representa uma acao de recolhimento de um produto, identificada pelo
  `recall_number` da FDA.
  ============================================================================================

  Este fato responde a outra metade da pergunta de farmacovigilancia. O FAERS mostra o que os
  pacientes relataram; o RES mostra o que a industria e a FDA fizeram a respeito de um produto.
  Um recall pode ocorrer por contaminacao, esterilidade, rotulagem ou desvio de fabricacao --
  nem sempre por um sinal clinico.

  `event_id` NAO e chave: ele agrupa recalls relacionados a uma mesma acao e se repete entre
  linhas. Usa-lo como `unique_key` perderia registros no MERGE.

  Assim como o fato de eventos adversos, este e um fato de grao fino ligado a `dim_farmaco`
  pelo mesmo vocabulario RxNorm -- o que permite cruzar as duas fontes pelo mesmo farmaco.
#}

{{ config(
    materialized='incremental',
    unique_key='id_recall',
    incremental_strategy='delete+insert'
) }}

with recalls as (

    select * from {{ ref('stg_res') }}

    {% if is_incremental() %}
    -- Mesma politica do fato de eventos adversos: reler a borda do watermark e seguro porque
    -- a chave e deterministica e a carga e idempotente.
    where ingest_time >= (select coalesce(max(ingest_time), '1900-01-01') from {{ this }})
    {% endif %}

),

com_farmaco as (

    select
        r.*,
        m.rxcui
    from recalls r
    left join {{ ref('rxnorm_mapping') }} m on m.nome_normalizado = r.nome_normalizado

),

final as (

    select
        {{ chave_hash(['c.recall_number']) }}               as id_recall,

        -- chaves estrangeiras. Recalls sem nome de substancia, generico ou marca caem no
        -- membro "nao informado" da dimensao, preservando a integridade referencial.
        {{ id_farmaco_de('c.rxcui', 'c.nome_normalizado') }} as id_farmaco,
        cast(strftime(c.report_date, '%Y%m%d') as integer)   as id_data_relatorio,
        {{ chave_hash(['c.fonte']) }}                        as id_fonte,
        b.id_bula,

        -- dimensao degenerada
        c.recall_number,
        c.event_id,

        -- atributos da acao
        c.classificacao,
        c.classificacao_nivel,
        c.situacao,
        c.empresa,
        c.produto_descricao,
        c.motivo,
        c.nome_normalizado,
        c.openfda_generic_name,
        c.openfda_brand_name,

        -- relogios e frescor
        c.report_date,
        c.recall_initiation_date,
        date_diff('day', c.recall_initiation_date, c.report_date)
                                                            as dias_ate_relatorio,
        c.event_time,
        c.ingest_time,
        date_diff('hour', c.event_time, c.ingest_time)      as latencia_ingestao_horas,

        c.fonte
    from com_farmaco c
    left join {{ ref('dim_bula') }} b on b.setid = c.openfda_spl_set_id

)

select * from final
