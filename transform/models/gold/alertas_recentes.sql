{#
  ============================================================================================
  CAMADA DE SERVING (Fase 6) — consumo futuro pela LLM / dashboards.

  GRAO: um par farmaco-reacao grave (`id_evento`) com `receivedate` nos ultimos 7 dias.
  ============================================================================================

  Por que esta tabela existe
  --------------------------
  O esquema estrela (`fato_*` + `dim_*`) e o contrato analitico. Uma LLM, porem, nao deve
  montar joins a cada pergunta: ela precisa de uma fatia DENORMALIZADA, ja filtrada, com
  fonte e datas citaveis em cada linha.

  "Graves" e "recentes"
  ---------------------
  - Grave: `gravidade = true` (campo `serious` do FAERS).
  - Recente: `receivedate >= current_date - 7 dias` -- data em que a FDA recebeu o relato,
    nao o `ingest_time` nosso. Assim a janela descreve o mundo, nao o atraso do pipeline.

  Contagem
  --------
  O grao continua sendo o do fato (par farmaco-reacao). Somar linhas NAO conta relatos
  clinicos; use `count(distinct safetyreportid)`.

  Publicacao
  ----------
  Janela movel: linhas saem quando envelhecem. O UPSERT sozinho nao removeria chaves
  expiradas no Iceberg; por isso o contrato declara `replace_on_publish=True`.
#}

{{ config(materialized='table') }}

with base as (

    select
        f.id_evento,
        f.safetyreportid,
        f.safetyreportversion,
        f.id_farmaco,
        f.id_reacao,
        f.id_bula,
        f.nome_normalizado,
        f.gravidade,
        f.desfecho_fatal,
        f.desfecho_codigo,
        f.suspeito_primario,
        f.caracterizacao_medicamento,
        f.produto_relatado,
        f.pais_ocorrencia,
        f.paciente_sexo,
        f.paciente_idade,
        f.paciente_idade_unidade,
        f.receivedate,
        f.receiptdate,
        f.event_time,
        f.ingest_time,
        f.latencia_atualizacao_horas,
        f.fonte
    from {{ ref('fato_evento_adverso') }} f
    where f.gravidade
      and f.receivedate >= (current_date - interval 7 day)

),

final as (

    select
        b.id_evento,
        b.safetyreportid,
        b.safetyreportversion,

        coalesce(df.nome_farmaco, b.nome_normalizado)       as farmaco,
        b.nome_normalizado                                  as farmaco_reportado,
        df.rxcui,
        dr.reacao,
        dr.reacao_normalizada,

        b.gravidade,
        b.desfecho_fatal,
        b.desfecho_codigo,
        b.suspeito_primario,
        b.caracterizacao_medicamento,
        b.produto_relatado,
        b.pais_ocorrencia,
        b.paciente_sexo,
        b.paciente_idade,
        b.paciente_idade_unidade,

        -- Datas citaveis pela LLM (fonte + captura).
        b.receivedate                                       as data_recebimento_fda,
        b.receiptdate                                       as data_atualizacao_fda,
        b.event_time,
        b.ingest_time,
        b.latencia_atualizacao_horas,
        b.fonte,
        db.setid                                            as setid_bula,
        db.source_url                                       as url_bula
    from base b
    left join {{ ref('dim_farmaco') }} df on df.id_farmaco = b.id_farmaco
    left join {{ ref('dim_reacao') }} dr on dr.id_reacao = b.id_reacao
    left join {{ ref('dim_bula') }} db on db.id_bula = b.id_bula

)

select * from final
