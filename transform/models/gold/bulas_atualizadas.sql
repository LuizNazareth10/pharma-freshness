{#
  ============================================================================================
  CAMADA DE SERVING (Fase 6) — consumo futuro pela LLM / dashboards.

  GRAO: uma bula SPL (`setid` / `id_bula`) cuja `published_date` cai nos ultimos 3 dias.
  ============================================================================================

  Por que esta tabela existe
  --------------------------
  A dimensao `dim_bula` guarda o estado corrente de TODA bula conhecida. Para a LLM (e para
  um painel de "o que mudou esta semana"), o util e a FATIA recente, ja com nome de farmaco,
  data de revisao e URL citavel.

  "Modificadas" neste lab
  -----------------------
  A bronze faz UPSERT por `setid` e mantem so a versao corrente. Sem historico de versoes,
  "modificada" = `published_date` recente na versao que temos. Quando a bronze passar a
  guardar historico, esta view passa a filtrar por mudanca de `spl_version` na janela.

  Publicacao
  ----------
  Janela movel: mesmo motivo de `alertas_recentes` — `replace_on_publish=True`.
#}

{{ config(materialized='table') }}

select
    b.id_bula,
    b.setid,
    b.spl_version,
    b.titulo_original,
    b.produto_nome,
    b.laboratorio,
    coalesce(df.nome_farmaco, b.nome_normalizado, b.produto_nome) as farmaco,
    b.nome_normalizado                              as farmaco_reportado,
    df.rxcui,
    b.id_farmaco,

    -- Data de revisao citavel (publicacao no DailyMed).
    b.published_date                                as data_revisao,
    b.id_data_publicacao,
    b.event_time,
    b.ingest_time,
    b.fonte,
    b.source_url
from {{ ref('dim_bula') }} b
left join {{ ref('dim_farmaco') }} df on df.id_farmaco = b.id_farmaco
where b.published_date >= (current_date - interval 3 day)
