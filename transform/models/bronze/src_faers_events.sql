{#
  Aterrissagem local da tabela Iceberg `bronze.faers_events`.

  Grao: identico a fonte -- uma linha por `safetyreportid`. Nenhuma regra de negocio aqui.

  Por que este modelo existe
  --------------------------
  1. Custo: tres modelos silver precisam do `patient_payload`. Sem esta materializacao, cada
     um dispararia sua propria varredura do Iceberg no MinIO. Aqui a leitura remota acontece
     uma vez por execucao.
  2. Concorrencia: o plugin do dbt-duckdb registra a fonte dentro da transacao que a usa.
     Dois modelos lendo a MESMA fonte em paralelo colidem no catalogo do DuckDB. Com a leitura
     concentrada em um unico modelo, os modelos seguintes leem uma tabela local comum.
  3. Consistencia: todos os modelos derivados enxergam exatamente o mesmo snapshot Iceberg,
     mesmo que a bronze receba uma nova carga no meio da execucao.
#}

{{ config(materialized='table') }}

select * from {{ source('bronze', 'faers_events') }}
