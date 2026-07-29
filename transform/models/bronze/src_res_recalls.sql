{#
  Aterrissagem local da tabela Iceberg `bronze.res_recalls`.
  Grao identico a fonte: uma linha por `recall_number`. Ver `src_faers_events` para o motivo.
#}

{{ config(materialized='table') }}

select * from {{ source('bronze', 'res_recalls') }}
