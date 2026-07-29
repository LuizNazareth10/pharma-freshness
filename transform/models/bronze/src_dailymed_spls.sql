{#
  Aterrissagem local da tabela Iceberg `bronze.dailymed_spls`.
  Grao identico a fonte: uma linha por `setid`. Ver `src_faers_events` para o motivo do padrao.
#}

{{ config(materialized='table') }}

select * from {{ source('bronze', 'dailymed_spls') }}
