{#
  Staging do DailyMed.

  Grao: uma linha por `setid`, no estado mais recente conhecido.

  A bronze ja faz UPSERT por `setid`, mas o modelo nao confia nisso: deduplica de novo com
  ROW_NUMBER. Um staging que so funciona porque a camada anterior se comportou bem e um
  staging fragil -- e a mesma dedup passa a valer se um dia a bronze virar append-only.
#}

with fonte as (

    select * from {{ ref('src_dailymed_spls') }}

),

deduplicado as (

    select
        *,
        row_number() over (
            partition by setid
            order by ingest_time desc, spl_version desc
        ) as rn
    from fonte

),

tipado as (

    select
        setid,
        cast(spl_version as integer)                            as spl_version,
        trim(title)                                             as titulo_original,

        -- O titulo do DailyMed segue o padrao "PRODUTO (INGREDIENTES) FORMA [LABORATORIO]".
        -- A extracao abaixo e best-effort e esta documentada como tal: o texto canonico da
        -- bula vive no XML completo, que esta fase ainda nao ingere.
        nullif(trim(regexp_extract(title, '\[([^\]]*)\]$', 1)), '')      as laboratorio,
        nullif(trim(regexp_replace(
            regexp_replace(title, '\s*\[[^\]]*\]\s*$', ''),
            '\s*\(.*$', ''
        )), '')                                                 as produto_nome,

        cast(published_date as date)                            as published_date,
        cast(event_time as timestamp with time zone)            as event_time,
        cast(ingest_time as timestamp with time zone)           as ingest_time,
        fonte,
        source_url,
        extraction_id
    from deduplicado
    where rn = 1

)

select * from tipado
