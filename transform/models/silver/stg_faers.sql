{#
  Staging do FAERS no nivel do relato.

  Grao: uma linha por `safetyreportid`.

  Este modelo NAO explode medicamentos nem reacoes. Um relato cita varios medicamentos e
  varias reacoes, e a fonte nao diz qual medicamento se liga a qual reacao. Misturar os dois
  arrays aqui produziria um grao ambiguo. A explosao acontece em modelos proprios
  (`stg_faers_drugs`, `stg_faers_reactions`), onde o grao e declarado explicitamente.
#}

with fonte as (

    select * from {{ ref('src_faers_events') }}

),

deduplicado as (

    select
        *,
        row_number() over (
            partition by safetyreportid
            order by ingest_time desc, safetyreportversion desc, extraction_id desc
        ) as rn
    from fonte

),

tipado as (

    select
        safetyreportid,
        cast(safetyreportversion as integer)                    as safetyreportversion,
        cast(receivedate as date)                               as receivedate,
        cast(receiptdate as date)                               as receiptdate,
        cast(serious as boolean)                                as grave,
        nullif(trim(occurcountry), '')                          as pais_ocorrencia,

        -- Demografia do paciente. Codigos da FDA: 1 = masculino, 2 = feminino.
        {{ faers_sexo('json_extract_string(patient_payload, \'$.patientsex\')') }} as paciente_sexo,
        try_cast(
            json_extract_string(patient_payload, '$.patientonsetage') as double
        )                                                       as paciente_idade,
        json_extract_string(
            patient_payload, '$.patientonsetageunit'
        )                                                       as paciente_idade_unidade,

        cast(coalesce(json_array_length(patient_payload, '$.drug'), 0) as integer)
                                                                as qtd_medicamentos,
        cast(coalesce(json_array_length(patient_payload, '$.reaction'), 0) as integer)
                                                                as qtd_reacoes,

        cast(event_time as timestamp with time zone)            as event_time,
        cast(ingest_time as timestamp with time zone)           as ingest_time,
        fonte,
        source_url,
        extraction_id
    from deduplicado
    where rn = 1

)

select * from tipado
