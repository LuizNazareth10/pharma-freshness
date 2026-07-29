{#
  Explosao do array `patient.reaction` do FAERS.

  Grao: uma linha por (`safetyreportid`, `reaction_seq`) -- uma reacao relatada dentro de um
  relato, na posicao original do array.

  `reactionmeddrapt` e o termo preferencial (PT) do dicionario MedDRA. A API expoe o TEXTO do
  termo e a versao do dicionario, mas nao o codigo numerico MedDRA: o dicionario e licenciado.
  Por isso a dimensao de reacao usa o termo normalizado como identidade, e nao um codigo.

  `reactionoutcome` (codigo da FDA):
    1 = recuperado, 2 = em recuperacao, 3 = nao recuperado,
    4 = recuperado com sequelas, 5 = fatal, 6 = desconhecido.
#}

with relatos as (

    select
        safetyreportid,
        patient_payload,
        event_time,
        ingest_time,
        fonte,
        source_url,
        extraction_id
    from {{ ref('src_faers_events') }}

),

deduplicado as (

    select * exclude (rn) from (
        select
            *,
            row_number() over (
                partition by safetyreportid order by ingest_time desc, extraction_id desc
            ) as rn
        from relatos
    )
    where rn = 1

),

explodido as (

    select
        safetyreportid,
        event_time,
        ingest_time,
        fonte,
        source_url,
        extraction_id,
        {{ explodir_json_array('patient_payload', '$.reaction') }}
    from deduplicado

),

tipado as (

    select
        safetyreportid,
        cast(posicao as integer)                                    as reaction_seq,

        json_extract_string(elemento, '$.reactionmeddrapt')         as reacao_termo_original,
        json_extract_string(
            elemento, '$.reactionmeddraversionpt'
        )                                                           as meddra_versao,
        cast(json_extract_string(elemento, '$.reactionoutcome') as integer)
                                                                    as desfecho_codigo,

        cast(event_time as timestamp with time zone)                as event_time,
        cast(ingest_time as timestamp with time zone)               as ingest_time,
        fonte,
        source_url,
        extraction_id
    from explodido

),

final as (

    select
        *,
        -- Termo canonico usado como identidade da reacao. O MedDRA e case-insensitive na
        -- pratica: "Death" e "DEATH" sao o mesmo termo preferencial.
        {{ nome_farmaco_normalizado('reacao_termo_original') }}      as reacao_normalizada,
        case desfecho_codigo
            when 1 then 'Recuperado'
            when 2 then 'Em recuperacao'
            when 3 then 'Nao recuperado'
            when 4 then 'Recuperado com sequelas'
            when 5 then 'Fatal'
            when 6 then 'Desconhecido'
            else null
        end                                                         as desfecho,
        desfecho_codigo = 5                                         as desfecho_fatal
    from tipado

)

select * from final
