{#
  Explosao do array `patient.drug` do FAERS.

  Grao: uma linha por (`safetyreportid`, `drug_seq`) -- uma entrada de medicamento dentro de um
  relato, na posicao original do array.

  Este grao preserva a fonte fielmente, inclusive suas repeticoes: o mesmo medicamento pode
  aparecer duas vezes no mesmo relato com dosagens diferentes. Consolidar aqui destruiria
  informacao. A consolidacao por identidade de farmaco acontece na gold, onde o grao analitico
  e declarado.

  `drugcharacterization` (codigo da FDA):
    1 = suspeito, 2 = concomitante, 3 = interagente.
  Somente o codigo 1 indica que o notificador suspeitou do medicamento.
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
        {{ explodir_json_array('patient_payload', '$.drug') }}
    from deduplicado

),

tipado as (

    select
        safetyreportid,
        cast(posicao as integer)                                        as drug_seq,

        json_extract_string(elemento, '$.medicinalproduct')             as produto_relatado,
        json_extract_string(
            elemento, '$.activesubstance.activesubstancename'
        )                                                               as substancia_ativa,

        cast(json_extract_string(elemento, '$.drugcharacterization') as integer)
                                                                        as caracterizacao_codigo,
        json_extract_string(elemento, '$.drugindication')               as indicacao,
        json_extract_string(elemento, '$.drugdosagetext')               as dosagem_texto,
        json_extract_string(elemento, '$.drugdosageform')               as forma_farmaceutica,

        -- Bloco `openfda`: harmonizacao que a propria FDA aplica ao registro. Nem todo
        -- medicamento relatado tem esse bloco; quando falta, resta o nome livre.
        json_extract_string(elemento, '$.openfda.generic_name[0]')      as openfda_generic_name,
        json_extract_string(elemento, '$.openfda.brand_name[0]')        as openfda_brand_name,
        json_extract_string(elemento, '$.openfda.spl_set_id[0]')        as openfda_spl_set_id,
        {{ json_array_texto('elemento', '$.openfda.rxcui') }}           as openfda_rxcui,

        cast(event_time as timestamp with time zone)                    as event_time,
        cast(ingest_time as timestamp with time zone)                   as ingest_time,
        fonte,
        source_url,
        extraction_id
    from explodido

),

final as (

    select
        *,
        case caracterizacao_codigo
            when 1 then 'Suspeito'
            when 2 then 'Concomitante'
            when 3 then 'Interagente'
            else null
        end                                                             as caracterizacao,
        caracterizacao_codigo = 1                                       as suspeito_primario,

        -- Nome levado ao RxNorm. A substancia ativa vem primeiro porque o RxNorm normaliza
        -- para ingrediente; o nome comercial e o ultimo recurso.
        {{ nome_farmaco_normalizado(
            'coalesce(substancia_ativa, openfda_generic_name, produto_relatado)'
        ) }}                                                            as nome_normalizado
    from tipado

)

select * from final
