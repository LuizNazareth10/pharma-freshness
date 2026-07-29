{#
  Staging do Recall Enterprise System.

  Grao: uma linha por `recall_number`.

  Atencao ao `event_id`: ele agrupa recalls relacionados a uma mesma acao e NAO e unico por
  linha. Usa-lo como chave produziria perda de registros no join.
#}

with fonte as (

    select * from {{ ref('src_res_recalls') }}

),

deduplicado as (

    select
        *,
        row_number() over (
            partition by recall_number
            order by ingest_time desc, extraction_id desc
        ) as rn
    from fonte

),

tipado as (

    select
        recall_number,
        event_id,
        cast(report_date as date)                                   as report_date,
        cast(recall_initiation_date as date)                        as recall_initiation_date,

        nullif(trim(classification), '')                            as classificacao,
        nullif(trim(status), '')                                    as situacao,
        nullif(trim(recalling_firm), '')                            as empresa,
        nullif(trim(product_description), '')                       as produto_descricao,
        nullif(trim(reason_for_recall), '')                         as motivo,

        -- Classe de risco da FDA. Class I e a mais grave: risco razoavel de dano serio a saude.
        case
            when classification = 'Class I' then 1
            when classification = 'Class II' then 2
            when classification = 'Class III' then 3
            else null
        end                                                         as classificacao_nivel,

        json_extract_string(openfda_payload, '$.generic_name[0]')   as openfda_generic_name,
        json_extract_string(openfda_payload, '$.brand_name[0]')     as openfda_brand_name,
        json_extract_string(openfda_payload, '$.substance_name[0]') as openfda_substance_name,
        json_extract_string(openfda_payload, '$.spl_set_id[0]')     as openfda_spl_set_id,
        {{ json_array_texto('openfda_payload', '$.rxcui') }}        as openfda_rxcui,

        cast(event_time as timestamp with time zone)                as event_time,
        cast(ingest_time as timestamp with time zone)               as ingest_time,
        fonte,
        source_url,
        extraction_id
    from deduplicado
    where rn = 1

),

final as (

    select
        *,
        -- Mesma regra de normalizacao do FAERS, para que os dois mundos encontrem o mesmo
        -- farmaco no RxNorm.
        {{ nome_farmaco_normalizado(
            'coalesce(openfda_substance_name, openfda_generic_name, openfda_brand_name)'
        ) }}                                                        as nome_normalizado
    from tipado

)

select * from final
