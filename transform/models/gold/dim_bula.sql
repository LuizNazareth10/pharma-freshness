{#
  Dimensao de bula (SPL do DailyMed).

  Grao: uma linha por `setid`, na versao corrente conhecida.

  Por que o DailyMed e dimensao e nao fato: ele descreve o ESTADO OFICIAL de um produto, nao um
  acontecimento. O fato correspondente seria "uma publicacao de versao de bula", e ele so passa
  a ter mais de uma linha por `setid` quando a bronze guardar o historico de versoes -- hoje a
  bronze faz UPSERT por `setid` e mantem apenas o estado corrente.

  `spl_version` e `published_date` ficam aqui como atributos correntes. Quando o historico de
  versoes existir, esta dimensao vira uma SCD tipo 2 e ganha `valido_de` / `valido_ate`.

  `id_farmaco` liga a bula ao mesmo vocabulario RxNorm usado pelos eventos adversos, o que
  permite perguntar "a bula deste farmaco mudou depois do aumento de relatos?".
#}

with bulas as (

    select * from {{ ref('stg_dailymed') }}

),

com_nome as (

    select
        b.*,
        {{ nome_farmaco_normalizado('b.produto_nome') }} as nome_normalizado
    from bulas b

),

com_farmaco as (

    -- Bulas cujo nome de produto nao foi resolvido ficam com `id_farmaco` nulo em vez de
    -- apontar para o membro "nao informado": aqui a ausencia de ligacao e informacao util
    -- (o parsing do titulo falhou), e nao uma categoria de negocio.
    -- O join com `rxnorm_mapping` permanece, mas agora so para CONFIRMAR que o nome extraido do
    -- titulo e um nome conhecido da dimensao. A chave em si vem do nome, nao do RxCUI.
    select
        c.*,
        case
            when m.nome_normalizado is null then null
            else {{ id_farmaco_de('m.nome_normalizado') }}
        end as id_farmaco
    from com_nome c
    left join {{ ref('rxnorm_mapping') }} m on m.nome_normalizado = c.nome_normalizado

),

final as (

    select
        {{ chave_hash(['setid']) }}                     as id_bula,
        setid,
        spl_version,
        titulo_original,
        produto_nome,
        laboratorio,
        nome_normalizado,
        id_farmaco,
        published_date,
        cast(strftime(published_date, '%Y%m%d') as integer) as id_data_publicacao,
        event_time,
        ingest_time,
        fonte,
        source_url
    from com_farmaco

)

select * from final
