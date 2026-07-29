{#
  Dimensao de reacao adversa.

  Grao: um termo preferencial (PT) MedDRA distinto, na forma normalizada.

  Limite honesto desta dimensao: o openFDA expoe o TEXTO do termo MedDRA
  (`reactionmeddrapt`) e a versao do dicionario, mas NAO o codigo numerico. O MedDRA e
  licenciado pelo ICH e seus codigos nao circulam em API publica.

  O documento de fundacao previa `dim_reacao(codigo_meddra, ...)`. Como o codigo nao existe no
  dado disponivel, a identidade aqui e uma chave substituta derivada do termo normalizado. Um
  campo `codigo_meddra` sempre nulo daria a impressao de rastreabilidade que nao temos.

  Consequencia: nao ha hierarquia MedDRA (PT -> HLT -> SOC). Agrupar reacoes por orgao ou
  sistema exigiria uma licenca do dicionario.
#}

with reacoes as (

    select
        reacao_normalizada,
        reacao_termo_original,
        meddra_versao
    from {{ ref('stg_faers_reactions') }}
    where reacao_normalizada is not null

),

agrupado as (

    select
        reacao_normalizada,
        -- Grafia mais frequente do termo, para exibicao.
        mode(reacao_termo_original)                             as termo_exibicao,
        max(meddra_versao)                                      as meddra_versao_max,
        count(*)                                                as ocorrencias
    from reacoes
    group by reacao_normalizada

),

final as (

    select
        {{ chave_hash(['reacao_normalizada']) }}                as id_reacao,
        reacao_normalizada,
        termo_exibicao                                          as reacao,
        meddra_versao_max                                       as meddra_versao,
        ocorrencias                                             as ocorrencias_observadas
    from agrupado

)

select * from final
