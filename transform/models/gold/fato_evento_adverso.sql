{#
  ============================================================================================
  GRAO: uma linha representa um par farmaco-reacao DISTINTO dentro de um relato de evento
  adverso enviado a FDA.

  Chave logica: (safetyreportid, id_farmaco, id_reacao) -> `id_evento`.
  ============================================================================================

  Por que este grao, e nao "um relato por linha"
  ----------------------------------------------
  O documento de fundacao descreve o grao como "um evento adverso individual, por um farmaco
  especifico", mas o exemplo usava `unique_key='report_id'`. As duas coisas nao podem ser
  verdade ao mesmo tempo: um relato cita varios farmacos e varias reacoes, entao uma linha por
  relato NAO e uma linha por farmaco. Adotamos o grao descrito em palavras -- mais fino -- e a
  chave passa a ser composta.

  Com uma linha por relato, a pergunta central da farmacovigilancia ("quantos relatos associam
  o farmaco X a reacao Y?") exigiria abrir um JSON em tempo de consulta. Com o grao de par,
  ela e um COUNT com dois filtros.

  O produto cartesiano e proposital -- e precisa ser entendido
  ------------------------------------------------------------
  O FAERS lista medicamentos e reacoes como DUAS listas independentes. A fonte nao diz qual
  medicamento se liga a qual reacao. Cruzar as duas listas dentro do relato e exatamente o que
  a analise de desproporcionalidade (PRR, ROR) faz com dados de notificacao espontanea.

  Isso significa que somar linhas NAO conta eventos clinicos. Um relato com 3 farmacos e
  4 reacoes gera 12 linhas. As metricas corretas sao contagens DISTINTAS de
  `safetyreportid` -- e por isso `qtd_farmacos_relato` e `qtd_reacoes_relato` ficam na tabela,
  para que quem consulta enxergue o fator de multiplicacao.

  Nenhuma linha aqui prova causalidade. Um relato registra suspeita, nao nexo causal.

  Deduplicacao dentro do relato
  -----------------------------
  O mesmo medicamento aparece repetido no mesmo relato quando ha registros de dosagem
  diferentes (nesta base, centenas de casos). Como o grao e por IDENTIDADE de farmaco, essas
  repeticoes colapsam. `qtd_entradas_medicamento` preserva quantas entradas originais foram
  consolidadas, e `caracterizacao_codigo` fica com o menor valor -- se o medicamento foi
  suspeito em alguma entrada, o par e tratado como suspeito.

  Materializacao incremental
  --------------------------
  A carga usa `merge` por `id_evento`. Como a chave e deterministica (hash do grao),
  reprocessar os mesmos dados atualiza as mesmas linhas em vez de duplicar. O filtro
  incremental usa `ingest_time`: so relatos capturados depois do ultimo ja carregado entram.
#}

{{ config(
    materialized='incremental',
    unique_key='id_evento',
    incremental_strategy='delete+insert'
) }}

with relatos as (

    select * from {{ ref('stg_faers') }}

    {% if is_incremental() %}
    -- Reprocessa apenas relatos capturados desde a ultima carga. O `>=` e proposital: uma
    -- carga interrompida no meio de um mesmo `ingest_time` seria parcialmente perdida com `>`.
    -- Reler a borda e seguro porque o MERGE e idempotente.
    where ingest_time >= (select coalesce(max(ingest_time), '1900-01-01') from {{ this }})
    {% endif %}

),

medicamentos_resolvidos as (

    -- O RxCUI precisa ser resolvido ANTES do agrupamento. Agrupar por nome e so depois
    -- resolver produziria duas linhas para o mesmo farmaco quando dois nomes distintos
    -- apontam para o mesmo ingrediente -- exatamente o caso que o RxNorm existe para unificar.
    select
        d.safetyreportid,
        d.nome_normalizado,
        rx.rxcui,
        {{ chave_identidade_farmaco('rx.rxcui', 'd.nome_normalizado') }} as chave_identidade,
        d.caracterizacao_codigo,
        d.produto_relatado,
        d.substancia_ativa,
        d.openfda_spl_set_id
    from {{ ref('stg_faers_drugs') }} d
    join relatos r on r.safetyreportid = d.safetyreportid
    left join {{ ref('rxnorm_mapping') }} rx on rx.nome_normalizado = d.nome_normalizado
    where d.nome_normalizado is not null

),

medicamentos as (

    select
        safetyreportid,
        chave_identidade,
        max(rxcui)                                          as rxcui,
        min(nome_normalizado)                               as nome_normalizado,
        -- Menor codigo vence: se o medicamento foi suspeito (1) em alguma entrada do relato,
        -- o par e tratado como suspeito.
        min(caracterizacao_codigo)                          as caracterizacao_codigo,
        count(*)                                            as qtd_entradas_medicamento,
        min(produto_relatado)                               as produto_relatado,
        min(substancia_ativa)                               as substancia_ativa,
        min(openfda_spl_set_id)                             as openfda_spl_set_id
    from medicamentos_resolvidos
    group by safetyreportid, chave_identidade

),

reacoes as (

    select
        r.safetyreportid,
        r.reacao_normalizada,
        min(r.desfecho_codigo)                              as desfecho_codigo,
        bool_or(r.desfecho_fatal)                           as desfecho_fatal
    from {{ ref('stg_faers_reactions') }} r
    join relatos rel on rel.safetyreportid = r.safetyreportid
    where r.reacao_normalizada is not null
    group by r.safetyreportid, r.reacao_normalizada

),

pares as (

    select
        rel.safetyreportid,
        m.nome_normalizado,
        m.chave_identidade,
        re.reacao_normalizada,

        m.caracterizacao_codigo,
        m.qtd_entradas_medicamento,
        m.produto_relatado,
        m.substancia_ativa,
        m.openfda_spl_set_id,

        re.desfecho_codigo,
        re.desfecho_fatal,

        rel.grave,
        rel.pais_ocorrencia,
        rel.paciente_sexo,
        rel.paciente_idade,
        rel.paciente_idade_unidade,
        rel.safetyreportversion,
        rel.qtd_medicamentos                                as qtd_medicamentos_relato,
        rel.qtd_reacoes                                     as qtd_reacoes_relato,
        rel.receivedate,
        rel.receiptdate,
        rel.event_time,
        rel.ingest_time,
        rel.fonte
    from relatos rel
    join medicamentos m on m.safetyreportid = rel.safetyreportid
    join reacoes re on re.safetyreportid = rel.safetyreportid

),

com_chaves as (

    select
        p.*,
        {{ chave_hash(['p.chave_identidade']) }}            as id_farmaco,
        {{ chave_hash(['p.reacao_normalizada']) }}          as id_reacao
    from pares p

),

final as (

    select
        {{ chave_hash([
            'c.safetyreportid', 'c.id_farmaco', 'c.id_reacao'
        ]) }}                                               as id_evento,

        -- chaves estrangeiras do esquema estrela
        c.id_farmaco,
        c.id_reacao,
        cast(strftime(c.receivedate, '%Y%m%d') as integer)   as id_data_recebimento,
        {{ chave_hash(['c.fonte']) }}                        as id_fonte,
        b.id_bula,

        -- dimensao degenerada: o identificador do relato vive no proprio fato
        c.safetyreportid,
        c.safetyreportversion,

        -- atributos do par
        c.caracterizacao_codigo                             as caracterizacao_codigo,
        case c.caracterizacao_codigo
            when 1 then 'Suspeito'
            when 2 then 'Concomitante'
            when 3 then 'Interagente'
            else null
        end                                                 as caracterizacao_medicamento,
        c.caracterizacao_codigo = 1                         as suspeito_primario,
        c.desfecho_codigo,
        c.desfecho_fatal,
        c.grave                                             as gravidade,
        c.produto_relatado,
        c.substancia_ativa,

        -- contexto do paciente e do relato
        c.pais_ocorrencia,
        c.paciente_sexo,
        c.paciente_idade,
        c.paciente_idade_unidade,
        c.qtd_entradas_medicamento,
        c.qtd_medicamentos_relato,
        c.qtd_reacoes_relato,

        -- relogios e frescor
        c.receivedate,
        c.receiptdate,
        c.event_time,
        c.ingest_time,

        -- DUAS latencias, porque o FAERS tem DOIS relogios e eles respondem perguntas
        -- diferentes. Reportar apenas a primeira produziria um numero verdadeiro sobre a
        -- pergunta errada.
        --
        -- `latencia_ingestao_horas` parte de `receivedate`, a data em que a FDA recebeu o
        -- relato pela PRIMEIRA vez. Ela mede a idade do caso quando o capturamos. Como um
        -- relato antigo pode ser revisado anos depois, esse numero chega facilmente a milhares
        -- de horas -- e isso e uma caracteristica da fonte, nao um atraso do pipeline.
        date_diff('hour', c.event_time, c.ingest_time)      as latencia_ingestao_horas,

        -- `latencia_atualizacao_horas` parte de `receiptdate`, a data da informacao mais
        -- recente daquela versao. Esta e a latencia que o pipeline realmente controla: quanto
        -- tempo levamos para capturar a ULTIMA novidade sobre o caso. E a base honesta do
        -- staleness gap da Fase 4.
        date_diff('hour', c.receiptdate::timestamptz, c.ingest_time)
                                                            as latencia_atualizacao_horas,

        c.fonte
    from com_chaves c
    left join {{ ref('dim_bula') }} b on b.setid = c.openfda_spl_set_id

)

select * from final
