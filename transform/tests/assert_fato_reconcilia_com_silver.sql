-- Teste singular: reconciliacao entre a silver e a gold.
--
-- Um erro classico de modelagem dimensional e perder linhas num JOIN: uma chave estrangeira
-- que nao casa transforma um LEFT JOIN mal escrito, ou um INNER JOIN inadvertido, em perda
-- silenciosa de dados. O modelo continua rodando, os testes de unicidade continuam passando,
-- e o numero simplesmente fica menor.
--
-- A verificacao aqui e de conjunto: todo relato que tem, ao mesmo tempo, pelo menos um
-- medicamento nomeado e pelo menos uma reacao nomeada PRECISA aparecer no fato. Se nao
-- aparece, o cruzamento perdeu o relato.
--
-- Relatos sem medicamento nomeado ou sem reacao nomeada sao legitimamente ausentes: nao ha par
-- farmaco-reacao para representar. Por isso eles sao excluidos da expectativa.
--
-- Este teste roda apenas em carga completa. Numa execucao incremental o fato contem o
-- historico acumulado e a silver reflete a janela corrente, entao a comparacao direta nao vale.

{{ config(enabled=(not var('carga_incremental', false))) }}

with relatos_elegiveis as (

    select distinct d.safetyreportid
    from {{ ref('stg_faers_drugs') }} d
    join {{ ref('stg_faers_reactions') }} r on r.safetyreportid = d.safetyreportid
    where d.nome_normalizado is not null
      and r.reacao_normalizada is not null

),

relatos_no_fato as (

    select distinct safetyreportid
    from {{ ref('fato_evento_adverso') }}

)

select
    e.safetyreportid,
    'relato elegivel ausente do fato' as problema
from relatos_elegiveis e
left join relatos_no_fato f on f.safetyreportid = e.safetyreportid
where f.safetyreportid is null
