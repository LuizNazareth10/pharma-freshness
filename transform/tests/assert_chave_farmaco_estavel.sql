{#
  A chave do farmaco precisa depender APENAS do nome normalizado.

  Este teste guarda a correcao de um incidente real. A chave costumava embutir o RxCUI quando
  ele existia. Como o RxNorm resolve nomes novos aos poucos (`RXNORM_MAX_LOOKUPS`), um farmaco
  entrava no fato como `nome:CORTISONE` e, na execucao seguinte -- ja resolvido --, a dimensao
  reconstruida passava a chama-lo `rxcui:3117`. O fato e incremental: as linhas antigas ficaram
  apontando para uma identidade que nao existia mais. Resultado: 6.066 linhas orfas.

  O teste `relationships` detecta o sintoma (a orfandade) somente DEPOIS que ela acontece, e so
  se a mudanca do RxNorm ja tiver ocorrido entre duas execucoes. Este teste ataca a causa: se
  alguem voltar a misturar enriquecimento na chave, ele reprova na mesma execucao, sem depender
  de o RxNorm mudar de ideia.

  Verifica as duas pontas -- a dimensao e o fato -- porque as duas precisam concordar.

  A chave esperada e montada AQUI, explicitamente, em vez de chamar `id_farmaco_de`. Isso e
  proposital: se o teste reaproveitasse a macro, alguem poderia reintroduzir o RxCUI dentro
  dela e os dois lados mudariam juntos, deixando o teste passar exatamente no cenario que ele
  existe para impedir.
#}

with dimensao as (

    select
        'dim_farmaco'                       as modelo,
        id_farmaco                          as chave_gravada,
        {{ chave_hash(["'" ~ prefixo_nome() ~ "' || nome_normalizado"]) }} as chave_esperada,
        nome_normalizado
    from {{ ref('dim_farmaco') }}
    where nome_normalizado is not null

),

fato as (

    select
        'fato_evento_adverso'               as modelo,
        id_farmaco                          as chave_gravada,
        {{ chave_hash(["'" ~ prefixo_nome() ~ "' || nome_normalizado"]) }} as chave_esperada,
        nome_normalizado
    from {{ ref('fato_evento_adverso') }}
    where nome_normalizado is not null

)

select * from (
    select * from dimensao
    union all
    select * from fato
)
where chave_gravada <> chave_esperada
