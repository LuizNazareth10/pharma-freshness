{#
  Dimensao conformada de farmaco.

  Grao: uma identidade de farmaco.

  Definicao de identidade -- a decisao central desta dimensao (macro `chave_identidade_farmaco`):
    - com RxCUI: a identidade e o RxCUI. Todos os nomes que resolvem para o mesmo ingrediente
      viram UMA linha. "TACROLIMUS", "Tacrolimus" e "PROGRAF" deixam de contar separado.
    - sem RxCUI: a identidade e o proprio nome normalizado. O farmaco continua na dimensao,
      marcado como nao mapeado.
    - sem nome algum: cai no membro "nao informado" descrito abaixo.

  A segunda regra e deliberada. Descartar o que o RxNorm nao conhece silenciaria justamente os
  produtos mais irregulares -- combinacoes, manipulados, itens importados -- que sao os que
  mais interessam a farmacovigilancia. Um evento adverso nunca some do modelo por falta de
  vocabulario; ele fica visivel e rotulado como nao mapeado.

  Membro "nao informado"
  ----------------------
  Nem todo recall do RES traz nome de substancia, generico ou marca. Sem uma linha para
  representar essa ausencia, o fato ficaria com uma chave estrangeira orfa -- e o teste de
  integridade referencial falharia com razao.

  A alternativa comum e deixar a FK nula, mas isso obriga todo relatorio a usar LEFT JOIN e faz
  as linhas sumirem silenciosamente de qualquer INNER JOIN. Um membro explicito mantem a
  integridade referencial e deixa "nao informado" visivel como categoria, em vez de ausente.

  Consequencia pratica ao consultar: agregacoes por farmaco devem considerar
  `identidade_confiavel` quando o objetivo for comparar volumes entre farmacos.
#}

with mapeamento as (

    select * from {{ ref('rxnorm_mapping') }}
    where nome_normalizado is not null

),

identidade as (

    select
        *,
        {{ chave_identidade_farmaco('rxcui', 'nome_normalizado') }} as chave_identidade
    from mapeamento

),

agrupado as (

    select
        chave_identidade,
        max(rxcui)                                          as rxcui,
        max(rxnorm_nome)                                    as rxnorm_nome,
        max(rxnorm_tty)                                     as rxnorm_tty,
        max(tipo_correspondencia)                           as tipo_correspondencia,
        max(score)                                          as score_correspondencia,
        bool_or(nivel_ingrediente)                          as nivel_ingrediente,
        max(consultado_em)                                  as rxnorm_consultado_em,

        -- Nomes de origem que colapsaram nesta identidade. Guardar isso torna a normalizacao
        -- auditavel: da para explicar por que dois nomes viraram a mesma linha.
        list_sort(list_distinct(list(nome_normalizado)))     as nomes_originais,
        count(distinct nome_normalizado)                    as qtd_nomes_originais,
        min(nome_normalizado)                               as nome_representativo
    from identidade
    group by chave_identidade

),

observados as (

    select
        {{ chave_hash(['chave_identidade']) }}              as id_farmaco,
        rxcui,
        coalesce(rxnorm_nome, nome_representativo)          as nome_farmaco,
        nome_representativo,
        rxnorm_nome,
        rxnorm_tty,
        tipo_correspondencia,
        score_correspondencia,
        nivel_ingrediente,
        rxcui is not null                                   as mapeado_rxnorm,

        -- Identidade confiavel = resolvida no RxNorm E no nivel de ingrediente. Um RxCUI de
        -- apresentacao (SCD/SBD/BN) identifica um produto, nao o principio ativo.
        rxcui is not null and nivel_ingrediente             as identidade_confiavel,

        nomes_originais,
        qtd_nomes_originais,
        rxnorm_consultado_em
    from agrupado

),

nao_informado as (

    select
        {{ chave_hash(["'" ~ chave_farmaco_nao_informado() ~ "'"]) }} as id_farmaco,
        cast(null as varchar)                               as rxcui,
        'Nao informado'                                     as nome_farmaco,
        cast(null as varchar)                               as nome_representativo,
        cast(null as varchar)                               as rxnorm_nome,
        cast(null as varchar)                               as rxnorm_tty,
        'nao_mapeado'                                       as tipo_correspondencia,
        cast(null as double)                                as score_correspondencia,
        false                                               as nivel_ingrediente,
        false                                               as mapeado_rxnorm,
        false                                               as identidade_confiavel,
        cast([] as varchar[])                               as nomes_originais,
        0                                                   as qtd_nomes_originais,
        cast(null as varchar)                               as rxnorm_consultado_em

)

select * from observados
union all
select * from nao_informado
