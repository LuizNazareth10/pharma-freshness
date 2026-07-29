{#
  Dimensao conformada de farmaco.

  GRAO: um nome de farmaco normalizado, como a fonte o reportou.
  Chave: `id_farmaco` = hash(`nome_normalizado`).

  Por que o grao e o NOME e nao o ingrediente
  -------------------------------------------
  A primeira versao usava o ingrediente RxNorm como identidade: todos os nomes que resolvem
  para o mesmo RxCUI viravam UMA linha. Era mais elegante, e quebrou em producao.

  O RxCUI e enriquecimento, e enriquecimento melhora com o tempo. `RXNORM_MAX_LOOKUPS` limita
  quantos nomes novos sao resolvidos por execucao, entao um farmaco entra no fato hoje como
  `nome:CORTISONE` e, amanha, ja resolvido, a dimensao passa a chama-lo `rxcui:3117`. Como o
  fato e incremental, as linhas antigas continuam apontando para a identidade velha -- que a
  reconstrucao da dimensao acabou de apagar.

  Em 2026-07-29 isso produziu 6.066 linhas de fato orfas, e o teste `relationships` reprovou.

  A regra geral que evita a classe inteira desse problema: **uma chave substituta so pode
  depender de dados que a linha de fato ja carrega e que nao mudam.** O nome reportado nunca
  muda; o RxCUI, sim.

  A conformacao nao se perdeu -- ela virou atributo
  -------------------------------------------------
  `id_ingrediente` e `rxcui` agrupam nomes diferentes do mesmo principio ativo. A diferenca e
  que agora eles sao ATRIBUTOS: quando o RxNorm aprende algo novo, a dimensao e reescrita no
  lugar e nenhuma chave estrangeira e invalidada.

  Contar eventos por principio ativo:

      select d.rxnorm_nome, count(distinct f.safetyreportid)
      from gold.fato_evento_adverso f
      join gold.dim_farmaco d using (id_farmaco)
      where d.identidade_confiavel
      group by d.rxnorm_nome

  Contar por nome reportado (util para distinguir marca de generico) e o `group by` direto em
  `d.nome_farmaco`. As duas perguntas passaram a ser respondiveis; antes, so a primeira era.

  Membro "nao informado"
  ----------------------
  Nem todo recall do RES traz nome de substancia, generico ou marca. Sem uma linha para
  representar essa ausencia, o fato ficaria com chave estrangeira orfa. Deixar a FK nula
  obrigaria todo relatorio a usar LEFT JOIN e faria as linhas sumirem de qualquer INNER JOIN;
  um membro explicito mantem a integridade e deixa "nao informado" visivel como categoria.

  Farmacos que o RxNorm nao conhece PERMANECEM na dimensao, marcados por
  `mapeado_rxnorm = false`. Descarta-los silenciaria justamente os produtos mais irregulares --
  combinacoes, manipulados, importados -- que sao os que mais interessam a farmacovigilancia.
#}

with mapeamento as (

    select * from {{ ref('rxnorm_mapping') }}
    where nome_normalizado is not null

),

observados as (

    select
        {{ id_farmaco_de('nome_normalizado') }}             as id_farmaco,
        nome_normalizado,
        coalesce(rxnorm_nome, nome_normalizado)             as nome_farmaco,

        -- --- enriquecimento RxNorm: atributos, nunca chave ---------------------------------
        rxcui,
        rxnorm_nome,
        rxnorm_tty,
        tipo_correspondencia,
        score                                               as score_correspondencia,
        nivel_ingrediente,
        rxcui is not null                                   as mapeado_rxnorm,

        -- Identidade confiavel = resolvida no RxNorm E no nivel de ingrediente. Um RxCUI de
        -- apresentacao (SCD/SBD/BN) identifica um produto, nao o principio ativo.
        rxcui is not null and nivel_ingrediente             as identidade_confiavel,

        -- Rollup por principio ativo. Nomes distintos que resolvem para o mesmo ingrediente
        -- compartilham este valor, permitindo agrupar sem depender da chave.
        {{ id_ingrediente_de('rxcui', 'nome_normalizado') }} as id_ingrediente,

        consultado_em                                       as rxnorm_consultado_em
    from mapeamento

),

nao_informado as (

    select
        {{ chave_hash(["'" ~ chave_farmaco_nao_informado() ~ "'"]) }} as id_farmaco,
        cast(null as varchar)                               as nome_normalizado,
        'Nao informado'                                     as nome_farmaco,
        cast(null as varchar)                               as rxcui,
        cast(null as varchar)                               as rxnorm_nome,
        cast(null as varchar)                               as rxnorm_tty,
        'nao_mapeado'                                       as tipo_correspondencia,
        cast(null as double)                                as score_correspondencia,
        false                                               as nivel_ingrediente,
        false                                               as mapeado_rxnorm,
        false                                               as identidade_confiavel,
        {{ chave_hash(["'" ~ chave_farmaco_nao_informado() ~ "'"]) }} as id_ingrediente,
        cast(null as varchar)                               as rxnorm_consultado_em

)

select * from observados
union all
select * from nao_informado
