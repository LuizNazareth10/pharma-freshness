{#
  Macros de normalizacao compartilhadas entre os modelos silver e gold.

  Elas existem para que a MESMA regra seja aplicada em todos os lugares. Se cada modelo
  escrevesse seu proprio `upper(trim(...))`, duas tabelas poderiam divergir silenciosamente e
  o join entre elas perderia linhas sem ninguem perceber.
#}

{% macro nome_farmaco_normalizado(coluna) -%}
    {#
      Forma canonica de um nome de farmaco, usada como chave de juncao com o RxNorm.
      Passos: maiusculas, remocao de pontuacao de borda, colapso de espacos internos.
      Nao traduz nem corrige grafia -- isso e trabalho do RxNorm, nao de uma regex.
    #}
    nullif(
        trim(regexp_replace(
            regexp_replace(upper(trim(cast({{ coluna }} as varchar))), '[.,;:]+$', ''),
            '\s+', ' ', 'g'
        )),
        ''
    )
{%- endmacro %}


{% macro faers_sexo(coluna) -%}
    {# Codigos do FAERS para sexo do paciente. Valores fora do dominio viram NULL, nunca um
       rotulo inventado. #}
    case cast({{ coluna }} as varchar)
        when '1' then 'Masculino'
        when '2' then 'Feminino'
        else null
    end
{%- endmacro %}


{% macro chave_hash(colunas) -%}
    {#
      Chave substituta deterministica.

      md5 sobre as colunas do grao, separadas por um caractere que nao aparece nos dados.
      Ser deterministica e essencial: a mesma linha logica precisa gerar a mesma chave em toda
      execucao, senao o MERGE incremental criaria duplicatas em vez de atualizar.

      O separador evita colisao entre ('AB','C') e ('A','BC'). `coalesce` marca o NULL de forma
      explicita para que NULL e a string vazia nao gerem a mesma chave.
    #}
    md5(
        {%- for coluna in colunas %}
        coalesce(cast({{ coluna }} as varchar), '<null>')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
{%- endmacro %}


{% macro chave_identidade_farmaco(nome) -%}
    {#
      Identidade textual de um farmaco: o NOME normalizado como a fonte o reportou.

      Por que o RxCUI NAO entra nesta chave
      -------------------------------------
      A versao anterior desta macro usava o RxCUI quando ele existia e caia no nome quando nao.
      Parecia melhor -- unificava "TACROLIMUS" e "PROGRAF" numa linha so -- mas quebrou em
      producao, e a causa vale registrar.

      O RxCUI e ENRIQUECIMENTO, e enriquecimento melhora com o tempo: o cache do RxNorm cresce a
      cada execucao, e `RXNORM_MAX_LOOKUPS` garante que nomes novos fiquem sem resolver na
      primeira passagem. Um farmaco entrava no fato como `nome:CORTISONE`, o RxNorm o resolvia na
      execucao seguinte, e a dimensao -- reconstruida por inteiro -- passava a chama-lo
      `rxcui:3117`. As linhas de fato ja gravadas continuavam apontando para uma identidade que
      nao existia mais.

      Resultado observado em 2026-07-29: 6.066 linhas de fato orfas, e o teste de integridade
      referencial reprovando com razao.

      A regra que evita isso e geral: a chave substituta so pode depender de dados que a propria
      linha de fato ja carrega e que nao mudam. O nome reportado nunca muda; o RxCUI, sim.

      A conformacao por ingrediente nao se perde -- ela virou ATRIBUTO da dimensao
      (`id_ingrediente`, `rxcui`), que pode ser reescrito no lugar sem invalidar chave nenhuma.
      Contar por principio ativo passa a ser `group by id_ingrediente` em vez de depender de a
      chave ja vir colapsada.
    #}
    coalesce(
        '{{ prefixo_nome() }}' || {{ nome }},
        '{{ chave_farmaco_nao_informado() }}'
    )
{%- endmacro %}


{% macro id_farmaco_de(nome) -%}
    {# Chave substituta ESTAVEL do farmaco: depende so do nome reportado. #}
    {{ chave_hash([chave_identidade_farmaco(nome)]) }}
{%- endmacro %}


{% macro chave_identidade_ingrediente(rxcui, nome) -%}
    {#
      Identidade do INGREDIENTE, para rollup: o RxCUI quando resolvido, o nome quando nao.

      Esta e a antiga regra de identidade, agora no lugar certo. Ela vive como atributo da
      dimensao, e nao como chave de juncao, justamente porque pode mudar quando o RxNorm
      aprende algo novo sobre um nome.
    #}
    coalesce(
        '{{ prefixo_rxcui() }}' || {{ rxcui }},
        '{{ prefixo_nome() }}' || {{ nome }},
        '{{ chave_farmaco_nao_informado() }}'
    )
{%- endmacro %}


{% macro id_ingrediente_de(rxcui, nome) -%}
    {# Chave de agrupamento por principio ativo. NAO e chave estrangeira de fato. #}
    {{ chave_hash([chave_identidade_ingrediente(rxcui, nome)]) }}
{%- endmacro %}


{% macro prefixo_rxcui() -%}rxcui:{%- endmacro %}
{% macro prefixo_nome() -%}nome:{%- endmacro %}
{% macro chave_farmaco_nao_informado() -%}nao_informado{%- endmacro %}


{% macro json_array_texto(coluna, caminho) -%}
    {# Extrai um array JSON de strings como VARCHAR[], devolvendo lista vazia quando ausente. #}
    coalesce(
        try_cast(json_extract({{ coluna }}, '{{ caminho }}') as varchar[]),
        cast([] as varchar[])
    )
{%- endmacro %}


{% macro explodir_json_array(coluna, caminho) -%}
    {#
      Explode um array JSON preservando a posicao original do elemento.

      O indice vem de `range`, e nao de `row_number`, porque a ordem de saida de UNNEST nao e
      garantida por si so. A posicao do array e o unico identificador estavel de um item dentro
      do relato: o FAERS nao numera medicamentos nem reacoes.
    #}
    unnest(coalesce(
        try_cast(json_extract({{ coluna }}, '{{ caminho }}') as JSON[]),
        cast([] as JSON[])
    )) as elemento,
    unnest(range(
        1::bigint,
        cast(coalesce(json_array_length({{ coluna }}, '{{ caminho }}'), 0) as bigint) + 1
    )) as posicao
{%- endmacro %}
