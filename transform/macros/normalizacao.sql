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


{% macro chave_identidade_farmaco(rxcui, nome) -%}
    {#
      Forma textual da identidade de um farmaco: o RxCUI quando resolvido, o nome normalizado
      quando nao. Quando nem nome existe, cai no membro "nao informado".

      Esta macro e a UNICA definicao dessa regra. Ela nasceu de um bug real: a expressao estava
      repetida em quatro modelos, e o fato agrupava por NOME enquanto a dimensao agrupava por
      RxCUI. Dois nomes que resolvem para o mesmo ingrediente ("TACROLIMUS" e "TACROLIMUS
      ANHYDROUS") viravam uma linha na dimensao e duas no fato, quebrando o grao declarado.
    #}
    coalesce(
        '{{ prefixo_rxcui() }}' || {{ rxcui }},
        '{{ prefixo_nome() }}' || {{ nome }},
        '{{ chave_farmaco_nao_informado() }}'
    )
{%- endmacro %}


{% macro id_farmaco_de(rxcui, nome) -%}
    {# Chave substituta do farmaco, derivada da identidade textual acima. #}
    {{ chave_hash([chave_identidade_farmaco(rxcui, nome)]) }}
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
