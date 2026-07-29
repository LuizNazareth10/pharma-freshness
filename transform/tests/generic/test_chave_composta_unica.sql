{#
  Teste generico: a combinacao de colunas e unica.

  O teste `unique` do dbt olha uma coluna de cada vez. Quando o grao e composto -- como
  (safetyreportid, drug_seq) -- nenhuma coluna sozinha e unica, e testar cada uma
  separadamente falharia sem que exista qualquer problema.

  Este teste declara o grao inteiro. E o unico teste que realmente verifica a frase escrita no
  cabecalho de cada modelo: "uma linha representa ...".

  O projeto escreve o proprio teste em vez de instalar `dbt_utils` para se manter sem
  dependencias externas de rede -- e porque o mecanismo de teste generico e curto o bastante
  para ser lido e entendido.

  Uso:
    data_tests:
      - chave_composta_unica:
          colunas: [safetyreportid, drug_seq]
#}

{% test chave_composta_unica(model, colunas) %}

with contagem as (

    select
        {{ colunas | join(', ') }},
        count(*) as qtd_linhas
    from {{ model }}
    group by {{ colunas | join(', ') }}
    having count(*) > 1

)

select * from contagem

{% endtest %}
