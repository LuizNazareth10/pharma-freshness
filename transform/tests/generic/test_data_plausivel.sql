{#
  Teste generico: a data esta dentro de uma janela plausivel.

  Duas falhas reais que este teste pega:
    - datas absurdamente antigas (1900, 0001), tipicas de parsing errado de string para data;
    - datas no futuro, que em dado de origem quase sempre indicam fuso trocado ou digitacao.

  A borda superior aceita uma folga em dias porque as fontes usam precisao de dia e fusos
  diferentes: um registro publicado "hoje" em outro fuso pode parecer amanha em UTC.

  Uso:
    data_tests:
      - data_plausivel:
          minima: "1990-01-01"
          folga_futuro_dias: 2
#}

{% test data_plausivel(model, column_name, minima, folga_futuro_dias=2) %}

select
    {{ column_name }} as valor_invalido
from {{ model }}
where {{ column_name }} is not null
  and (
        {{ column_name }} < cast('{{ minima }}' as date)
     or {{ column_name }} > current_date + interval ({{ folga_futuro_dias }}) day
  )

{% endtest %}
