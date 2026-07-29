{#
  Dimensao de calendario.

  Grao: um dia.

  Uma dimensao de data gerada, e nao derivada dos dados observados, permite responder
  "quantos relatos houve na terca-feira passada?" mesmo quando a resposta e zero. Uma dimensao
  construida a partir das datas presentes nos fatos nao tem linhas para os dias sem evento, e
  esses dias somem dos relatorios em vez de aparecerem como zero.

  A chave `id_data` e o proprio dia no formato YYYYMMDD: legivel, ordenavel e estavel.
#}

with calendario as (

    select
        cast(dia as date) as data
    from unnest(range(
        cast('{{ var("data_minima_plausivel") }}' as date),
        cast('{{ var("data_minima_plausivel") }}' as date)
            + interval ({{ var("dias_calendario") }}) day,
        interval 1 day
    )) as t(dia)

),

final as (

    select
        cast(strftime(data, '%Y%m%d') as integer)       as id_data,
        data,
        cast(year(data) as integer)                     as ano,
        cast(month(data) as integer)                    as mes,
        cast(quarter(data) as integer)                  as trimestre,
        cast(day(data) as integer)                      as dia_do_mes,
        cast(dayofweek(data) as integer)                as dia_da_semana,
        strftime(data, '%Y-%m')                         as ano_mes,
        cast(year(data) as varchar) || '-T'
            || cast(quarter(data) as varchar)           as ano_trimestre,
        dayofweek(data) in (0, 6)                       as fim_de_semana
    from calendario

)

select * from final
