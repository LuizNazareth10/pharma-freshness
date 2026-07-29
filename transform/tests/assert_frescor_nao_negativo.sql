-- Teste singular: o pipeline nunca captura um dado antes de ele existir.
--
-- `ingest_time` < `event_time` significa latencia negativa, o que e fisicamente impossivel e
-- na pratica denuncia um destes erros: horario local gravado como se fosse UTC, `event_time`
-- montado a partir do campo de data errado, ou relogio da maquina fora de sincronia.
--
-- Este teste protege diretamente a tese do projeto. A Fase 4 vai calcular o staleness gap a
-- partir da diferenca entre esses dois relogios; se a diferenca puder ser negativa, a metrica
-- inteira perde sentido.
--
-- Ha uma tolerancia de 1 hora: as fontes publicam datas com precisao de dia, e o
-- `event_time` e a meia-noite UTC daquele dia. Um registro capturado no mesmo dia em que foi
-- publicado pode ficar poucos minutos "antes" por arredondamento.

with violacoes as (

    select
        'fato_evento_adverso' as modelo,
        cast(id_evento as varchar) as chave,
        event_time,
        ingest_time,
        latencia_ingestao_horas
    from {{ ref('fato_evento_adverso') }}
    where latencia_ingestao_horas < -1

    union all

    select
        'fato_recall' as modelo,
        cast(id_recall as varchar) as chave,
        event_time,
        ingest_time,
        latencia_ingestao_horas
    from {{ ref('fato_recall') }}
    where latencia_ingestao_horas < -1

)

select * from violacoes
