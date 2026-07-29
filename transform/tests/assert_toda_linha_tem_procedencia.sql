-- Teste singular: toda linha publicada sabe dizer de onde veio e quando chegou.
--
-- Este e o requisito de dominio do projeto virado codigo. A regra "toda resposta deve citar
-- fonte e data" so pode ser cumprida por uma LLM no futuro se CADA linha do modelo carregar
-- `fonte`, `event_time` e `ingest_time`. Uma unica linha sem procedencia produz uma afirmacao
-- que ninguem consegue auditar.
--
-- E um teste de contrato, nao de nulos avulsos: por isso cobre os dois fatos de uma vez e
-- devolve qual campo faltou, para que a falha seja acionavel sem investigacao adicional.

with campos_obrigatorios as (

    select
        'fato_evento_adverso'                       as modelo,
        cast(id_evento as varchar)                  as chave,
        case
            when fonte is null       then 'fonte'
            when event_time is null  then 'event_time'
            when ingest_time is null then 'ingest_time'
        end                                         as campo_ausente
    from {{ ref('fato_evento_adverso') }}

    union all

    select
        'fato_recall'                               as modelo,
        cast(id_recall as varchar)                  as chave,
        case
            when fonte is null       then 'fonte'
            when event_time is null  then 'event_time'
            when ingest_time is null then 'ingest_time'
        end                                         as campo_ausente
    from {{ ref('fato_recall') }}

)

select * from campos_obrigatorios
where campo_ausente is not null
