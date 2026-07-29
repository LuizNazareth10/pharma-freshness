{#
  Por padrao o dbt prefixa o schema do modelo com o schema do target, gerando `main_silver`.
  Aqui usamos o schema declarado tal como esta, para que o nome no DuckDB seja identico ao
  namespace Iceberg correspondente: `silver.stg_faers` no motor, `silver.stg_faers` no lakehouse.
  Nomes iguais nas duas pontas evitam erro humano na hora de publicar e de auditar.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
