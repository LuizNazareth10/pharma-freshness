"""DAG semanal: RES (recalls) -> silver -> gold -> qualidade.

Por que o RES tem uma DAG propria em vez de virar mais uma tarefa da diaria
--------------------------------------------------------------------------
A cadencia da fonte manda. O Recall Enterprise System publica semanalmente; busca-lo de hora em
hora ou todo dia gasta cota da openFDA para receber, na maior parte das vezes, exatamente o
mesmo conteudo. Alinhar o agendamento a cadencia real da origem e o que separa um pipeline
eficiente de um que apenas parece ativo.

A separacao tambem isola a falha: a openFDA fora do ar no domingo nao pode impedir a ingestao
do DailyMed na segunda.

A janela movel de 90 dias (`RES_LOOKBACK_DAYS`) continua valendo: um recall antigo muda de
status sem mudar `report_date`, entao o extrator relê a borda e o UPSERT resolve o resto.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pharma_tarefas import (  # noqa: E402
    ingerir,
    notificar,
    publicar,
    transformar,
    validar_contratos,
)

FUSO = pendulum.timezone("UTC")

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="pipeline_farmacovigilancia_semanal",
    description="Ingestao semanal do RES, modelagem e publicacao das tabelas afetadas.",
    # Segunda-feira as 7h UTC, uma hora depois da diaria, para nao disputar o arquivo DuckDB
    # com ela caso a diaria atrase.
    schedule="0 7 * * 1",
    start_date=datetime(2026, 7, 1, tzinfo=FUSO),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["farmacovigilancia", "semanal", "fase-4"],
    doc_md=__doc__,
) as dag:
    ingestao_res = PythonOperator(
        task_id="ingestao_res",
        python_callable=ingerir,
        op_kwargs={"source": "res"},
        doc_md="Extrai recalls com janela movel de 90 dias e faz UPSERT em `bronze.res_recalls`.",
    )

    transformar_modelos = PythonOperator(
        task_id="transformar",
        python_callable=transformar,
        doc_md="`dbt build` completo: o RES alimenta dim_farmaco e metricas_frescor tambem.",
    )

    publicar_silver = PythonOperator(
        task_id="publicar_silver",
        python_callable=publicar,
        op_kwargs={"layer": "silver"},
    )

    publicar_gold = PythonOperator(
        task_id="publicar_gold",
        python_callable=publicar,
        op_kwargs={"layer": "gold"},
    )

    testes_qualidade = PythonOperator(
        task_id="validar_contratos",
        python_callable=validar_contratos,
        retries=0,
    )

    aviso_final = PythonOperator(
        task_id="notificar",
        python_callable=notificar,
        retries=0,
        trigger_rule="all_done",
    )

    (
        ingestao_res
        >> transformar_modelos
        >> publicar_silver
        >> publicar_gold
        >> testes_qualidade
        >> aviso_final
    )
