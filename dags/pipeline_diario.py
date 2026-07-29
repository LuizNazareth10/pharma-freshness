"""DAG diaria: DailyMed e FAERS -> silver -> gold -> qualidade -> frescor.

Sequencia
---------
    ingestao_dailymed ─┐
                       ├─> transformar ─> publicar_silver ─> publicar_gold ─┐
    ingestao_faers ────┘                                                    │
                                              ┌─────────────────────────────┘
                                              v
                              validar_contratos ─> verificar_frescor ─> notificar

As duas ingestoes rodam EM PARALELO porque atingem APIs diferentes e gravam tabelas bronze
diferentes -- nao ha ordem entre elas. O documento de fundacao as encadeia em serie; encadear o
que e independente so faz a janela de execucao crescer sem reduzir risco.

O que vem depois e estritamente serial, e por um motivo real: o dbt escreve no arquivo DuckDB
e o publish le dele. DuckDB aceita um unico escritor por vez, entao paralelizar aqui
produziria erro de bloqueio, nao ganho de tempo.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# O diretorio das DAGs precisa estar no path para que `pharma_tarefas` seja importavel tanto
# no Airflow quanto no pytest que valida a integridade das DAGs.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pharma_tarefas import (  # noqa: E402
    ingerir,
    notificar,
    publicar,
    transformar,
    validar_contratos,
    verificar_frescor,
)

FUSO = pendulum.timezone("UTC")

default_args = {
    # Retry existe porque a falha esperada aqui e transitoria: rate limit da openFDA, timeout
    # de rede, MinIO reiniciando. Todas as tarefas sao idempotentes, entao repetir e seguro.
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # Sem timeout, uma tarefa travada em socket segura o slot para sempre e a proxima execucao
    # nunca comeca -- a falha silenciosa mais comum em pipeline agendado.
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="pipeline_farmacovigilancia_diario",
    description="Ingestao diaria, modelagem dbt, publicacao Iceberg e medicao de frescor.",
    schedule="0 6 * * *",
    start_date=datetime(2026, 7, 1, tzinfo=FUSO),
    # catchup=False e obrigatorio aqui. Com True, o Airflow dispararia uma execucao para CADA
    # dia entre start_date e hoje. Alem de inutil -- as APIs entregam o estado atual, nao um
    # recorte historico por data -- isso significaria centenas de execucoes concorrendo pelo
    # mesmo arquivo DuckDB e pelo rate limit da openFDA.
    catchup=False,
    # Uma execucao por vez: o DuckDB e o catalogo SQLite do Iceberg tem um unico escritor.
    max_active_runs=1,
    default_args=default_args,
    tags=["farmacovigilancia", "diario", "fase-4"],
    doc_md=__doc__,
) as dag:
    ingestao_dailymed = PythonOperator(
        task_id="ingestao_dailymed",
        python_callable=ingerir,
        op_kwargs={"source": "dailymed"},
        doc_md="Extrai o indice de bulas SPL e faz UPSERT em `bronze.dailymed_spls`.",
    )

    ingestao_faers = PythonOperator(
        task_id="ingestao_faers",
        python_callable=ingerir,
        op_kwargs={"source": "faers"},
        doc_md="Extrai relatos de eventos adversos e faz UPSERT em `bronze.faers_events`.",
    )

    transformar_modelos = PythonOperator(
        task_id="transformar",
        python_callable=transformar,
        doc_md="`dbt build`: materializa silver e gold e roda os testes de cada modelo.",
    )

    publicar_silver = PythonOperator(
        task_id="publicar_silver",
        python_callable=publicar,
        op_kwargs={"layer": "silver"},
        doc_md="Publica os modelos silver do DuckDB como tabelas Iceberg no MinIO.",
    )

    publicar_gold = PythonOperator(
        task_id="publicar_gold",
        python_callable=publicar,
        op_kwargs={"layer": "gold"},
        doc_md="Publica o esquema estrela e a serie de metricas de frescor.",
    )

    testes_qualidade = PythonOperator(
        task_id="validar_contratos",
        python_callable=validar_contratos,
        # Contrato violado nao e falha transitoria: repetir daria o mesmo resultado e so
        # atrasaria o diagnostico.
        retries=0,
        doc_md="Great Expectations sobre as tabelas JA publicadas -- a reconciliacao pos-carga.",
    )

    frescor = PythonOperator(
        task_id="verificar_frescor",
        python_callable=verificar_frescor,
        retries=0,
        doc_md=(
            "Compara o staleness gap com os SLOs. Falha apenas quando o atraso e do PIPELINE; "
            "atraso da FONTE vira aviso no log."
        ),
    )

    aviso_final = PythonOperator(
        task_id="notificar",
        python_callable=notificar,
        retries=0,
        # Roda mesmo se o frescor reprovar: o resumo e mais util justamente quando algo falhou.
        trigger_rule="all_done",
        doc_md="Resume a execucao no log. Ponto de integracao com Slack/e-mail em producao.",
    )

    [ingestao_dailymed, ingestao_faers] >> transformar_modelos
    transformar_modelos >> publicar_silver >> publicar_gold
    publicar_gold >> testes_qualidade >> frescor >> aviso_final
