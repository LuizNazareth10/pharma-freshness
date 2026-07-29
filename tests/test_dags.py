"""Testes de integridade das DAGs.

Por que testar DAG fora do Airflow
----------------------------------
Um erro de sintaxe ou de importacao em um arquivo de DAG nao aparece no `docker compose up`.
Ele aparece como "DAG Import Error" numa aba do console web que ninguem abriu -- e o pipeline
simplesmente nunca roda, silenciosamente. Estes testes transformam isso em falha de CI.

Alem da importacao, dois riscos especificos deste projeto sao verificados:

  * `catchup=True` por acidente. O documento de fundacao avisa que isso dispararia meses de
    execucoes retroativas. Aqui seria pior: dezenas de execucoes concorrendo pelo mesmo arquivo
    DuckDB e pelo rate limit da openFDA.

  * `max_active_runs` maior que 1. O DuckDB e o catalogo SQLite do Iceberg aceitam um unico
    escritor; duas execucoes simultaneas corromperiam ou travariam a transformacao.

Os testes sao pulados quando o Airflow nao esta instalado, porque o ambiente de
desenvolvimento local do projeto nao o inclui -- ele vive na imagem de orquestracao.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow so existe na imagem de orquestracao.")

from airflow.models import DagBag  # noqa: E402


def _dags_dir() -> Path:
    """Localiza a pasta de DAGs no repositorio ou dentro da imagem de orquestracao.

    No repositorio ela fica ao lado de `tests/`. Na imagem, o codigo e montado em /opt/pharma
    enquanto as DAGs vao para a pasta que o Airflow espera, /opt/airflow/dags. Assumir apenas o
    primeiro caso faz o DagBag ler um diretorio vazio e os testes falharem com "DAG nao existe",
    escondendo que o problema era o caminho.
    """
    candidatos = [Path(__file__).resolve().parents[1] / "dags"]
    configurado = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if configurado:
        candidatos.insert(0, Path(configurado))

    for candidato in candidatos:
        if candidato.is_dir() and any(candidato.glob("pipeline_*.py")):
            return candidato
    raise AssertionError(f"Pasta de DAGs nao encontrada. Procurei em: {candidatos}")


DAGS_DIR = _dags_dir()


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    # As DAGs importam `pharma_tarefas`, que vive ao lado delas.
    sys.path.insert(0, str(DAGS_DIR))
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_nenhuma_dag_falha_ao_importar(dagbag: DagBag) -> None:
    assert not dagbag.import_errors, f"DAGs com erro de importacao: {dagbag.import_errors}"


def test_as_duas_dags_esperadas_existem(dagbag: DagBag) -> None:
    assert set(dagbag.dag_ids) == {
        "pipeline_farmacovigilancia_diario",
        "pipeline_farmacovigilancia_semanal",
    }


def test_catchup_desligado_em_todas(dagbag: DagBag) -> None:
    """Com catchup ligado, o Airflow dispararia uma execucao por dia desde a start_date."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} dispararia execucoes retroativas."


def test_uma_execucao_por_vez(dagbag: DagBag) -> None:
    """DuckDB e o catalogo SQLite do Iceberg tem um unico escritor."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.max_active_runs == 1, f"{dag_id} permitiria escritores concorrentes."


def test_tarefas_de_ingestao_tem_retry(dagbag: DagBag) -> None:
    """Falha de rede e rate limit sao transitorios; as tarefas sao idempotentes."""
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            if task.task_id.startswith("ingestao"):
                assert task.retries >= 1, f"{task.task_id} deveria tentar de novo."


def test_tarefas_tem_timeout(dagbag: DagBag) -> None:
    """Sem timeout, uma tarefa travada segura o slot e a proxima execucao nunca comeca."""
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.execution_timeout is not None, f"{task.task_id} sem execution_timeout."


def test_ordem_da_dag_diaria(dagbag: DagBag) -> None:
    """A ordem carrega regras reais: publicar antes de validar, validar antes de medir."""
    dag = dagbag.dags["pipeline_farmacovigilancia_diario"]

    def depende_de(task_id: str) -> set[str]:
        return set(dag.get_task(task_id).upstream_task_ids)

    # As duas ingestoes sao independentes e devem correr em paralelo.
    assert depende_de("ingestao_dailymed") == set()
    assert depende_de("ingestao_faers") == set()

    assert depende_de("transformar") == {"ingestao_dailymed", "ingestao_faers"}
    assert depende_de("publicar_silver") == {"transformar"}
    assert depende_de("publicar_gold") == {"publicar_silver"}
    # A validacao de contrato le a tabela PUBLICADA: precisa vir depois do publish.
    assert depende_de("validar_contratos") == {"publicar_gold"}
    assert depende_de("verificar_frescor") == {"validar_contratos"}


def test_notificacao_roda_mesmo_apos_falha(dagbag: DagBag) -> None:
    """O resumo e mais util justamente quando algo deu errado."""
    for dag in dagbag.dags.values():
        notificar = dag.get_task("notificar")
        # `trigger_rule` e um enum de string; comparar pelo valor evita depender do repr.
        assert getattr(notificar.trigger_rule, "value", notificar.trigger_rule) == "all_done"


def test_dag_semanal_so_ingere_res(dagbag: DagBag) -> None:
    """A cadencia da fonte manda: o RES publica semanalmente."""
    dag = dagbag.dags["pipeline_farmacovigilancia_semanal"]
    ingestoes = [task.task_id for task in dag.tasks if task.task_id.startswith("ingestao")]

    assert ingestoes == ["ingestao_res"]
