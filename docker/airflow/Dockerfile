# Imagem do Airflow com o pacote do pipeline instalado.
#
# Por que uma imagem propria, e nao a oficial pura
# ------------------------------------------------
# As tarefas chamam `pharma_pipeline` diretamente (PythonOperator), entao o interpretador que o
# Airflow usa precisa enxergar dlt, dbt, PyIceberg e Great Expectations. Montar so o codigo-fonte
# nao bastaria: faltariam as dependencias.
#
# O custo dessa escolha e real e vale registrar: as arvores de dependencia do Airflow e do
# pipeline passam a viver no mesmo ambiente e podem brigar por versoes. Em producao o padrao e
# separar -- KubernetesPodOperator, ECS ou uma imagem de worker propria -- para que atualizar o
# Airflow nao force atualizar o dbt. Aqui, num laboratorio de uma maquina, a imagem unica e
# muito mais simples de operar e o conflito e verificavel no proprio build (veja o smoke test
# no fim do arquivo).
ARG AIRFLOW_VERSION=3.1.8
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER airflow

# O arquivo de constraints do Airflow fixa as versoes com que aquela release foi testada.
# Usa-lo impede que a resolucao do pip rebaixe algo do proprio Airflow ao instalar o pipeline.
ENV PIP_CONSTRAINT=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

WORKDIR /opt/pharma

# Copiamos apenas os metadados primeiro para aproveitar o cache de camadas: mudar o codigo-fonte
# nao invalida a instalacao das dependencias.
COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src ./src

# As dependencias do pipeline nao constam do constraints do Airflow, entao a instalacao delas
# roda sem constraint. O `--no-deps` na segunda linha instala apenas o nosso pacote, ja que as
# dependencias vieram no passo anterior.
RUN PIP_CONSTRAINT= pip install --no-cache-dir \
        "dlt[filesystem]==1.29.1" \
        "pyarrow==22.0.0" \
        "pyiceberg[sql-sqlite,s3fs]==0.11.1" \
        "duckdb==1.4.5" \
        "dbt-core==1.10.22" \
        "dbt-duckdb==1.10.1" \
        "great-expectations==1.19.1" \
        "psutil==7.2.2" \
        "python-dotenv==1.2.2" \
        "requests==2.34.2" \
        `# pytest vive aqui porque os testes de integridade das DAGs exigem o Airflow, que so` \
        `# existe nesta imagem. Sem ele, o comando documentado no README nao roda.` \
        "pytest==9.1.1" \
    && pip install --no-cache-dir --no-deps -e .

# Smoke test de build: se as duas arvores de dependencia forem incompativeis, o build falha
# aqui -- e nao as 3h da manha, dentro de uma tarefa do scheduler.
RUN python -c "import airflow, dlt, duckdb, pyiceberg, great_expectations; \
from dbt.cli.main import dbtRunner; \
from pharma_pipeline.cli import build_parser; \
build_parser(); \
print('smoke test ok: airflow', airflow.__version__)"

WORKDIR /opt/airflow
