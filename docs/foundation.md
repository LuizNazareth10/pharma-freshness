# Pipeline de Engenharia de Dados em Farmacovigilância

> Projeto de aprendizado: construção de um pipeline completo de dados  
> Tema: frescor diário em segurança de medicamentos (DailyMed, FAERS, RES, RxNorm)  
> Baseado na trilha de Engenharia de Dados · Edição 2026

---

## Índice

1. [Contexto e objetivo](#1-contexto-e-objetivo)
2. [Ajuste importante: batch vs. streaming](#2-ajuste-importante-batch-vs-streaming)
3. [O stack escolhido e por que](#3-o-stack-escolhido-e-por-que)
4. [Arquitetura geral](#4-arquitetura-geral)
5. [As fontes de dados](#5-as-fontes-de-dados)
6. [Plano de implementação dia a dia](#6-plano-de-implementação-dia-a-dia)
   - [Fase 1 — Fundação](#fase-1--fundação-dias-1-2)
   - [Fase 2 — Ingestão batch](#fase-2--ingestão-batch-dias-3-5)
   - [Fase 3 — Modelagem](#fase-3--modelagem-dias-6-8)
   - [Fase 4 — Orquestração](#fase-4--orquestração-dias-9-10)
   - [Fase 5 — Streaming](#fase-5--streaming-dias-11-12)
   - [Fase 6 — Fechamento](#fase-6--fechamento-dias-13-14)
7. [O fio condutor do tema](#7-o-fio-condutor-do-tema)
8. [Como usar IA para o código](#8-como-usar-ia-para-o-código)
9. [Próximos passos após o pipeline](#9-próximos-passos-após-o-pipeline)

---

## 1. Contexto e objetivo

### O problema do domínio

**Farmacovigilância** é a prática de detectar e reagir a eventos adversos de medicamentos assim que eles aparecem. O volume é enorme — a FDA processou mais de 2 milhões de relatos de eventos adversos em 2025 — e a informação de segurança tem **meia-vida curta**: um novo alerta pode mudar a conduta clínica no mesmo dia em que é publicado.

O problema técnico central é o **staleness gap**: um LLM só sabe o que viu no treino, e sistemas RAG tradicionais indexam em lotes. Entre a mudança no mundo e o sistema "saber" disso, existe um atraso que pode ter consequências reais.

### O objetivo do projeto

Construir um pipeline de engenharia de dados que:

- Ingere fontes públicas da FDA e da NLM diariamente e semanalmente
- Trata o **frescor** (quanto tempo os dados demoram a chegar ao sistema) como uma **variável de engenharia medida** — não como um efeito colateral da ingestão
- Produz camadas de dados bem definidas (bronze → silver → gold) com rastreabilidade completa
- Prepara a base para uma LLM ser ancorada nesse dado no futuro

Este documento cobre apenas a **engenharia de dados** — a LLM fica para uma fase posterior.

---

## 2. Ajuste importante: batch vs. streaming

Antes de escolher o stack, é necessário entender que **as fontes deste projeto não são de streaming de verdade**:

| Fonte | Cadência real |
|-------|--------------|
| DailyMed | Diária |
| FAERS | Diária (painel público desde 2025) |
| RES (recalls) | Semanal |
| RxNorm | Semanal |

Montar Kafka + Flink para dados que mudam uma vez por dia seria um antipadrão clássico: pagar a complexidade do streaming sem o benefício correspondente.

A abordagem adotada é:

- **Batch** para a ingestão real das fontes (é o que o cenário pede)
- **Um módulo isolado de streaming** com Kafka e Flink, onde o histórico do FAERS é *simulado* como um feed de eventos ao vivo — para aprender as ferramentas no contexto certo sem distorcer a arquitetura

Isso ensina as duas coisas e também ensina **quando cada uma cabe**, que é a habilidade mais valiosa.

---

## 3. O stack escolhido e por que

Todo o ambiente roda localmente com Docker, sem custo de nuvem na fase de aprendizado.

### Infraestrutura base

**Docker + docker-compose**

A base de tudo. Você sobe Kafka, Airflow, MinIO e o resto com um único comando. Aprende o Volume 10 da trilha na prática: containers, isolamento, reprodutibilidade.

```bash
docker-compose up -d
```

### Ingestão

**Python + dlt (data load tool)**

O `dlt` é o padrão moderno open-source para extrair de APIs REST. Cuida automaticamente de paginação, carga incremental e evolução de schema — exatamente os problemas do Volume 6. É preferível ao Fivetran/Airbyte neste contexto porque você vê o código e entende o mecanismo.

```python
import dlt

@dlt.resource(primary_key="report_id", write_disposition="merge")
def faers_events(updated_since=dlt.sources.incremental("receivedate")):
    # lógica de extração
    yield from api.get_events(since=updated_since.start_value)
```

### Object storage

**MinIO**

Um S3 compatível que roda no seu Docker. Você aprende object storage (Volume 5) sem precisar de conta na AWS. Quando migrar para produção, o código não muda — só troca o endpoint.

### Formato de tabela

**Apache Iceberg**

Sobre o MinIO, as tabelas são Iceberg. Isso dá:

- Time travel (consultar o dado como estava em uma data anterior)
- Evolução de schema sem reescrever histórico
- `event_time` vs `ingest_time` como campos nativos — a medida de frescor do projeto

O tempo entre os dois campos é o staleness gap medido como variável de engenharia.

### Transformação

**dbt**

Transforma os dados entre as camadas. Cada camada é um conjunto de modelos dbt:

- **Bronze → Silver**: limpeza, tipagem, deduplicação, normalização via RxNorm
- **Silver → Gold**: modelo dimensional (fato + dimensões)

Testes declarativos em YAML garantem que nenhum registro chega sem fonte e sem data.

### Qualidade

**Great Expectations**

Valida contratos de dados. No contexto deste projeto, a regra mais importante é: toda linha do modelo final deve ter `fonte`, `event_time` e `ingest_time` preenchidos — porque "toda resposta deve citar fonte e data" é um requisito do domínio.

### Orquestração

**Apache Airflow**

Coordena tudo: agenda, define dependências entre tarefas, faz retry em caso de falha, permite backfill quando necessário. Um DAG por pipeline (ingestão diária, ingestão semanal, transformação dbt).

### Streaming (módulo isolado)

**Kafka + Apache Flink**

Kafka recebe os eventos simulados do FAERS (um script publica o histórico evento a evento como se chegassem ao vivo). Flink consome, aplica deduplicação em janela de tempo e grava em Iceberg com merge-on-read. O objetivo pedagógico é comparar a latência e o custo operacional dos dois caminhos.

---

## 4. Arquitetura geral

```
┌─────────────────────────────────────────────────────┐
│              Fontes públicas (FDA / NLM)             │
│  DailyMed · diária    FAERS · diária    RES · semanal   RxNorm · semanal  │
└────────────────────────┬────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
    Python + dlt                  Kafka + Flink
    (batch real)              (streaming simulado)
          │                             │
          └──────────────┬──────────────┘
                         │
┌────────────────────────▼────────────────────────────┐
│          Lakehouse — Apache Iceberg sobre MinIO       │
│                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │    Bronze    │  │    Silver    │  │    Gold    │ │
│  │  bruto,      │  │  limpo,      │  │  fato +    │ │
│  │  como veio   │  │  tipado,     │  │  dimensões │ │
│  │  + timestamps│  │  dedup,      │  │  pronto    │ │
│  │              │  │  RxNorm      │  │  p/ consumo│ │
│  └──────────────┘  └──────────────┘  └────────────┘ │
└────────────────────────┬────────────────────────────┘
                         │
               ┌─────────┴─────────┐
               │                   │
            dbt               Great Expectations
        (transforma)           (valida qualidade)
               │
               └────────────────────────┐
                                        │
                               Apache Airflow
                           (orquestra tudo)
```

### O que cada camada armazena

**Bronze** — exatamente o que a fonte entregou. Nenhuma transformação de negócio, só padronização técnica mínima (encoding, tipos de data). Cada registro carrega:
- `event_time`: quando o evento ocorreu no mundo real
- `ingest_time`: quando o pipeline capturou o dado
- `fonte`: qual sistema originou o registro

**Silver** — dado limpo, tipado, deduplicado. RxNorm é aplicado aqui para normalizar nomes de fármacos entre as fontes (o mesmo medicamento pode se chamar "ibuprofeno", "Ibuprofen" ou "IBUPROFEN" dependendo da fonte). É a camada de integração.

**Gold** — modelo dimensional pronto para consumo. O esquema estrela da camada gold:

```
fato_evento_adverso
├── id_evento (PK)
├── id_farmaco (FK → dim_farmaco)
├── id_reacao (FK → dim_reacao)
├── id_data (FK → dim_data)
├── id_fonte (FK → dim_fonte)
├── gravidade
├── desfecho
├── event_time
└── ingest_time

dim_farmaco: rxcui, nome_normalizado, principio_ativo
dim_reacao: codigo_meddra, descricao, categoria
dim_data: data, mes, trimestre, ano
dim_fonte: nome, cadencia, url_api
```

---

## 5. As fontes de dados

Todas as fontes são públicas, sem necessidade de parceria, credenciamento ou dado sintético.

### DailyMed (bulas estruturadas)

- **Mantida por**: National Library of Medicine (NLM)
- **O que captura**: bulas vigentes — indicações, contraindicações, interações, reações adversas, boxed warnings
- **Formato**: SPL (Structured Product Labeling) em XML/JSON
- **Acesso**: API REST sem autenticação
  ```
  dailymed.nlm.nih.gov/dailymed/services/v2/spls.json
  ```
- **Cadência**: diária — é a fonte mais fresca do pipeline

### FAERS (eventos adversos)

- **Mantida por**: FDA
- **O que captura**: relatos individuais (ICSR) — fármaco envolvido, reação, gravidade, desfecho
- **Formato**: JSON via openFDA
- **Acesso**: sem autenticação obrigatória; em 27/07/2026, o limite oficial é 240 requisições/minuto tanto sem chave (por IP) quanto com chave (por chave), mas a chave eleva a cota diária de 1.000 para 120.000 requisições
  ```
  api.fda.gov/drug/event.json
  ```
- **Cadência**: diária desde 2025 (painel público); a API legada ainda documenta atualização trimestral — esse descompasso é parte da contribuição do projeto

### RES — Recall Enterprise System

- **O que captura**: recolhimentos de medicamentos já classificados por nível de risco
- **Formato**: JSON
- **Acesso**:
  ```
  api.fda.gov/drug/enforcement.json
  ```
- **Cadência**: semanal

### RxNorm

- **O que é**: vocabulário normalizado de nomes de fármacos, mantido pela NLM
- **Por que é essencial**: é a "ponte" que liga DailyMed, FAERS e RES sobre o mesmo fármaco, mesmo quando o nome escrito varia entre as fontes
- **Acesso**: API REST sem autenticação
  ```
  rxnav.nlm.nih.gov
  ```
- **Cadência**: semanal

---

## 6. Plano de implementação dia a dia

Cada "dia" é um bloco de aprendizado, não uma obrigação de 24 horas. O ritmo é seu. A ordem importa: cada fase depende da anterior funcionar.

---

### Fase 1 — Fundação (Dias 1–2)

**Objetivo**: o ambiente sobe e você sabe navegar nele.

#### Dia 1 — Subir o ambiente com Docker

**O que fazer**:
1. Criar um arquivo `docker-compose.yml` com o serviço MinIO
2. Subir com `docker-compose up -d`
3. Acessar o console web do MinIO em `localhost:9001`
4. Criar um bucket chamado `farmacovigilancia`
5. Subir um arquivo de teste via interface e verificar que aparece

**O que você aprende**: containers, object storage, como o "S3 local" funciona, docker-compose

**Referência da trilha**: Volume 10 (containers, Kubernetes), Volume 5 (object storage como fundação do lakehouse)

---

#### Dia 2 — Explorar as APIs manualmente

**O que fazer**:
1. Fazer uma requisição manual para a API do DailyMed e ver a estrutura do JSON
2. Fazer uma requisição para o FAERS e notar a paginação (`skip`, `limit`, `total`)
3. Testar o rate limit na prática (o que acontece quando passa do limite vigente; 240 req/min em 27/07/2026)
4. Anotar o schema de cada resposta: quais campos existem, quais são datas, quais podem ser nulos

**O que você aprende**: APIs REST na prática, paginação, rate limiting, como os dados chegam antes de qualquer pipeline

**Referência da trilha**: Volume 6 (rate limiting, paginação em ingestão via API)

> **Não escreva nenhum pipeline ainda.** Esse dia é só exploração. Entender os dados antes de codificar é a diferença entre um pipeline que funciona e um que parece funcionar.

---

### Fase 2 — Ingestão batch (Dias 3–5)

**Objetivo**: dado real da FDA chega ao lakehouse de forma confiável.

#### Dia 3 — Primeira ingestão com dlt

**O que fazer**:
1. Instalar dlt e configurar para gravar no MinIO
2. Criar um extrator simples para o DailyMed
3. Gravar o resultado como Parquet na pasta `bronze/dailymed/` do bucket
4. **Carimbrar cada registro** com `ingest_time = datetime.utcnow()` — esse campo é fundamental para medir o frescor depois

**Exemplo de estrutura**:
```python
@dlt.resource(primary_key="set_id", write_disposition="merge")
def dailymed_spls():
    for record in api.get_spls():
        yield {
            **record,
            "ingest_time": datetime.utcnow().isoformat(),
            "fonte": "dailymed"
        }
```

**O que você aprende**: extração com dlt, ELT (dado bruto vai primeiro para o destino), camada bronze, formato Parquet

**Referência da trilha**: Volume 5 (Parquet, lakehouse), Volume 6 (ELT, carga incremental)

---

#### Dia 4 — Transformar bronze em tabela Iceberg

**O que fazer**:
1. Usar PyIceberg (ou DuckDB com extensão Iceberg) para converter os Parquet soltos em uma tabela Iceberg de verdade
2. Fazer uma segunda ingestão de dados novos
3. Verificar que um segundo snapshot foi criado na tabela
4. Fazer uma consulta de time travel: ver a tabela como ela estava antes da segunda ingestão

```sql
-- time travel no Iceberg
SELECT * FROM bronze.dailymed_spls
AS OF TIMESTAMP '2026-07-26 00:00:00'
```

**O que você aprende**: formato de tabela transacional, snapshots, time travel na prática, por que Iceberg é diferente de Parquet solto

**Referência da trilha**: Volume 5 (Iceberg, time travel, evolução de schema)

---

#### Dia 5 — Ingestão incremental e idempotência

**O que fazer**:
1. Ajustar o extrator do DailyMed para ser incremental (só busca o que mudou desde a última execução usando um watermark)
2. Rodar a ingestão duas vezes seguidas com os mesmos dados de entrada
3. Confirmar que a segunda execução não gerou duplicatas (o MERGE por `primary_key` garante isso)
4. Adicionar FAERS e RES ao pipeline batch com a mesma lógica

**O que você aprende**: carga incremental, idempotência, MERGE/UPSERT, watermark, por que retry sem idempotência é perigoso

**Referência da trilha**: Volume 6 (idempotência, MERGE, watermarks)

---

### Fase 3 — Modelagem (Dias 6–8)

**Objetivo**: dado bruto vira modelo analítico consumível.

#### Dia 6 — Camada silver com dbt

**O que fazer**:
1. Instalar dbt e configurar para ler do Iceberg (via DuckDB ou Trino local)
2. Criar modelos `stg_dailymed`, `stg_faers`, `stg_res` que:
   - Limpam e tipam os campos
   - Deduplicam usando `ROW_NUMBER() OVER (PARTITION BY chave ORDER BY ingest_time DESC)`
   - Normalizam nomes de fármacos via RxNorm (consulta à API ou tabela de mapeamento local)
3. Materializar como tabelas Iceberg na camada silver

**Exemplo de modelo dbt**:
```sql
-- models/silver/stg_faers.sql
WITH dedup AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY report_id
      ORDER BY ingest_time DESC
    ) AS rn
  FROM {{ source('bronze', 'faers_events') }}
)
SELECT
  report_id,
  receivedate::DATE AS event_date,
  patient_drug_medicinalproduct AS farmaco_nome_original,
  reaction_reactionmeddrapt AS reacao,
  serious,
  ingest_time,
  fonte
FROM dedup
WHERE rn = 1
```

**O que você aprende**: dbt na prática, staging models, deduplicação, dimensão conformada com RxNorm

**Referência da trilha**: Volume 4 (modelagem dimensional), Volume 9 (dbt, transformação como código)

---

#### Dia 7 — Modelo dimensional na gold

**O que fazer**:
1. Definir o grão da tabela fato em uma frase: *"Cada linha representa um evento adverso individual relatado à FDA, por um fármaco específico"*
2. Construir `fato_evento_adverso` e as dimensões: `dim_farmaco`, `dim_reacao`, `dim_data`, `dim_fonte`
3. Usar materialização `incremental` para a fato (não recriar o histórico inteiro a cada execução)

**Exemplo de fato incremental**:
```sql
-- models/gold/fato_evento_adverso.sql
{{ config(
    materialized='incremental',
    unique_key='report_id',
    incremental_strategy='merge'
) }}

SELECT
  f.report_id,
  d.id_farmaco,
  r.id_reacao,
  dt.id_data,
  fo.id_fonte,
  f.serious AS gravidade,
  f.event_date,
  f.ingest_time
FROM {{ ref('stg_faers') }} f
LEFT JOIN {{ ref('dim_farmaco') }} d ON f.rxcui = d.rxcui
LEFT JOIN {{ ref('dim_reacao') }} r ON f.reacao = r.codigo_meddra
LEFT JOIN {{ ref('dim_data') }} dt ON f.event_date = dt.data
LEFT JOIN {{ ref('dim_fonte') }} fo ON f.fonte = fo.nome

{% if is_incremental() %}
WHERE f.ingest_time > (SELECT MAX(ingest_time) FROM {{ this }})
{% endif %}
```

**O que você aprende**: grão, star schema, fato e dimensões, materialização incremental

**Referência da trilha**: Volume 4 (grão, star schema, fato e dimensão, SCD)

---

#### Dia 8 — Testes de qualidade

**O que fazer**:
1. Adicionar testes dbt no `schema.yml`:
   - `unique` e `not_null` no campo `report_id` da fato
   - `relationships` garantindo que todo `id_farmaco` existe na `dim_farmaco`
2. Criar testes Great Expectations:
   - Nenhuma linha pode ter `ingest_time` nulo
   - Nenhuma linha pode ter `fonte` nulo
   - `event_date` deve estar dentro de um range plausível
3. Rodar `dbt test` e ver os resultados

**Exemplo de schema.yml**:
```yaml
models:
  - name: fato_evento_adverso
    columns:
      - name: report_id
        tests: [unique, not_null]
      - name: ingest_time
        tests: [not_null]
      - name: id_farmaco
        tests:
          - relationships:
              to: ref('dim_farmaco')
              field: id_farmaco
```

**O que você aprende**: testes de dados, reconciliação, por que "toda resposta deve citar fonte e data" vira um teste automático

**Referência da trilha**: Volume 6 (reconciliação pós-carga), Volume 9 (testes dbt, qualidade como padrão)

---

### Fase 4 — Orquestração (Dias 9–10)

**Objetivo**: o pipeline roda sozinho, se recupera de falhas e reporta o que aconteceu.

#### Dia 9 — Primeiro DAG no Airflow

**O que fazer**:
1. Adicionar Airflow ao docker-compose
2. Criar um DAG `pipeline_diario` com a sequência:
   - `ingestao_dailymed` → `ingestao_faers` → `dbt_silver` → `dbt_gold` → `testes_qualidade` → `notificar`
3. Configurar `catchup=False` (se deixar True por acidente você vai disparar meses de execuções retroativas)
4. Testar retry: derrubar um serviço no meio da execução e ver o Airflow tentar novamente

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='pipeline_farmacovigilancia',
    schedule_interval='0 6 * * *',  # todo dia às 6h
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=2),
    }
) as dag:
    ...
```

**O que você aprende**: DAGs, retry, timeout, dependências entre tarefas, catchup vs backfill

**Referência da trilha**: Volume 9 (Airflow, retry, backfill, catchup)

---

#### Dia 10 — Medir o frescor (o coração do tema)

**O que fazer**:
1. Criar uma tabela `metricas_frescor` que para cada execução do pipeline registra:
   - Por fonte: `event_time_mais_recente`, `ingest_time`, diferença entre os dois (o staleness gap)
2. Criar um modelo dbt que calcula essa métrica automaticamente
3. Criar um alerta no Airflow quando o staleness gap de qualquer fonte ultrapassar um limiar

```sql
-- models/gold/metricas_frescor.sql
SELECT
  fonte,
  MAX(event_time) AS event_time_mais_recente,
  MAX(ingest_time) AS ultimo_ingest,
  DATEDIFF('hour', MAX(event_time), MAX(ingest_time)) AS staleness_gap_horas
FROM {{ ref('fato_evento_adverso') }}
GROUP BY fonte
```

**O que você aprende**: observabilidade do pipeline, staleness gap como variável medida, SLA em dados

**Referência da trilha**: Volume 6 (watermarks, reconciliação), Volume 9 (SLA misses, DataOps)

> **Este é o momento mais importante do projeto.** Quando você mede o staleness gap, você transforma o "frescor diário" de intenção em número. Isso é exatamente o que a apresentação propõe: tratar frescor como variável de engenharia, não efeito colateral.

---

### Fase 5 — Streaming (Dias 11–12)

**Objetivo**: aprender Kafka e Flink no contexto certo, entendendo quando streaming é e não é adequado.

#### Dia 11 — Kafka: simular o feed de eventos

**O que fazer**:
1. Adicionar Kafka ao docker-compose (imagem Bitnami é a mais simples)
2. Criar um tópico `faers.eventos`
3. Escrever um script produtor que lê o histórico FAERS e publica evento a evento, com um delay artificial (ex: 1 evento por segundo), simulando chegada contínua
4. Criar um consumidor simples e ver os eventos chegando
5. Explorar partições e offsets no console do Kafka

```python
from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for evento in historico_faers:
    producer.send('faers.eventos', value=evento)
    time.sleep(0.1)  # simula chegada contínua
```

**O que você aprende**: tópicos, partições, offset, produtor, consumidor, commit de offset

**Referência da trilha**: Volume 7 (arquitetura Kafka, consumer groups, o caminho de uma mensagem)

---

#### Dia 12 — Flink: consumir e gravar no lakehouse

**O que fazer**:
1. Configurar um job Flink (pode começar com PyFlink) que:
   - Lê do tópico `faers.eventos`
   - Aplica deduplicação por `report_id` numa janela de 10 minutos
   - Grava em Iceberg com merge-on-read (MoR)
2. Comparar a latência desse caminho com o batch (horas vs. segundos)
3. Refletir: para um dashboard que atualiza a cada 4 horas, qual caminho vale mais a pena operar?

**O que você aprende**: processamento stateful, janelas de tempo, merge-on-read, quando streaming justifica sua complexidade

**Referência da trilha**: Volume 7 (Flink, janelas, exactly-once), Volume 5 (copy-on-write vs merge-on-read)

---

### Fase 6 — Fechamento (Dias 13–14)

**Objetivo**: o projeto está documentado, reproduzível e pronto para a próxima fase.

#### Dia 13 — Camada de consumo e documentação

**O que fazer**:
1. Rodar `dbt docs generate && dbt docs serve` para ver o lineage completo de todas as tabelas
2. Criar views ou tabelas gold específicas para o futuro consumo pela LLM:
   - `alertas_recentes`: eventos adversos graves dos últimos 7 dias, com fonte e data citadas
   - `bulas_atualizadas`: bulas modificadas nos últimos 3 dias, com data de revisão
3. Documentar o grão de cada tabela em comentários dbt

**Exemplo de documentação dbt**:
```yaml
models:
  - name: alertas_recentes
    description: >
      Eventos adversos graves reportados nos últimos 7 dias.
      Grão: um evento adverso individual por linha.
      Toda linha tem fonte e ingest_time preenchidos (garantido por teste not_null).
```

**O que você aprende**: lineage, documentação como parte do pipeline, camada de serving

**Referência da trilha**: Volume 9 (dbt docs, DataOps)

---

#### Dia 14 — Revisão de ponta a ponta

**O que fazer**:
1. Parar todos os containers: `docker-compose down -v`
2. Subir tudo do zero: `docker-compose up -d`
3. Rodar o pipeline completo manualmente uma vez
4. Verificar que a tabela `metricas_frescor` está preenchida corretamente
5. Verificar que `dbt test` passa sem erros
6. Se funciona de forma reproduzível, o projeto está pronto

**O que você aprende**: reprodutibilidade como critério de qualidade, visão sistêmica de ponta a ponta

**Referência da trilha**: Volume 10 (IaC, reprodutibilidade)

---

## 7. O fio condutor do tema

O campo `event_time` vs `ingest_time` aparece desde o Dia 3 e se torna uma métrica formal no Dia 10. Esse é o **staleness gap** — a diferença entre quando algo aconteceu no mundo e quando o seu sistema soube disso.

Na apresentação de referência, esse conceito é o problema central: LLMs só sabem o que viram no treino, e RAG tradicional indexa em lotes, criando um atraso. A contribuição do projeto é tratar esse atraso como uma variável de engenharia — algo que se mede, monitora e otimiza — em vez de um efeito colateral inevitável.

Ao construir o pipeline, você vai ver esse conceito se materializar:

- **Dia 3**: você carimba `ingest_time` em cada registro
- **Dia 4**: o time travel do Iceberg te deixa "viajar" para estados anteriores da tabela
- **Dia 10**: você calcula o staleness gap por fonte e cria alertas quando passa de um limiar
- **Dia 12**: o caminho streaming reduz o gap de horas para segundos — e você mede a diferença

Quando você conectar a LLM nessa base no futuro, cada resposta dela poderá citar não só a fonte, mas também o `ingest_time` — e o sistema poderá alertar quando uma resposta está ancorada em dados que estão "velhos" além de um limiar.

---

## 8. Como usar IA para o código

Como você vai usar IA para escrever o código, uma disciplina importante:

**Antes de pedir o código de cada dia, escreva você mesmo, em uma frase, o que aquela etapa faz e por quê.**

Exemplos:

- Dia 3: *"Estou extraindo dados do DailyMed e gravando como Parquet no MinIO, carimbando ingest_time em cada registro para medir o frescor depois"*
- Dia 7: *"Estou construindo uma tabela fato cujo grão é um evento adverso individual, ligada a dimensões de fármaco, reação, data e fonte"*
- Dia 10: *"Estou calculando a diferença entre event_time e ingest_time por fonte para medir o staleness gap como variável"*

Se você consegue escrever a frase, você entendeu. Se não consegue, o código vai funcionar mas você não vai ter aprendido. **A IA escreve a sintaxe; a lógica precisa ser sua.**

---

## 9. Próximos passos após o pipeline

Ao final do Dia 14, você tem:

- Um lakehouse com três camadas bem definidas (bronze, silver, gold)
- Um modelo dimensional com fato de eventos adversos + dimensões
- Ingestão diária orquestrada e monitorada
- Medição do staleness gap como variável de engenharia
- Um módulo de streaming com Kafka e Flink (isolado e pedagógico)
- Documentação de lineage completa

O que vem a seguir para conectar a LLM:

1. **Camada semântica**: criar uma API simples (FastAPI) que expõe as views gold para a LLM consultar
2. **RAG sobre os dados**: indexar os textos de bulas (DailyMed) em um vetor store, usando o `ingest_time` como metadado para rankeamento por frescor
3. **Agente de farmacovigilância**: um agente LLM que usa as ferramentas de consulta ao lakehouse e ao vetor store, citando fonte e data em toda resposta
4. **Benchmark as-of**: um conjunto de perguntas com gabarito que muda conforme novas bulas chegam — para medir se o frescor realmente melhora a corretude das respostas

---

*Documento gerado a partir da trilha de Engenharia de Dados · Edição 2026*  
*Projeto: Frescor Diário em Farmacovigilância*
*UFJF — Programa de Pós-Graduação em Ciência da Computação*
