# Fase 4 — Orquestração (Dias 9–10)

> **Objetivo:** o pipeline roda sozinho, se recupera de falhas e reporta o que aconteceu.
> **Estado:** implementada e validada localmente.

Até aqui, o pipeline funciona — mas só quando alguém digita os comandos na ordem certa. A Fase 4
troca esse "alguém" por um agendador, e acrescenta a peça que dá nome ao projeto: a medição do
**staleness gap** como variável de engenharia.

---

## Índice

1. [O que foi construído](#1-o-que-foi-construído)
2. [A arquitetura da fase e por que ela é assim](#2-a-arquitetura-da-fase-e-por-que-ela-é-assim)
3. [Dia 9 — Primeiro DAG no Airflow](#3-dia-9--primeiro-dag-no-airflow)
4. [Dia 10 — Medir o frescor](#4-dia-10--medir-o-frescor)
5. [Como executar](#5-como-executar)
6. [Como consultar o resultado](#6-como-consultar-o-resultado)
7. [Resultados observados](#7-resultados-observados)
8. [O que quebrou de verdade](#8-o-que-quebrou-de-verdade)
9. [Solução de problemas](#9-solução-de-problemas)
10. [Limites conscientes desta fase](#10-limites-conscientes-desta-fase)

---

## 1. O que foi construído

| Peça | Onde | O que faz |
|---|---|---|
| 5 serviços de orquestração | `docker-compose.yml` | Postgres, init, api-server, scheduler, dag-processor |
| Imagem própria do Airflow | `docker/airflow/Dockerfile` | Airflow 3.1.8 com o pipeline instalado |
| DAG diária | `dags/pipeline_diario.py` | DailyMed + FAERS → silver → gold → qualidade → frescor |
| DAG semanal | `dags/pipeline_semanal.py` | RES, na cadência real da fonte |
| Funções das tarefas | `dags/pharma_tarefas.py` | Fronteira fina entre Airflow e o pacote do pipeline |
| Modelo de frescor | `transform/models/gold/metricas_frescor.sql` | Série temporal de medições, três relógios |
| Avaliação e alerta | `src/pharma_pipeline/freshness.py` | Julga as medições contra os SLOs declarados |
| Comando de frescor | `pharma-pipeline freshness` | Relatório legível ou JSON, com código de saída |

---

## 2. A arquitetura da fase e por que ela é assim

```
┌──────────────────────── Docker (profile: orquestracao) ────────────────────────┐
│                                                                                 │
│   airflow-db          airflow-scheduler      airflow-dag-processor              │
│   (Postgres)          (agenda e executa)     (lê os arquivos de DAG)            │
│       │                      │                       │                          │
│       └──────────────────────┴───────────────────────┘                          │
│                              │                                                  │
│                     airflow-apiserver  ──────►  http://localhost:8081           │
│                                                                                 │
│   Todos rodam a MESMA imagem: apache/airflow:3.1.8 + pharma_pipeline            │
└─────────────────────────────────────────┬───────────────────────────────────────┘
                                          │  volumes compartilhados
                          ┌───────────────┴────────────────┐
                          │                                │
                   ./.local  (catálogo Iceberg          ./transform
                   SQLite, DuckDB, cache RxNorm)        (projeto dbt)
                          │
                          ▼
                       MinIO  (Iceberg: bronze, silver, gold)
```

### Por que uma imagem própria do Airflow

As tarefas chamam `pharma_pipeline` diretamente, via `PythonOperator`. Para isso, o
interpretador do Airflow precisa enxergar `dlt`, `dbt`, `PyIceberg` e `Great Expectations` —
montar só o código-fonte não bastaria.

O custo é real e vale declarar: as árvores de dependência do Airflow e do pipeline passam a
conviver no mesmo ambiente. Em produção, o padrão é separá-las (`KubernetesPodOperator`, ECS,
ou uma imagem de worker própria), para que atualizar o Airflow não obrigue a atualizar o dbt.
Num laboratório de uma máquina, a imagem única é muito mais simples de operar — e o conflito,
se existir, aparece no `docker build`, não às 3h da manhã dentro do scheduler:

```dockerfile
RUN python -c "import airflow, dlt, duckdb, pyiceberg, great_expectations; ..."
```

Esse *smoke test* de build é o que garante que a escolha continua válida a cada rebuild.

### Por que o Airflow fica num profile

```bash
docker compose up -d                              # só MinIO — Fases 1 a 3
docker compose --profile orquestracao up -d       # + Postgres e Airflow
```

As Fases 1–3 não precisam de orquestrador. Deixar cinco containers subindo em todo `up -d`
tornaria o laboratório pesado sem necessidade.

### O estado é compartilhado com o host — e isso tem preço

O volume `./.local` é montado dentro do container. Isso permite rodar a DAG no Airflow e depois
inspecionar o resultado pelo CLI do host, com os mesmos comandos das fases anteriores.

A contrapartida: **o DuckDB e o catálogo SQLite do Iceberg aceitam um único escritor**. Rodar
`pharma-pipeline transform` no host enquanto uma DAG executa vai travar ou corromper. As DAGs
declaram `max_active_runs=1` para se proteger de si mesmas, mas não há como o Airflow impedir
um comando digitado no host.

Em produção isso desaparece: o catálogo vira um REST Catalog compartilhado (Polaris, Nessie,
Glue) e o motor de consulta deixa de ser um arquivo local.

---

## 3. Dia 9 — Primeiro DAG no Airflow

### 3.1 A sequência, e onde ela difere do documento de fundação

O documento propõe uma cadeia estritamente serial:

```
ingestao_dailymed → ingestao_faers → dbt_silver → dbt_gold → testes → notificar
```

A implementação paraleliza as ingestões:

```
ingestao_dailymed ─┐
                   ├─> transformar ─> publicar_silver ─> publicar_gold ─┐
ingestao_faers ────┘                                                    │
                                          ┌─────────────────────────────┘
                                          ▼
                          validar_contratos ─> verificar_frescor ─> notificar
```

O motivo é concreto: DailyMed e FAERS atingem **APIs diferentes** e gravam **tabelas bronze
diferentes**. Não existe ordem entre elas — encadear o que é independente só alonga a janela de
execução sem reduzir risco.

Já o que vem depois é serial de propósito, e não por conservadorismo: o dbt **escreve** no
arquivo DuckDB e o `publish` **lê** dele. Paralelizar ali produziria erro de bloqueio, não ganho
de tempo.

A ordem `publicar_gold → validar_contratos` também carrega uma regra: o Great Expectations valida
a tabela **já publicada no Iceberg**, não o modelo dentro do DuckDB. Ele é a reconciliação
pós-carga — precisa rodar depois da carga.

### 3.2 `catchup=False` não é detalhe de configuração

```python
catchup=False
```

Com `catchup=True`, o Airflow dispararia uma execução para **cada dia** entre `start_date` e
hoje. Aqui isso seria duplamente errado:

1. **Inútil.** As APIs entregam o estado atual, não um recorte histórico por data. Reexecutar a
   ingestão de 15 de julho não traz os dados de 15 de julho — traz os dados de hoje.
2. **Destrutivo.** Dezenas de execuções competiriam pelo mesmo arquivo DuckDB e pela cota da
   openFDA.

Existe um teste automatizado que falha se alguém ligar `catchup` sem querer.

### 3.3 Retry só onde repetir faz sentido

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}
```

As ingestões herdam isso porque suas falhas típicas são **transitórias**: rate limit da openFDA,
timeout de rede, MinIO reiniciando. E repetir é seguro porque a Fase 2 já garantiu
idempotência — watermark do dlt na leitura, UPSERT por chave na escrita.

Mas duas tarefas declaram `retries=0`:

```python
testes_qualidade = PythonOperator(task_id="validar_contratos", retries=0, ...)
frescor          = PythonOperator(task_id="verificar_frescor",  retries=0, ...)
```

Contrato violado não é falha transitória. Repetir daria exatamente o mesmo resultado e só
atrasaria o diagnóstico em 15 minutos.

O `execution_timeout` cobre a falha mais silenciosa de um pipeline agendado: uma tarefa travada
num socket segura o slot para sempre, e a execução seguinte nunca começa. Sem timeout, o
pipeline "não falha" — ele apenas para, sem alarme.

### 3.4 Por que a lógica não mora no arquivo da DAG

Um arquivo de DAG é reavaliado pelo processador **a cada poucos segundos**. Tudo que estiver no
nível do módulo roda nessa varredura. Uma DAG que abre o catálogo Iceberg durante o parse deixa
o scheduler lento e pode derrubar a varredura inteira quando o MinIO está fora do ar.

Por isso os imports pesados ficam **dentro** das funções, em `dags/pharma_tarefas.py`:

```python
def ingerir(source: str, **contexto):
    from pharma_pipeline.iceberg import sync_bronze_to_iceberg   # só quando a tarefa roda
    from pharma_pipeline.ingestion import ingest_source
```

As DAGs declaram ordem e política de retry. A regra de negócio continua no pacote
`pharma_pipeline`, testável sem Airflow — e é por isso que 51 dos testes do projeto rodam em
menos de um segundo, sem subir container nenhum.

### 3.5 Uma DAG semanal separada para o RES

A cadência da fonte manda. O Recall Enterprise System publica semanalmente; buscá-lo todo dia
gasta cota da openFDA para receber o mesmo conteúdo. A separação também isola a falha: a
openFDA fora do ar no domingo não impede a ingestão do DailyMed na segunda.

---

## 4. Dia 10 — Medir o frescor

Esta é a parte que o documento de fundação chama de "o momento mais importante do projeto".

### 4.1 Um número só esconde o problema

O exemplo do documento calcula um único gap:

```sql
DATEDIFF('hour', MAX(event_time), MAX(ingest_time)) AS staleness_gap_horas
```

Esse número mistura duas causas de atraso que precisam ser separadas, porque **uma é nossa e a
outra não**. A implementação mede três relógios:

| Métrica | Fórmula | O que mede | Quem controla |
|---|---|---|---|
| `atraso_da_fonte_horas` | `ultimo_ingest − event_time_mais_recente` | Idade do dado **quando o capturamos** | A origem |
| `atraso_do_pipeline_horas` | `agora − ultimo_ingest` | Há quanto tempo não capturamos | **Nós** |
| `idade_do_dado_horas` | `agora − event_time_mais_recente` | O que o consumidor recebe agora | Soma dos dois |

Confundir os dois primeiros leva ao erro operacional clássico:

- alguém vê o gap alto, conclui que o pipeline quebrou, investiga o código por horas — e
  descobre que a FDA é que não publicou nada;
- ou o contrário: o pipeline está parado há três dias e ninguém percebe, porque a fonte também
  é lenta e o número agregado parece "normal para essa fonte".

### 4.2 A tabela é um log, não um estado

Todas as outras tabelas da gold descrevem o estado atual: republicar sem dado novo não muda nada.
`metricas_frescor` é o oposto, de propósito. O documento pede uma tabela que *"para cada execução
do pipeline registra"* o atraso — ou seja, uma **série temporal de observações**.

```sql
{{ config(materialized='incremental', unique_key='id_medicao',
          incremental_strategy='delete+insert') }}
```

Grão: uma linha por `(fonte, medicao_em)`. Cada execução acrescenta medições novas, mesmo que os
dados não tenham mudado — porque *"nada mudou desde ontem"* também é informação de frescor.

É essa série que responde o que uma foto instantânea não responde: **o atraso do FAERS está
crescendo?** O DailyMed degradou depois de uma mudança de infraestrutura?

Todas as linhas de uma execução compartilham `medicao_em = {{ run_started_at }}`, o relógio do
dbt. Usar `now()` daria timestamps ligeiramente diferentes por fonte e impediria comparar as três
na mesma medição.

### 4.3 Por que ler da silver, e não do fato

O exemplo do documento agrupa `fato_evento_adverso` por `fonte`. Naquele fato, porém, `fonte`
vale sempre `'faers'` — ele só contém FAERS. O `GROUP BY` devolveria **uma única linha**, e as
outras duas fontes ficariam invisíveis justamente na tabela que existe para vigiá-las.

Os modelos `stg_*` cobrem as três fontes, cada uma no seu grão natural.

### 4.4 Os limiares são código versionado

Os SLOs vivem no seed `transform/seeds/fonte_referencia.csv`:

| fonte | `sla_ingestao_horas` | `sla_frescor_fonte_horas` |
|---|---|---|
| dailymed | 36 | 72 |
| faers | 36 | 3600 |
| res | 180 | 336 |

Mudar um limiar exige um commit — não é um ajuste invisível no console de alguém.

O valor do FAERS merece explicação, porque ele parece errado à primeira vista. A FDA documenta
o painel público como **diário**. O endpoint `api.fda.gov/drug/event.json`, porém, é recarregado
em lotes: em 2026-07-28, o `receiptdate` mais recente disponível era **2026-03-31**, com
`last_updated = 2026-04-28`. O atraso real medido foi de **2874 horas — cerca de 120 dias**.

Usar a cadência prometida (24 h) como limiar faria o alerta disparar em toda execução, para
sempre. Um alarme cronicamente vermelho não é vigilância — é ruído que ensina a equipe a ignorar
o painel. O limiar reflete a **cadência real medida**, com margem para o próximo lote.

### 4.5 Duas severidades, e por que não podem ser a mesma

```python
if relatorio.violacoes_pipeline:
    raise RuntimeError(...)          # falha a DAG: a culpa é nossa

for item in relatorio.violacoes_fonte:
    LOGGER.warning(...)              # avisa, mas não falha: não controlamos
```

| Violação | Ação | Falha a DAG? |
|---|---|---|
| SLO do **pipeline** | Investigar o pipeline: não rodou, falhou ou travou | **Sim** |
| SLO da **fonte** | Avisar quem consome que o dado está velho | Não |

Quando os dois estouram ao mesmo tempo, a causa que controlamos tem precedência e o incidente
**não é contado duas vezes** — há um teste para exatamente isso.

---

## 5. Como executar

### Pré-requisitos

Docker Desktop, o `.env` criado e a bronze já populada (Fase 2).

### Subir a orquestração

```powershell
.\scripts\day9\Start-Airflow.ps1            # use -Rebuild após mudar dependências
```

O script espera o `api-server` ficar saudável antes de dizer que está pronto — evita o falso
negativo de abrir o navegador cedo demais.

Console em <http://localhost:8081> (usuário e senha = `AIRFLOW_ADMIN_*` no `.env`;
padrão `admin` / `admin`).

> No Airflow 3 o comando `airflow users create` **não existe** mais. A autenticação padrão é
> o SimpleAuthManager: sem o arquivo de senhas, o apiserver gera uma senha aleatória e
> imprime só no log. O `airflow-init` deste projeto grava a senha do `.env` em
> `.local/airflow/passwords.json` para o login ser previsível.

> A porta padrão é **8081**, não 8080: no Windows, o próprio Docker Desktop costuma manter a
> 8080 ocupada.

### Rodar uma DAG sem esperar o agendamento

```powershell
.\scripts\day9\Test-Dag.ps1 -Dag diario                       # DAG inteira
.\scripts\day9\Test-Dag.ps1 -Dag diario -Tarefa verificar_frescor   # uma tarefa só
```

`airflow dags test` executa as tarefas na ordem do grafo, no processo atual, imprimindo tudo no
terminal. É a forma mais rápida de validar: não exige despausar, esperar horário, nem caçar log
no console web.

### Ligar o agendamento de verdade

As DAGs começam **pausadas**, de propósito — subir o Airflow não deve disparar ingestão sem que
alguém decida isso.

```powershell
docker compose --profile orquestracao exec airflow-scheduler `
    airflow dags unpause pipeline_farmacovigilancia_diario
```

### Ver o frescor

```powershell
.\scripts\day10\Show-Frescor.ps1                # avaliação + medições
.\scripts\day10\Show-Frescor.ps1 -Historico     # + snapshots da série
```

### Parar

```powershell
docker compose --profile orquestracao down      # preserva volumes e dados
```

---

## 6. Como consultar o resultado

### Relatório legível

```powershell
pharma-pipeline freshness --formato texto
```

```
Frescor avaliado em 2026-07-29 16:50 UTC
  dailymed: ok -- dado com 40 h de idade; fonte 37 h, pipeline 3 h.
  faers: ok -- dado com 2896 h de idade; fonte 2893 h, pipeline 3 h.
  res: ok -- dado com 184 h de idade; fonte 162 h, pipeline 22 h.
```

### Como código de saída, para automação

```powershell
pharma-pipeline freshness --fail-on-breach
```

Sai com **1** apenas quando o SLO do **pipeline** é violado. Atraso da fonte nunca derruba a
execução — pelo motivo da seção 4.5.

### A série temporal

```powershell
pharma-pipeline query gold.metricas_frescor `
  --columns fonte,medicao_em,atraso_da_fonte_horas,atraso_do_pipeline_horas,situacao `
  --limit 20
```

### Perguntas que a série responde

```sql
-- O atraso de uma fonte está crescendo ao longo do tempo?
SELECT fonte, medicao_em, atraso_da_fonte_horas
FROM gold.metricas_frescor
WHERE fonte = 'faers'
ORDER BY medicao_em;

-- Qual fonte mais violou o SLO do pipeline?
SELECT fonte, COUNT(*) AS violacoes
FROM gold.metricas_frescor
WHERE violou_sla_pipeline
GROUP BY fonte
ORDER BY violacoes DESC;

-- Qual a idade média do dado entregue, por fonte?
SELECT fonte, ROUND(AVG(idade_do_dado_horas), 1) AS idade_media_horas
FROM gold.metricas_frescor
GROUP BY fonte;
```

---

## 7. Resultados observados

### A DAG completa, de ponta a ponta

Execução real em 2026-07-29, todas as 8 tarefas com sucesso:

| Tarefa | Estado | O que fez |
|---|---|---|
| `ingestao_dailymed` | success | Índice SPL → `bronze.dailymed_spls` |
| `ingestao_faers` | success | Relatos → `bronze.faers_events` |
| `transformar` | success | `dbt build`: 166 nós, **PASS=165 WARN=1 ERROR=0** |
| `publicar_silver` | success | 7 tabelas Iceberg |
| `publicar_gold` | success | 8 tabelas, incluindo 177.387 linhas de fato |
| `validar_contratos` | success | Great Expectations nas tabelas publicadas |
| `verificar_frescor` | success | Severidade `ok` |
| `notificar` | success | Resumo no log |

O único WARN é o `drugcharacterization = 4` da seção 8 — comportamento projetado, não defeito.

### O staleness gap medido

```
Frescor avaliado em 2026-07-29 16:50 UTC
  dailymed: ok -- dado com 40 h de idade; fonte 37 h, pipeline 3 h.
  faers:    ok -- dado com 2896 h de idade; fonte 2893 h, pipeline 3 h.
  res:      ok -- dado com 184 h de idade; fonte 162 h, pipeline 22 h.
```

| Fonte | Evento mais recente | Atraso da fonte | Atraso do pipeline | Idade do dado |
|---|---|---|---|---|
| dailymed | 2026-07-28 | 37 h | 3 h | 40 h |
| faers | 2026-03-31 | **2893 h** | 3 h | **2896 h** |
| res | 2026-07-22 | 162 h | 22 h | 184 h |

A linha do FAERS é o achado central do projeto. Ela mostra, com número medido, exatamente o
problema que o documento de fundação descreve na introdução: entre o evento existir no mundo e o
sistema saber dele, há um atraso — e aqui ele é de **cerca de 120 dias**, numa fonte
oficialmente descrita como diária.

Não é um defeito do pipeline: o `atraso_do_pipeline_horas` de **3 h** mostra que a captura está
em dia. É uma característica da fonte, que agora está **medida, versionada e monitorada** em vez
de suposta.

Repare no contraste na mesma tabela: os três atrasos de pipeline (3 h, 3 h, 22 h) estão todos
dentro do SLO, enquanto o atraso do FAERS é de 120 dias. Um único número agregado misturaria as
duas coisas e esconderia justamente essa leitura.

---

## 8. O que quebrou de verdade

Sete problemas reais apareceram durante a implementação — e nenhum deles teria aparecido sem
rodar o pipeline de verdade, com volume de verdade. Vale a leitura: cada um representa uma
classe de falha diferente.

### O `upsert` matava o processo sem deixar mensagem

Publicar `silver.stg_faers_drugs` com 38.639 linhas simplesmente **matava o processo**: sem
exceção, sem traceback, sem log. Só o código de saída revelava o quê:

```
EXITCODE = -1073741571      # 0xC00000FD = STATUS_STACK_OVERFLOW
```

A causa está dentro do PyIceberg. Para saber quais linhas substituir, o `upsert` monta uma
expressão booleana com **uma comparação por linha da entrada**. Dezenas de milhares de linhas
produzem uma árvore profunda demais, e o interpretador estoura a pilha nativa — que Python não
consegue transformar em exceção.

A correção não foi aumentar a pilha, e sim **não pedir ao `upsert` o que ele não precisa
fazer**:

```python
novos, alterados = _separar_novos_de_alterados(candidates, chaves_existentes, join_cols)

with table.transaction() as transacao:
    if novos.num_rows:
        transacao.append(novos, ...)          # chave nova: não há nada que localizar
    for lote in _em_lotes(alterados, 2_000):  # chave existente: em lotes
        transacao.upsert(lote, ...)
```

Uma chave que ainda não existe na tabela não precisa localizar nada — um `append` resolve, sem
expressão nenhuma. Como o caso normal de um pipeline em crescimento é "chegaram linhas novas",
isso elimina o problema na maioria das execuções. O que sobra vai em lotes de 2.000.

Tudo acontece dentro de **uma transação**, então a publicação de uma tabela continua produzindo
**um único snapshot**, mesmo dividida em várias operações.

Resultado medido na mesma tabela: **de morrer após 30 s para concluir em 4,8 s**, publicando
35.050 linhas. O ganho não é só de robustez — `append` não reescreve arquivo existente.

### A chave do fármaco embutia um dado que muda com o tempo

Este é o mais importante dos seis, e é um defeito de **modelagem** que só o volume da Fase 4
revelou.

```
relationships_fato_evento_adverso_id_farmaco__id_farmaco__ref_dim_farmaco_
  fail - Got 6066 results, configured to fail if != 0
```

6.066 linhas do fato apontavam para um `id_farmaco` que não existia mais em `dim_farmaco`.

A causa, em uma frase: **a chave substituta embutia o RxCUI, e o RxCUI melhora com o tempo.**

A identidade do fármaco era "o RxCUI quando resolvido, o nome quando não". Parecia melhor —
unificava `TACROLIMUS` e `PROGRAF` numa linha só. Mas `RXNORM_MAX_LOOKUPS` limita quantos nomes
novos são resolvidos por execução, então a sequência era:

1. `CORTISONE` entra no fato incremental com identidade `nome:CORTISONE`;
2. na execução seguinte o RxNorm o resolve para o RxCUI `3117`;
3. `dim_farmaco`, que é reconstruída por inteiro, passa a chamá-lo `rxcui:3117`;
4. as linhas de fato já gravadas continuam apontando para `nome:CORTISONE` — que acabou de
   deixar de existir.

O fato é incremental de propósito (Dia 7). A dimensão é reconstruída de propósito. A combinação
das duas com uma chave instável é que estava errada.

**A regra geral:** uma chave substituta só pode depender de dados que a linha de fato já
carrega e que não mudam. O nome reportado nunca muda; o RxCUI, sim.

A correção move o enriquecimento para onde ele pode ser reescrito sem quebrar nada:

| Antes | Depois |
|---|---|
| Grão: uma identidade de ingrediente | Grão: **um nome de fármaco reportado** |
| `id_farmaco` = hash(rxcui ou nome) | `id_farmaco` = hash(**nome**) |
| Conformação embutida na chave | Conformação em `id_ingrediente` e `rxcui` — **atributos** |

A conformação por princípio ativo **não se perdeu** — mudou de lugar:

```sql
-- Contar por princípio ativo (o que a versão antiga fazia pela chave)
select d.rxnorm_nome, count(distinct f.safetyreportid)
from gold.fato_evento_adverso f
join gold.dim_farmaco d using (id_farmaco)
where d.identidade_confiavel
group by d.id_ingrediente, d.rxnorm_nome
```

Verificado após a correção: `amlodipine besylate` continua agrupando 2 nomes reportados
distintos em 177 relatos. E agora uma segunda pergunta ficou possível — contar por **nome
reportado**, que distingue marca de genérico. Antes, só a primeira era respondível.

O teste de regressão (`transform/tests/assert_chave_farmaco_estavel.sql`) monta a chave
esperada **explicitamente**, sem chamar a macro. Isso é deliberado: se o teste reaproveitasse a
macro, alguém poderia reintroduzir o RxCUI dentro dela e os dois lados mudariam juntos,
deixando o teste passar exatamente no cenário que ele existe para impedir. Sabotando a macro de
propósito, ele acusa 181.177 violações.

### O dbt segurava o banco e o publish não conseguia abrir

Este só existe **porque** o Airflow entrou em cena, e é o mais instrutivo.

```
_duckdb.ConnectionException: Connection Error: Can't open a connection to same database
file with a different configuration than existing connections
```

A tarefa `transformar` roda o `dbtRunner` **no processo atual** (decisão da Fase 3, necessária
para que o modelo Python `rxnorm_mapping` enxergue o pacote instalado). O dbt-duckdb guarda o
ambiente de conexão num singleton, e ao terminar o `dbt build` essa conexão continua **aberta e
em modo de escrita**.

A tarefa seguinte, `publicar_silver`, abre o mesmo arquivo em **somente leitura**. O DuckDB
recusa: duas configurações diferentes para o mesmo arquivo.

O detalhe que torna o caso interessante: **pela CLI isso nunca acontece**, porque cada comando
é um processo novo. O defeito só aparece quando transformação e publicação compartilham
processo — exatamente o que `airflow dags test` faz. Ou seja: um bug que a Fase 3 não tinha
como revelar, e que o caminho de validação documentado para o usuário exercita.

A correção é higiene de recurso — `run_dbt` agora libera a conexão num `finally`:

```python
try:
    result = dbtRunner().invoke(args)
finally:
    liberar_conexao_duckdb()
```

Um detalhe merece atenção: `close_all_connections()` do dbt-duckdb apenas descarta a referência
ao ambiente, **sem fechar o arquivo**. É preciso fechar o ambiente antes. E a inspeção tem de
ler o cache sem populá-lo — o acessor público `env()` *cria* um ambiente quando não existe, que
é o oposto do desejado. Há um teste de regressão para exatamente essa armadilha.

### O `--full-refresh` era descartado em `seed`

Ao acrescentar as colunas de SLO ao seed, o `dbt seed` passou a falhar com um erro de dialeto
CSV que não mencionava a causa:

```
Invalid Input Error: Error when sniffing file "fonte_referencia.csv".
It was not possible to automatically detect the CSV parsing dialect
```

A causa real: quando as **colunas** de um seed mudam, o dbt não altera a tabela existente — ele
tenta carregar o CSV novo na estrutura antiga. `--full-refresh` resolve, mas a flag estava
sendo filtrada:

```python
if full_refresh and args[0] in {"run", "build"}:      # "seed" faltava
```

`pharma-pipeline transform seed --full-refresh` não fazia nada — silenciosamente.

### Mudança de schema publicado dava erro ilegível

Publicar `dim_fonte` com colunas novas produzia isto, vindo de dentro do PyIceberg:

```
ValueError: Could not find column: 'sla_ingestao_horas'
```

A mensagem não diz qual tabela, qual modelo, nem o que fazer. Agora a verificação é explícita e
acontece antes:

```
gold.dim_fonte: o schema do modelo nao bate com o da tabela Iceberg publicada
(colunas novas no modelo: frescor_fonte_observacao, sla_frescor_fonte_horas,
sla_ingestao_horas). Republique com `pharma-pipeline publish gold.dim_fonte --recreate`
para descartar e recriar a tabela. Isso APAGA o historico de snapshots dela, portanto e
uma decisao explicita, e nao um efeito colateral da publicacao.
```

Evoluir schema publicado continua exigindo decisão consciente — o `--recreate` descarta o
histórico de snapshots daquela tabela.

### O motor de transformação encheu o disco

A execução seguinte morreu num ponto que não tinha relação aparente com a causa:

```
write /dev/stdout: Espaço insuficiente no disco.
```

O culpado era `.local/duckdb/pharma.duckdb`, com **1,46 GB**.

O DuckDB reaproveita blocos livres *dentro* do arquivo, mas nunca devolve espaço ao sistema
operacional. Cada `dbt build --full-refresh` reescreve todas as tabelas, e as versões antigas
viram espaço morto que o arquivo continua ocupando em disco. Não é defeito — é o custo de um
formato que privilegia escrita rápida. Mas num laboratório local ele consome o disco em
silêncio até algo quebrar longe da origem do problema.

A correção foi um comando de manutenção:

```powershell
pharma-pipeline compact
```

```json
{ "mb_antes": 1459.8, "mb_depois": 818.0, "reducao_percentual": 44.0 }
```

Ele usa `COPY FROM DATABASE`, que copia o conteúdo lógico para um arquivo novo e descarta o
espaço morto — muito mais rápido que reconstruir com o dbt, e sem refazer nenhuma consulta ao
RxNav. A troca só acontece **depois** que a cópia termina: se ela falhar no meio, o arquivo
original continua intacto.

Os 818 MB restantes são dado real, não desperdício: os modelos `src_*` materializam a bronze
localmente, e `src_faers_events` carrega `raw_payload` e `patient_payload` de 8.220 relatos.
É o preço, assumido na Fase 3, de não varrer o object storage a cada referência.

### A FDA enviou um código que o próprio padrão não define

A primeira execução completa da DAG ingeriu 7.720 relatos novos do FAERS — muito mais do que as
amostras usadas na Fase 3. O `dbt build` parou aqui:

```
FAIL 1 accepted_values_stg_faers_drugs_caracterizacao_codigo__False__1__2__3
RuntimeError: dbt build falhou
```

Investigando: **uma linha em 38.639**.

```
safetyreportid 26558175 | drug_seq 8 | HUMULIN R U-500 KWIKPEN | caracterizacao_codigo = 4
```

O padrão ICH E2B define apenas `1` (suspeito), `2` (concomitante) e `3` (interagente). Não
existe `4` — é erro de preenchimento na origem.

O modelo já lidava bem com isso: o `CASE ... ELSE NULL` devolveu `NULL` no rótulo em vez de
inventar significado. **O problema era a severidade do teste**, não a transformação.

Derrubar a ingestão diária inteira por causa de uma linha é desproporcional. Ignorar em
silêncio esconderia o dia em que a FDA realmente mudar o vocabulário. A correção expressa
exatamente essa distinção, em cada ferramenta no seu idioma:

```yaml
# dbt: avisa a partir de 1 ocorrência, falha a partir de 100
config:
  severity: error
  warn_if: ">0"
  error_if: ">100"
```

```python
# Great Expectations: tolera desvio pontual, reprova mudança sistemática
"kwargs": {"column": "caracterizacao_codigo", "value_set": [1, 2, 3, None], "mostly": 0.99}
```

Dezenas de ocorrências deixam de ser erro de digitação e passam a significar que a fonte mudou
de domínio — e aí alguém precisa revisar o modelo antes de continuar publicando. Há um teste
para cada lado dessa fronteira em `tests/test_quality.py`.

O valor bruto continua preservado na silver. Bronze e silver não apagam o que a fonte enviou;
quem traduz código em rótulo é a coluna derivada, e ela devolve `NULL` para o desconhecido.

### O que os cinco casos têm em comum

Nenhum deles era detectável por leitura de código, e nenhum apareceu nas Fases 1–3:

| Problema | Só aparece quando… | Classe |
|---|---|---|
| Chave do fármaco instável | o fato é incremental **e** o RxNorm melhora entre execuções | modelagem |
| `upsert` estoura a pilha | uma tabela cresce para dezenas de milhares de linhas | escala |
| Conexão do dbt presa | transformação e publicação dividem processo | acoplamento de recurso |
| `--full-refresh` em `seed` | um seed muda de colunas | flag filtrada em silêncio |
| Schema publicado divergente | um modelo ganha colunas | erro sem diagnóstico |
| Disco cheio | dezenas de reconstruções acumulam | crescimento silencioso |
| Código `4` da FDA | o volume real chega | dado fora do padrão |

Os cinco primeiros são defeitos de código ou de modelo e ganharam teste de regressão. O sexto
virou comando de manutenção. O sétimo virou política de severidade — e é o único que não tem
"correção", porque não é defeito nosso.

Vale notar o padrão: **nenhum deles é detectável lendo o código**. Todos exigiram executar o
pipeline duas vezes seguidas, com dado real e volume real. É exatamente para isso que a Fase 4
existe — orquestrar não é só agendar, é descobrir o que só quebra na segunda execução.

---

## 9. Solução de problemas

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `Bind for 0.0.0.0:8080 failed` | Docker Desktop já usa a 8080 | Ajuste `AIRFLOW_PORT` no `.env` |
| DAG não aparece no console | Erro de importação | `docker compose --profile orquestracao exec airflow-scheduler airflow dags list-import-errors` |
| Tarefa falha com `Connection refused` no MinIO | `MINIO_ENDPOINT` apontando para `localhost` | Dentro do container é `http://minio:9000`; já vem definido no compose |
| DuckDB travado ou corrompido | CLI do host e DAG escrevendo juntos | Pare a DAG, rode `pharma-pipeline transform build --full-refresh` |
| `MetricasIndisponiveis` | `metricas_frescor` ainda não publicada | `pharma-pipeline transform build` e depois `publish gold` |
| `api-server` não fica saudável | Migração ainda rodando | `docker compose --profile orquestracao logs airflow-init` |
| `scheduler` marcado `unhealthy` mas funcionando | O endpoint `/health` do scheduler é **desligado por padrão** | Já resolvido: `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true` no compose |
| Alerta de fonte sempre aceso | Limiar irreal para a fonte | Revise `sla_frescor_fonte_horas` no seed — veja a seção 4.4 |
| Nova execução trava em *"Filling up the DagBag"* | Execução anterior ficou presa em `running` e `max_active_runs=1` bloqueia a próxima | Veja abaixo |
| `Espaço insuficiente no disco` | Arquivo DuckDB cresceu com os `--full-refresh` | `pharma-pipeline compact` |
| `IO Error: ... já está sendo usado por outro processo` | Handle preso do compartilhamento de arquivos do Docker | `docker compose --profile orquestracao down`; se persistir, `wsl --shutdown` e reinicie o Docker Desktop |

### Execução anterior travada

Se um `airflow dags test` for interrompido no meio (Ctrl+C, container reiniciado), a execução
fica registrada como `running` para sempre. Como as DAGs declaram `max_active_runs=1`, nenhuma
execução nova começa — ela espera em silêncio.

O guardrail está funcionando como deveria; o que falta é limpar o estado órfão:

```powershell
docker compose --profile orquestracao exec airflow-db psql -U airflow -d airflow -c `
  "update task_instance set state='failed' where state in ('running','up_for_retry','queued','scheduled');
   update dag_run set state='failed', end_date=now() where state='running';"

docker compose --profile orquestracao restart airflow-scheduler
```

O restart do scheduler também libera o arquivo DuckDB, que pode continuar aberto pelo processo
interrompido — o sintoma no host é `IO Error: ... já está sendo usado por outro processo`.

---

## 10. Limites conscientes desta fase

O que existe aqui é suficiente para aprender e operar localmente, mas uma implantação
corporativa ainda exigiria:

- **Isolamento de execução.** Airflow e pipeline compartilham a mesma imagem. Em produção,
  `KubernetesPodOperator` ou uma imagem de worker separada evitam que atualizar um force
  atualizar o outro.
- **Catálogo compartilhado.** SQLite e DuckDB em arquivo aceitam um escritor. Um REST Catalog
  (Polaris, Nessie, Glue) remove a restrição de execução única.
- **Notificação de verdade.** `notificar` escreve no log. Slack, e-mail ou PagerDuty exigem um
  segredo, que não deve ser versionado neste repositório de estudo.
- **Backfill parametrizado.** As DAGs sempre ingerem a partir do watermark. Um backfill de
  janela específica ainda é feito pela CLI, com `--initial-date` e `--pipeline-suffix`.
- **SLO baseado em histórico.** Os limiares foram derivados de uma medição pontual. Com semanas
  de série acumulada, o correto é derivá-los de um percentil observado.
- **Alta disponibilidade.** Um scheduler, um Postgres sem réplica, volumes locais.

---

## Próximo passo

A [Fase 5](foundation.md#fase-5--streaming-dias-1112) monta um módulo isolado de Kafka e Flink
para comparar a latência do caminho streaming com a do batch — e entender quando cada um cabe.
Com `metricas_frescor` já registrando a latência do batch, essa comparação agora tem uma linha
de base **medida**, não estimada.
