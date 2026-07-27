# Fase 2 — Ingestão batch (Dias 3–5)

Ao concluir esta fase, você terá dados reais de três fontes chegando ao MinIO em Parquet, tabelas Iceberg com snapshots consultáveis, extração incremental com watermark e uma prova automatizada de idempotência.

Mais importante: você conseguirá explicar por que cada peça existe.

## 1. Modelo mental do pipeline

```text
DailyMed / openFDA
        │
        │ GET, paginação, retry, watermark
        ▼
recursos dlt
        │
        │ append imutável
        ▼
MinIO: bronze/<fonte>/.../*.parquet
        │
        │ leitura + deduplicação por chave
        ▼
PyIceberg: UPSERT
        │
        ├── data/*.parquet
        └── metadata/* (snapshots, manifests, schema)
                    │
                    ▼
         consulta atual ou time travel
```

Três frases resumem a arquitetura:

1. **dlt traz e registra o dado bruto.**
2. **Parquet organiza bytes em colunas, mas não é uma tabela transacional.**
3. **Iceberg adiciona estado, commits, snapshots e time travel sobre arquivos.**

## 2. Pré-requisitos e instalação

O Python suportado nesta fase é 3.12. O ambiente virtual impede que bibliotecas do projeto contaminem o Python global.

```powershell
.\scripts\day1\Start-MinIO.ps1
.\scripts\day1\Initialize-MinIO.ps1
.\scripts\day3\Setup-Phase2.ps1
.\.venv\Scripts\Activate.ps1
```

O script de setup executa conceitualmente:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

`-e` instala o pacote em modo editável: alterar `src/` muda imediatamente o comando `pharma-pipeline`. O extra `[dev]` inclui pytest e Ruff.

Verifique:

```powershell
pharma-pipeline --help
docker compose ps
.\scripts\day1\Test-MinIO.ps1
```

## 3. Arquivos importantes

| Arquivo | Importância |
|---|---|
| `pyproject.toml` | Versão do Python, dependências fixadas, comando CLI e configuração de testes/lint. É o contrato de construção do pacote. |
| `.env.example` | Modelo público de configuração. Não contém segredo real. |
| `.env` | Credenciais locais; ignorado pelo Git. |
| `src/pharma_pipeline/config.py` | Lê e valida toda configuração em um único lugar. |
| `src/pharma_pipeline/contracts.py` | Fonte, tabela, chave, cursor e limites de página. |
| `src/pharma_pipeline/http.py` | Clientes DailyMed/openFDA, paginação, intervalo entre chamadas e retry. |
| `src/pharma_pipeline/resources.py` | Define o que cada linha significa, acrescenta `ingest_time` e declara o incremental do dlt. |
| `src/pharma_pipeline/ingestion.py` | Liga recurso dlt ao destino S3/MinIO e grava Parquet. |
| `src/pharma_pipeline/iceberg.py` | Lê bronze, deduplica, faz UPSERT, lista snapshots e executa time travel. |
| `src/pharma_pipeline/cli.py` | Interface operacional; evita que você precise executar funções internas manualmente. |
| `tests/` | Prova repetível das regras locais, sem consumir APIs. |
| `.local/dlt/` | Estado local dos pipelines dlt. É gerado e ignorado pelo Git. |
| `.local/iceberg/catalog.db` | Catálogo SQLite de desenvolvimento do Iceberg. Não é dado da tabela, mas aponta para ela. |

O significado exato de cada tabela está em [contratos-dados-fase-2.md](contratos-dados-fase-2.md). Os trade-offs estão em [decisoes-arquitetura-fase-2.md](decisoes-arquitetura-fase-2.md).

## Dia 3 — Primeira ingestão com dlt

### 4. O que é dlt

dlt é uma biblioteca Python de ELT. Nesta implementação ela:

- chama a API;
- itera páginas;
- normaliza nomes e tipos;
- mantém estado incremental;
- cria pacotes de carga;
- grava Parquet no destino compatível com S3;
- registra schema, estado e cargas concluídas em prefixos `_dlt_*`.

ELT significa carregar antes de aplicar regras de negócio. A bronze recebe o dado com transformação técnica mínima; o significado analítico será construído depois.

### 5. Execute uma amostra DailyMed

```powershell
.\scripts\day3\Ingest-DailyMed.ps1 -InitialDate 2026-07-20 -PageSize 10 -MaxPages 1
```

Ou diretamente:

```powershell
pharma-pipeline ingest dailymed `
  --initial-date 2026-07-20 `
  --end-date 2026-07-27 `
  --page-size 10 `
  --max-pages 1 `
  --pipeline-suffix _dia3_lab
```

Por que o sufixo é obrigatório com `--max-pages`? Porque uma amostra parcial não pode compartilhar o watermark da ingestão completa. Sem essa separação, o pipeline poderia lembrar a data máxima da primeira página e nunca buscar páginas omitidas.

Para uma execução completa, remova ambos:

```powershell
pharma-pipeline ingest dailymed --initial-date 2026-07-20
```

Essa execução percorre todas as páginas da janela. Antes de fazer um backfill grande, estime o volume e respeite a fonte.

### 6. Encontre o Parquet no MinIO

Abra <http://localhost:9001> e navegue:

```text
farmacovigilancia/
└── bronze/
    └── dailymed/
        └── bronze_dailymed/
            ├── _dlt_loads/
            ├── _dlt_pipeline_state/
            ├── _dlt_version/
            └── dailymed_spls/
                └── load_id=<id>/
                    └── <arquivo>.parquet
```

Os prefixos `_dlt_*` não são lixo:

- `_dlt_loads` comprova cargas completas;
- `_dlt_pipeline_state` sincroniza o watermark;
- `_dlt_version` registra o schema conhecido.

O `load_id` cria lineage entre o pacote do dlt e o arquivo físico.

### 7. Por que Parquet

JSON é excelente para transportar objetos, mas ruim para varrer uma única coluna em milhões de linhas. Parquet armazena por coluna, preserva tipos, comprime bem e permite que mecanismos leiam apenas as colunas necessárias.

Ainda assim, Parquet sozinho não sabe:

- qual arquivo é atual;
- se um commit foi atômico;
- qual linha tem a mesma chave de outra;
- como voltar ao estado anterior.

É por isso que o Dia 4 existe.

### 8. O carimbo `ingest_time`

Cada linha recebe:

```python
"ingest_time": datetime.now(UTC)
```

Isso é melhor do que salvar texto sem timezone. Arrow e Iceberg preservam um timestamp UTC tipado. `event_time` vem do relógio operacional escolhido na fonte; `ingest_time` vem do pipeline. Em FAERS, por exemplo, usamos recebimento pela FDA, não o início clínico do evento. Essa distinção será importante ao nomear as métricas de frescor.

Para conferir:

```powershell
pharma-pipeline sync dailymed
pharma-pipeline query dailymed `
  --columns setid,spl_version,published_date,event_time,ingest_time `
  --limit 3
```

Observe `published_date`, `event_time` e `ingest_time`.

## Dia 4 — Parquet bronze vira tabela Iceberg

### 9. O que Iceberg acrescenta

Iceberg não substitui Parquet. Ele usa arquivos Parquet e acrescenta uma camada de metadata:

```text
snapshot
  └── manifest list
      └── manifests
          └── data files + estatísticas
```

Uma consulta lê o snapshot atual, descobre quais arquivos pertencem à tabela e aplica pruning usando estatísticas. Um commit novo cria metadata nova e mantém a anterior referenciável.

### 10. Crie a tabela e o primeiro snapshot

```powershell
.\scripts\day4\Sync-Iceberg.ps1 -Source dailymed
```

Equivalente:

```powershell
pharma-pipeline sync dailymed
pharma-pipeline snapshots dailymed
```

Na primeira sincronização:

- PyArrow lê todos os Parquets da tabela bronze;
- colunas internas `_dlt_*` são removidas do contrato de negócio;
- repetições são reduzidas à maior `ingest_time` por `setid`;
- o catálogo cria `bronze.dailymed_spls` se ainda não existe;
- `upsert` insere as linhas;
- Iceberg publica um snapshot atômico.

### 11. Crie um segundo snapshot de forma controlada

Para reproduzir a aula com poucos registros, use páginas diferentes no mesmo pipeline de laboratório:

```powershell
pharma-pipeline ingest dailymed `
  --initial-date 2026-07-20 --end-date 2026-07-27 `
  --page-size 5 --start-page 1 --max-pages 1 `
  --pipeline-suffix _snapshot_lab
pharma-pipeline sync dailymed

pharma-pipeline ingest dailymed `
  --initial-date 2026-07-20 --end-date 2026-07-27 `
  --page-size 5 --start-page 2 --max-pages 1 `
  --pipeline-suffix _snapshot_lab
pharma-pipeline sync dailymed

pharma-pipeline snapshots dailymed
```

Se essas chaves já existirem na sua tabela, o UPSERT corretamente não criará novos inserts. Para observar dois snapshots garantidos, faça o laboratório em um ambiente recém-criado ou escolha páginas ainda não ingeridas.

### 12. Time travel por snapshot

Copie o `snapshot_id` mais antigo retornado por `snapshots`:

```powershell
pharma-pipeline query dailymed `
  --snapshot-id 2125780955903303349 `
  --columns setid,title,published_date,ingest_time `
  --limit 5
```

O número acima é apenas exemplo; use o seu.

Time travel por timestamp ISO:

```powershell
pharma-pipeline query dailymed `
  --as-of "2026-07-27T14:41:58Z" `
  --columns setid,title,published_date `
  --limit 5
```

O comando resolve o snapshot que era atual naquele instante. É o equivalente conceitual a:

```sql
SELECT *
FROM bronze.dailymed_spls
AS OF TIMESTAMP '2026-07-27 14:41:58';
```

Nesta fase a consulta é feita pela API do PyIceberg; uma engine SQL será introduzida junto com dbt.

### 13. Snapshot não é cópia completa

Iceberg não duplica toda a tabela em cada commit. Snapshots referenciam arquivos. Arquivos inalterados podem ser compartilhados por vários snapshots; operações de UPSERT acrescentam/removem referências de forma transacional.

Não apague manualmente arquivos em `iceberg/.../data` ou `metadata`. A exclusão correta exige manutenção Iceberg consciente de snapshots e retenção.

## Dia 5 — Incrementalidade e idempotência

### 14. Watermark na prática

Cada recurso declara um cursor:

```python
dlt.sources.incremental(
    "published_date",  # no FAERS, o cursor equivalente e receiptdate
    initial_value=initial_date,
    primary_key="setid",
)
```

Após uma carga bem-sucedida, o dlt guarda o maior valor visto. Na próxima execução, `start_value` delimita a consulta à API. A borda é inclusiva para capturar registros tardios no mesmo dia; a chave filtra o que já foi visto nessa borda.

Os cursores são:

- DailyMed: `published_date`;
- FAERS: `receiptdate` (captura versões novas de relatos antigos);
- RES: `report_date`.

O RES também usa uma janela móvel de 90 dias porque um recall antigo pode mudar de status sem ganhar uma nova chave. Configure com `RES_LOOKBACK_DAYS`. O payload canônico impede que uma simples releitura seja confundida com mudança real.

### 15. Idempotência

Uma operação é idempotente quando repetir a mesma entrada produz o mesmo estado final. Isso é vital porque retry é normal: rede cai, processo reinicia, orquestrador repete tarefa.

Prove:

```powershell
pharma-pipeline verify-idempotency dailymed
```

Resultado esperado:

```json
{
  "passed": true,
  "rows_before": 10,
  "rows_after": 10,
  "snapshots_before": 2,
  "snapshots_after": 2
}
```

Não basta comparar contagem. O verificador também exige zero inserts, zero updates e o mesmo snapshot atual.

### 16. Ingira FAERS e RES

Laboratório pequeno para as três fontes:

```powershell
.\scripts\day5\Run-Batch-Lab.ps1 -PageSize 5 -MaxPages 1
```

Comandos explícitos:

```powershell
pharma-pipeline run faers `
  --initial-date 2026-03-31 --end-date 2026-03-31 `
  --page-size 5 --max-pages 1 --pipeline-suffix _faers_lab

pharma-pipeline run res `
  --initial-date 2026-07-01 --end-date 2026-07-27 `
  --page-size 5 --max-pages 1 --pipeline-suffix _res_lab
```

Depois:

```powershell
pharma-pipeline query faers `
  --columns safetyreportid,receivedate,serious,ingest_time `
  --limit 2
pharma-pipeline query res `
  --columns recall_number,report_date,classification,status,ingest_time `
  --limit 2
pharma-pipeline verify-idempotency faers
pharma-pipeline verify-idempotency res
```

### 17. O que os dados representam

Não compare as fontes como se tivessem o mesmo grão:

```text
DailyMed ── uma bula/versionamento regulatório
FAERS    ── um relato de segurança com N drogas e M reações
RES      ── um registro de produto recolhido dentro de um evento de recall
```

Elas se complementam:

- DailyMed informa o que a rotulagem oficial diz;
- FAERS oferece sinais relatados no mundo real;
- RES informa ações de recolhimento e motivo;
- `openfda` dentro de FAERS/RES ajuda a ligar NDC, RxCUI, substância e SPL quando a harmonização existe.

Essa combinação permite priorizar investigação, mas não prova causalidade clínica.

## 18. Estratégia de execução completa

Quando quiser sair do modo amostra:

1. defina datas iniciais em `.env`;
2. remova `--max-pages` e `--pipeline-suffix`;
3. comece com uma janela curta;
4. reconcilie contagens e datas máximas;
5. só então amplie o backfill.

```powershell
pharma-pipeline run dailymed
pharma-pipeline run faers
pharma-pipeline run res
```

Para FAERS histórico, a API paginada não é a ferramenta ideal para milhões de relatos. Divida intervalos em janelas pequenas ou use downloads bulk oficiais. A ingestão diária incremental e o backfill histórico são problemas diferentes e podem usar caminhos diferentes.

## 19. Testes e qualidade de código

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m ruff format --check src tests
```

Os testes unitários não chamam a internet. Eles validam:

- chaves reais de cada contrato;
- parsing da data textual DailyMed;
- paginação e limite superior;
- 404 sem resultados no openFDA;
- formação da janela de datas;
- remoção de colunas técnicas;
- deduplicação pela ingestão mais recente.

Os testes reais executados durante a implementação validaram MinIO, dlt e PyIceberg ponta a ponta. Como dependem de rede e estado, permanecem como comandos operacionais, não como unit tests padrão.

## 20. Solução de problemas

### `Nenhum Parquet encontrado`

Rode `ingest` antes de `sync` e confira o console MinIO em `bronze/<fonte>`.

### `--max-pages ... exige --pipeline-suffix`

Isso é uma proteção. Acrescente algo como `--pipeline-suffix _meu_lab`, ou remova `--max-pages` para uma carga completa.

### Erro de conexão S3/MinIO

```powershell
docker compose ps
.\scripts\day1\Test-MinIO.ps1
```

Confirme `MINIO_ENDPOINT=http://localhost:9000` e as credenciais de `.env`.

### openFDA retorna 404

Uma busca válida sem resultados é tratada como carga vazia. Se esperava dados, confira a janela: em 27/07/2026, a maior `receivedate` observada no endpoint FAERS era 31/03/2026, mostrando que a disponibilidade do endpoint não acompanha necessariamente a data atual.

### openFDA ultrapassa paginação segura

Reduza a janela de datas. Para backfill amplo, use arquivos bulk. O pipeline falha explicitamente para não omitir registros em silêncio.

### Catálogo existe, mas MinIO foi apagado

`docker compose down -v` remove os objetos, porém `.local/iceberg/catalog.db` continua no host. Catálogo e storage ficam inconsistentes. Em um laboratório descartável, remova de forma consciente o catálogo local e reconstrua tudo; não faça isso em dados importantes.

### MinIO existe, mas `.local` foi apagado

Os arquivos Iceberg continuam no bucket, mas o catálogo SQLite perdeu o ponteiro. Essa é uma limitação declarada do catálogo de desenvolvimento. Produção usará catálogo compartilhado e persistente.

## 21. Critério de conclusão

Você concluiu a Fase 2 quando consegue demonstrar e explicar:

- um arquivo Parquet real em `bronze/dailymed`;
- `ingest_time` UTC em cada linha;
- a diferença entre arquivo Parquet e tabela Iceberg;
- dois snapshots e uma consulta do snapshot antigo;
- qual watermark e chave cada fonte usa;
- uma segunda execução com zero duplicatas;
- as diferenças de grão entre DailyMed, FAERS e RES;
- por que amostra limitada usa estado separado;
- por que FAERS não deve ser descrito como prova causal.

A frase final desta fase é:

> “O dlt captura incrementalmente e deixa evidência imutável na bronze; o Iceberg transforma arquivos em uma tabela transacional, e a chave torna retries seguros.”

## 22. Referências oficiais

- [dlt — destino filesystem/object storage](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem)
- [dlt — incremental por cursor](https://dlthub.com/docs/general-usage/incremental/cursor)
- [PyIceberg — API, UPSERT e consultas](https://py.iceberg.apache.org/api/)
- [PyIceberg — configuração S3 e catálogo SQL](https://py.iceberg.apache.org/configuration/)
- [DailyMed — API `/spls`](https://dailymed.nlm.nih.gov/dailymed/webservices-help/v2/spls_api.cfm)
- [openFDA — Drug Adverse Events](https://open.fda.gov/apis/drug/event/)
- [openFDA — Drug Enforcement/recalls](https://open.fda.gov/apis/drug/enforcement/)
- [openFDA — autenticação e limites](https://open.fda.gov/apis/authentication/)
