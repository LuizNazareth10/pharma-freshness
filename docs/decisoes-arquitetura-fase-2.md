# Decisões de arquitetura da Fase 2

## 1. dlt para ingestão; PyIceberg para transações

O dlt controla extração, retry, schema técnico, estado incremental e escrita Parquet. O PyIceberg controla tabela, catálogo, UPSERT, snapshot e time travel.

Essa separação existe porque um arquivo Parquet não possui chave primária, transação ou histórico de commits. Chamar uma coleção de Parquets de “tabela com MERGE” esconderia justamente o conceito que o Dia 4 pretende ensinar.

## 2. Bronze imutável, Iceberg como estado consultável

O caminho Parquet é append-only:

```text
bronze/<fonte>/<dataset>/<tabela>/load_id=<id>/<arquivo>.parquet
```

O caminho Iceberg contém:

```text
iceberg/bronze/<tabela>/data/
iceberg/bronze/<tabela>/metadata/
```

O primeiro é a evidência bruta de cargas. O segundo é uma tabela lógica cujo snapshot aponta para arquivos de dados válidos naquele instante.

## 3. Catálogo SQLite somente para desenvolvimento

Iceberg precisa de um catálogo para mapear `bronze.dailymed_spls` à localização do metadata atual. Nesta fase, o catálogo é SQLite em `.local/iceberg/catalog.db`; os dados e metadados Iceberg ficam no MinIO.

Vantagem: ambiente simples, sem adicionar outro container antes de o conceito estar claro. Limite: SQLite não serve para múltiplos escritores concorrentes e o arquivo local precisa ser preservado. Em produção, a evolução natural é um REST Catalog como Lakekeeper/Polaris ou outro catálogo compartilhado.

## 4. UTC consciente de fuso

Foi usado `datetime.now(UTC)` em vez de `datetime.utcnow()`. O segundo produz um `datetime` sem informação de fuso e hoje é desencorajado pela própria biblioteca Python. Todo timestamp técnico do pipeline é UTC explícito.

As fontes oferecem datas com precisão de dia. Converter para `00:00:00+00:00` não cria precisão; apenas fornece um tipo uniforme. A documentação conserva essa limitação.

## 5. Payload completo mais colunas promovidas

FAERS contém estruturas profundas. Normalizá-las automaticamente em dezenas de tabelas na bronze tornaria a aula difícil e anteciparia decisões de grão. Por isso guardamos:

- colunas promovidas para identidade, data e inspeção;
- `raw_payload` canônico com todo o registro;
- `patient_payload` ou `openfda_payload` para objetos relevantes.

Na Fase 3, dbt poderá fazer a modelagem relacional conscientemente.

## 6. Watermark inclusivo com deduplicação na borda

O dlt mantém o maior cursor visto e hashes das chaves no valor máximo. A janela começa inclusiva. Isso é proposital: se novos registros aparecem com a mesma data do watermark, eles ainda podem ser capturados, enquanto as chaves já vistas são filtradas.

No FAERS, o cursor é `receiptdate`, não `receivedate`: a documentação da FDA define o primeiro como recebimento da informação mais recente e o segundo como primeiro recebimento do caso. Assim, uma revisão de um caso antigo volta a entrar na janela e atualiza `safetyreportid`.

No RES não há um `updated_at` confiável para toda mudança de status. Aplicamos `lag=90` ao cursor `report_date`, formando uma janela de atribuição configurável. Como isso relê dados, o sincronizador compara `raw_payload`: uma releitura byte-equivalente não muda a linha nem cria snapshot; uma mudança real atualiza a chave e carimba novo `ingest_time`.

Executar uma amostra com `--max-pages` pode não ler todos os registros do dia. Por isso o código exige `--pipeline-suffix` nesse modo. O estado parcial fica em um pipeline de laboratório e não avança o watermark principal.

## 7. Backfill e limite do openFDA

O endpoint openFDA aceita até 1.000 resultados por chamada e possui limite de paginação por `skip`. Backfills grandes devem ser divididos em janelas pequenas de datas; para histórico integral, os downloads bulk oficiais são mais adequados.

O pipeline lança erro em vez de continuar silenciosamente ao ultrapassar a paginação segura. Perder linhas sem avisar seria pior do que falhar.

## 8. Idempotência em duas camadas

Há duas barreiras:

1. o incremental do dlt não volta a materializar chaves já vistas na borda do watermark;
2. o `table.upsert(..., join_cols=[primary_key])` do PyIceberg insere novas chaves e atualiza chaves alteradas.

Se a entrada é idêntica, `verify-idempotency` exige:

- mesma contagem de linhas;
- mesma quantidade de snapshots;
- zero inserts;
- zero updates;
- mesmo snapshot atual.

## 9. Retry e uso responsável das APIs

O cliente HTTP tenta novamente somente GETs e respeita `Retry-After`. Os status 429 e 5xx usam backoff. O openFDA recebe um intervalo conservador entre páginas. Erros 404 de buscas openFDA são tratados como conjunto vazio porque esse endpoint usa 404 quando uma consulta válida não encontra resultados.

## 10. O que ainda não é produção

Esta fase é sólida para aprendizado e execução local, mas uma implantação corporativa também exigiria:

- catálogo Iceberg compartilhado e altamente disponível;
- credenciais de serviço com menor privilégio, nunca usuário root do MinIO;
- lock/orquestração para impedir escritores concorrentes;
- ingestão bulk para backfill completo do FAERS;
- métricas, alertas, tracing e dead-letter/reprocessamento;
- catálogo de dados, política de retenção e classificação de dados;
- testes de contrato contra fixtures versionadas e reconciliação com totais da fonte;
- tratamento formal de mudanças retroativas anteriores ao watermark.

Nesta fase, a sincronização Iceberg relê os Parquets bronze da fonte e reduz tudo pela chave antes do UPSERT. Isso maximiza clareza e segurança no volume do laboratório, porém custa O(histórico) por execução. A evolução para volume real é registrar arquivos/load IDs já commitados em uma tabela de controle Iceberg e ler apenas cargas novas. O marcador deve ser gravado somente depois do commit da tabela; se houver falha entre os dois, reprocessar é seguro graças ao UPSERT.

Esses itens não são escondidos: serão adicionados quando a complexidade correspondente entrar nas fases seguintes.
