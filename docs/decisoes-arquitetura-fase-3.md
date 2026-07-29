# Decisões de arquitetura da Fase 3

Cada seção registra uma decisão, a alternativa descartada e o critério usado. O objetivo é que
uma pessoa que discorde consiga saber exatamente onde discordar.

---

## 1. DuckDB é o motor; Iceberg é o armazenamento

O documento de fundação pedia "materializar como tabelas Iceberg na camada silver". Fazer isso
diretamente pelo dbt **não é possível hoje**: o `dbt-duckdb` lê Iceberg, mas não escreve.

| Alternativa | Por que foi descartada |
|---|---|
| Deixar silver/gold só no arquivo DuckDB | Perde snapshot, time travel e legibilidade por outro motor |
| Trocar por Trino ou Spark | Traria um cluster e um catálogo distribuído antes de o conceito de modelagem estar claro |
| **Separar motor e armazenamento** | **Adotada** |

O DuckDB é efêmero e reconstruível; o Iceberg guarda o estado publicado com histórico de
commits. É a mesma divisão de papéis da Fase 2, onde o `dlt` extraía e o PyIceberg
transacionava.

O ganho concreto: **a função de `UPSERT` que publica a gold é literalmente a mesma que publica
a bronze** (`upsert_arrow`). Uma implementação de idempotência, não três parecidas.

---

## 2. Leitura do Iceberg pelo plugin nativo do dbt-duckdb

O `dbt-duckdb` traz um plugin `iceberg` que usa o PyIceberg e entrega Arrow ao DuckDB. Escrever
um plugin próprio teria sido código sem retorno.

A extensão `iceberg` do DuckDB seria a outra via, mas exige apontar o caminho do arquivo de
metadata mais recente — informação que o catálogo já resolve. Usar o mesmo PyIceberg que grava
mantém uma única implementação de Iceberg no projeto.

---

## 3. Os modelos `src_*` existem por dois motivos, não um

Referenciar a `source` Iceberg diretamente em cada modelo silver falhou de verdade:

```
TransactionContext Error: Catalog write-write conflict on create
with "Schema bronze Table bronze faers_events"
```

O plugin registra a fonte dentro da transação que a usa; dois modelos lendo a mesma fonte em
paralelo colidem no catálogo do DuckDB.

Baixar `threads` para 1 esconderia o problema em vez de resolvê-lo, e cobraria o preço em toda
execução futura. A materialização única resolve a colisão **e** elimina a releitura remota — no
FAERS, três modelos precisam do mesmo `patient_payload`.

Efeito colateral desejável: todos os modelos derivados enxergam exatamente o mesmo snapshot
Iceberg, mesmo que a bronze receba uma carga nova no meio da execução.

---

## 4. Uma única fonte de verdade para a configuração

O `profiles.yml` contém apenas `env_var(...)`. Quem preenche é `pharma-pipeline transform`, a
partir do mesmo `Settings` do código Python:

```
Settings  →  variáveis de ambiente  →  profiles.yml
```

Duas consequências: nenhum segredo é versionado, e a porta do MinIO existe em um lugar só.
Configuração duplicada não fica errada no dia em que é escrita — fica errada seis meses depois,
quando alguém muda um lado.

---

## 5. O grão do fato contradiz o exemplo do documento de fundação

O documento define o grão como *"um evento adverso individual, por um fármaco específico"*, mas
o exemplo usava `unique_key='report_id'`. **As duas coisas são incompatíveis.**

Foi adotado o grão descrito em palavras: **um par fármaco–reação distinto dentro de um relato**.

O critério foi a pergunta que o modelo precisa responder. *"Quantos relatos associam o fármaco X
à reação Y?"* é a pergunta central da farmacovigilância; com uma linha por relato, ela exigiria
abrir um JSON em tempo de consulta.

O custo é o fator de multiplicação (≈37× nesta base), que está documentado no modelo, no
contrato e nas colunas `qtd_medicamentos_relato` / `qtd_reacoes_relato`.

---

## 6. O produto cartesiano dentro do relato é intencional

O FAERS lista medicamentos e reações como listas independentes e **não diz** qual se liga a
qual. Havia três saídas:

| Alternativa | Consequência |
|---|---|
| Ligar só o suspeito primário à primeira reação | Inventa uma relação que a fonte não afirma |
| Manter os arrays sem cruzar | Empurra o problema para quem consulta |
| **Cruzar dentro do relato e documentar** | **Adotada** — é o que PRR/ROR fazem |

O par fármaco–evento por relato é a unidade padrão da análise de desproporcionalidade em dados
de notificação espontânea. A honestidade fica na documentação e nas colunas que expõem o fator
de multiplicação, não em esconder o cruzamento.

---

## 7. RxNorm como modelo Python do dbt

A normalização poderia ser um script executado antes do dbt. Como modelo, ela vira um **nó do
grafo de dependências**.

O ganho não é estético: a ordem de execução deixa de depender de quem digita os comandos, um
`dbt run` seletivo continua correto, e a linhagem aparece inteira no `dbt docs` — exatamente o
que o Dia 13 vai pedir.

O risco — uma chamada de rede dentro do grafo — é contido por três mecanismos: `farmaco_nomes`
reduz o universo a nomes distintos antes de qualquer chamada; o cache em disco sobrevive entre
execuções; e falha de rede degrada para `nao_mapeado` em vez de derrubar a transformação.

---

## 8. Fármaco não mapeado permanece no modelo

Um nome que o RxNorm não conhece **não é descartado**. Ele entra em `dim_farmaco` com `rxcui`
nulo e `mapeado_rxnorm = false`.

Descartar silenciaria justamente os produtos mais irregulares — combinações, manipulados,
importados — que são os que mais interessam à farmacovigilância. Um evento adverso não pode
desaparecer por falta de vocabulário.

O preço é que a dimensão mistura identidades de qualidades diferentes. Por isso existe
`identidade_confiavel`: `rxcui` presente **e** de nível ingrediente (IN/MIN/PIN). Comparações
de volume entre fármacos devem filtrar por ele.

---

## 9. Membro "Não informado" em vez de chave estrangeira nula

Recalls sem nome de substância precisavam de destino. Deixar `id_farmaco` nulo obrigaria todo
relatório a usar `LEFT JOIN` e faria as linhas sumirem de qualquer `INNER JOIN` — perda
silenciosa, o pior tipo.

O membro explícito mantém a integridade referencial (verificada por teste) e transforma a
ausência em categoria visível.

Em `dim_bula` a escolha foi oposta — `id_farmaco` nulo — porque ali a ausência significa "o
parsing do título falhou", que é informação sobre a nossa extração, não uma categoria de
negócio. **A regra não é "sempre use membro desconhecido"; é "decida o que a ausência
significa".**

---

## 10. A identidade do fármaco mora em uma macro

A expressão da identidade estava repetida em quatro modelos, e isso produziu um bug real: o
fato agrupava por **nome** enquanto a dimensão agrupava por **RxCUI**, gerando 49 chaves
duplicadas.

Hoje `chave_identidade_farmaco` é a única definição da regra. A duplicação de lógica de chave
entre fato e dimensão é uma das formas mais comuns de quebrar um esquema estrela, porque o
sintoma aparece longe da causa.

---

## 11. `delete+insert` em vez de `merge` puro

Os fatos usam `incremental_strategy='delete+insert'` sobre uma chave determinística. O efeito é
o mesmo de um MERGE por chave, com implementação mais simples e previsível no DuckDB.

O filtro incremental usa `>=`, não `>`:

```sql
where ingest_time >= (select coalesce(max(ingest_time), '1900-01-01') from {{ this }})
```

Com `>`, uma carga interrompida no meio de um mesmo `ingest_time` perderia as linhas restantes
para sempre. Reler a borda é seguro justamente porque a chave é determinística — é o mesmo
raciocínio do watermark inclusivo da Fase 2.

---

## 12. Modelos determinísticos, metadado de publicação no snapshot

Nenhum modelo usa `current_timestamp`, `random()` ou ordenação sem critério de desempate.
Nenhuma linha carrega `publish_time`.

Isso é o que torna a idempotência **provável**, e não apenas afirmada: republicar sem mudança
de dado devolve `unchanged: true` nas 14 tabelas, sem criar snapshot. Se houvesse um carimbo de
"agora" nas linhas, toda republicação criaria um snapshot novo e a propriedade seria impossível
de verificar.

O instante da publicação vive nas propriedades do snapshot Iceberg — junto com a camada, o
modelo e **o grão da tabela**, que assim viaja com os dados.

Foi por isso também que `consultado_em` fica **nulo** para nomes ainda não consultados, em vez
de receber o horário atual.

---

## 13. Detecção de mudança por camada

A função `_compare_columns` decide o que significa "a linha mudou":

| Camada | Comparação | Motivo |
|---|---|---|
| bronze | só `raw_payload` | É o registro original; `ingest_time` e `extraction_id` mudam a cada leitura sem que a fonte tenha mudado |
| silver / gold | todas as colunas fora da chave | Os modelos são determinísticos, então qualquer diferença é diferença real |

O resultado prático foi observado: ao ingerir 200 bulas novas, apenas `stg_dailymed`,
`dim_bula`, `dim_farmaco` e `dim_fonte` geraram snapshot. Os fatos de FAERS e RES ficaram
intocados, sem commit vazio.

---

## 14. Testes genéricos próprios em vez de `dbt_utils`

`dbt_utils` traria `unique_combination_of_columns` pronto. Foi preferido escrever
`chave_composta_unica` e `data_plausivel`.

O critério foi remover uma dependência de rede na instalação (`dbt deps` acessa o hub) e manter
o projeto legível de ponta a ponta — o mecanismo de teste genérico do dbt cabe em vinte linhas e
vale mais entendido do que importado.

O custo é honesto: em um projeto de produção, `dbt_utils` provavelmente compensaria.

---

## 15. Duas barreiras de qualidade com fronteiras distintas

| | dbt test | Great Expectations |
|---|---|---|
| Onde | dentro do DuckDB | na tabela Iceberg publicada |
| Quando | antes de publicar | depois de publicar |
| Pergunta | "o modelo está certo?" | "o que os consumidores enxergam está certo?" |

Não é redundância. A segunda pega o que a primeira não vê: conversão de tipo na escrita, UPSERT
em chave errada, publicação parcial, camada esquecida. É a reconciliação pós-carga do Volume 6.

As expectativas são deliberadamente as **regras de domínio** do projeto — procedência,
identidade, plausibilidade de data, latência não negativa — e não uma cópia dos testes do dbt.

E são testadas contra dados inválidos (`tests/test_quality.py`), porque uma suíte que só foi
vista passar poderia estar aprovando tudo.

---

## 16. Duas latências, porque há dois relógios

O FAERS tem `receivedate` (primeiro recebimento) e `receiptdate` (informação mais recente).
Medir a partir do primeiro dá, nesta base, média de ~943 dias — verdade sobre a **idade do
caso**, não sobre o atraso do pipeline.

Publicar só esse número seria uma resposta correta para a pergunta errada, e contaminaria a
métrica de staleness gap da Fase 4. Por isso o fato traz as duas, com nomes e descrições que
dizem o que cada uma mede.

---

## 17. Normalização de tipos na fronteira Arrow

O DuckDB exporta `large_string`, timestamps em nanossegundos e — o caso que quebrou de verdade
— `timestamp with time zone` marcado com o fuso da **sessão**:

```
Column 'event_time' has an unsupported type: timestamp[us, tz=America/Sao_Paulo]
```

`normalize_arrow` converte tudo para os tipos que o Iceberg aceita, com timestamps em UTC. A
conversão é de rótulo: o Arrow guarda o instante como epoch, então nenhum horário é deslocado.

Há também `SET TimeZone = 'UTC'` na conexão de publicação. Cinto e suspensório, de propósito: o
resultado precisa ser idêntico em qualquer máquina, independentemente do fuso do sistema
operacional.

---

## 18. Versões fixadas com motivo declarado

```toml
"dbt-core>=1.10,<1.11"      # dbt-duckdb declara só `dbt-core>=1.8.0`
"dbt-duckdb==1.10.1"
"pandas>=2.2,<3"            # great-expectations 1.x não declara limite superior
```

Ambos os limites existem porque a biblioteca **de baixo** não os declara. Sem eles, o pip
instalaria um `dbt-core` mais novo do que o testado contra este adaptador, e um `pandas 3`
posterior à série 1.x do Great Expectations. Instalações reproduzíveis não podem depender de
sorte na resolução.

---

## 19. O que ainda não é produção

Além dos limites herdados da Fase 2 (catálogo SQLite, credenciais root do MinIO, escritor
único), esta fase acrescenta os seus:

- **A silver é reconstruída inteira a cada execução.** Correto neste volume; em volume real,
  `stg_faers_drugs` precisaria ser incremental.
- **A publicação lê a tabela inteira do DuckDB e compara com a inteira do Iceberg.** É O(tabela)
  por execução. A evolução é publicar apenas partições ou lotes marcados como alterados.
- **`dim_bula` não é SCD tipo 2.** Depende de a bronze passar a guardar histórico de versões.
- **Sem hierarquia MedDRA**, por licenciamento.
- **Sem contrato de schema formal** (`dbt contract`) nem alerta de *schema drift* da fonte.
- **Sem particionamento nas tabelas Iceberg.** Em volume real, `fato_evento_adverso` deveria
  ser particionada por data de recebimento.
- **O teto de consultas ao RxNorm é global**, não por fonte, e nomes excedentes só são
  resolvidos na execução seguinte.

Nenhum desses itens está escondido: eles entram quando a complexidade correspondente entrar nas
fases seguintes.
