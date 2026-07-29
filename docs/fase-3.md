# Fase 3 — Modelagem (Dias 6–8)

> **Objetivo:** dado bruto vira modelo analítico consumível.
> **Estado:** implementada e validada localmente.

Na Fase 2, dados reais da FDA chegaram ao lakehouse. Eles estão corretos e rastreáveis, mas
ainda não respondem a nenhuma pergunta de negócio: os medicamentos estão dentro de um JSON, o
mesmo fármaco aparece escrito de cinco formas e não existe nada que se pareça com uma tabela de
fatos.

Esta fase resolve isso.

---

## Índice

1. [O que foi construído](#1-o-que-foi-construído)
2. [A arquitetura da fase e por que ela é assim](#2-a-arquitetura-da-fase-e-por-que-ela-é-assim)
3. [Dia 6 — Camada silver com dbt](#3-dia-6--camada-silver-com-dbt)
4. [Dia 7 — Modelo dimensional na gold](#4-dia-7--modelo-dimensional-na-gold)
5. [Dia 8 — Testes de qualidade](#5-dia-8--testes-de-qualidade)
6. [Como executar](#6-como-executar)
7. [Como consultar o resultado](#7-como-consultar-o-resultado)
8. [O que os testes encontraram de verdade](#8-o-que-os-testes-encontraram-de-verdade)
9. [Resultados observados](#9-resultados-observados)
10. [Solução de problemas](#10-solução-de-problemas)
11. [Limites conscientes desta fase](#11-limites-conscientes-desta-fase)

---

## 1. O que foi construído

| Camada | Tabelas | O que acontece nela |
|---|---|---|
| **bronze** (leitura) | 3 modelos `src_*` | Aterrissagem local do Iceberg no motor de consulta |
| **silver** | 7 modelos | Limpeza, tipagem, deduplicação, explosão de arrays, normalização RxNorm |
| **gold** | 5 dimensões + 2 fatos | Modelo dimensional pronto para consumo |

Ao todo: **17 modelos, 1 seed e 124 testes**, todos executados por um comando.

---

## 2. A arquitetura da fase e por que ela é assim

```
        Iceberg bronze (MinIO)                    ← Fase 2
                  │
                  │  plugin `iceberg` do dbt-duckdb (PyIceberg → Arrow)
                  ▼
        ┌───────────────────────────┐
        │   DuckDB  (.local/…)      │   MOTOR DE TRANSFORMAÇÃO
        │   bronze.src_*            │   efêmero, reconstruível
        │   silver.*   gold.*       │   é aqui que o dbt trabalha
        └───────────────────────────┘
                  │
                  │  pharma-pipeline publish  (PyIceberg UPSERT)
                  ▼
    Iceberg silver + gold (MinIO)                 ARMAZENAMENTO DE ESTADO
    snapshots · time travel · transações          durável, versionado
                  │
                  ▼
        Great Expectations                         validação de contrato
```

### A decisão central: DuckDB transforma, Iceberg armazena

O documento de fundação dizia "materializar como tabelas Iceberg na camada silver". Fazer isso
**diretamente pelo dbt** não é possível hoje: o `dbt-duckdb` lê Iceberg, mas não escreve.

Havia duas saídas. A primeira seria abandonar o Iceberg nas camadas derivadas e deixar tudo no
arquivo DuckDB — mas aí silver e gold perderiam snapshot, time travel e a possibilidade de
serem lidas por outro motor. A segunda, adotada aqui, é separar os papéis:

- **DuckDB é o motor.** Rápido, embutido, ótimo em SQL analítico e JSON. O arquivo
  `.local/duckdb/pharma.duckdb` pode ser apagado a qualquer momento e reconstruído.
- **Iceberg é o armazenamento.** É onde o estado publicado vive, com histórico de commits.

Isso é exatamente a mesma divisão da Fase 2, onde o `dlt` extraía e o PyIceberg transacionava.
A consequência prática mais importante: **o `UPSERT` que publica a gold é a mesma função que
publica a bronze**. Uma única implementação de idempotência, não três parecidas.

### Uma única fonte de verdade para a configuração

O `profiles.yml` do dbt não contém nenhuma credencial — apenas `env_var(...)`. Quem preenche
essas variáveis é o comando `pharma-pipeline transform`, que as deriva do mesmo objeto
`Settings` usado pelo código Python.

```
Settings  →  variáveis de ambiente  →  profiles.yml
```

Sem isso, a porta do MinIO estaria escrita em dois lugares e divergiria no primeiro dia em que
alguém mudasse uma delas.

---

## 3. Dia 6 — Camada silver com dbt

### 3.1 Os modelos `src_*`: por que existe uma camada a mais

O caminho óbvio seria cada modelo silver ler direto da `source` Iceberg. Duas coisas quebraram
esse caminho, e ambas ensinam algo:

1. **Custo.** Três modelos precisam do `patient_payload` do FAERS. Cada referência à `source`
   dispara uma varredura nova do Iceberg no MinIO — três leituras remotas do mesmo dado.
2. **Concorrência.** O plugin registra a fonte dentro da transação que a usa. Dois modelos
   lendo a mesma fonte em paralelo colidem no catálogo do DuckDB:

   ```
   TransactionContext Error: Catalog write-write conflict on create
   with "Schema bronze Table bronze faers_events"
   ```

A solução — concentrar a leitura em um modelo por tabela — resolve os dois de uma vez. É o
padrão que se usaria em produção de qualquer forma.

### 3.2 Deduplicação

Todos os modelos `stg_*` deduplicam com `ROW_NUMBER`, mesmo a bronze já garantindo unicidade:

```sql
row_number() over (
    partition by safetyreportid
    order by ingest_time desc, safetyreportversion desc, extraction_id desc
) as rn
```

Repare no terceiro critério de ordenação. Sem um desempate determinístico, duas linhas com o
mesmo `ingest_time` seriam escolhidas de forma arbitrária, e o modelo produziria resultados
diferentes entre execuções — o que quebraria a idempotência da publicação mais adiante.

### 3.3 A explosão do FAERS

Este é o coração do Dia 6. O `patient_payload` guarda dois arrays independentes:

```json
{
  "drug":     [ {"medicinalproduct": "XELJANZ XR", "drugcharacterization": "1", ...}, ... ],
  "reaction": [ {"reactionmeddrapt": "Death", "reactionoutcome": "5"}, ... ]
}
```

Eles viram dois modelos, cada um com o grão declarado:

| Modelo | Grão |
|---|---|
| `stg_faers` | um relato |
| `stg_faers_drugs` | (relato, posição no array `drug`) |
| `stg_faers_reactions` | (relato, posição no array `reaction`) |

A posição vem de `range()`, e não de `row_number()`, porque a ordem de saída de `UNNEST` não é
garantida por si só. A posição no array é o único identificador estável de um item dentro do
relato — o FAERS não numera medicamentos nem reações.

**Uma descoberta importante ao inspecionar os dados reais:** o mesmo medicamento aparece
repetido dentro do mesmo relato quando há registros de dosagem diferentes. Nesta base, são
**570 pares (relato, fármaco) repetidos** em 507 relatos. A silver preserva essa repetição
fielmente; a consolidação acontece na gold, onde o grão analítico é declarado.

### 3.4 O bloco `openfda`: uma ponte que não estava no plano

Inspecionando o payload real, cada medicamento pode trazer:

```json
"openfda": {
  "rxcui": ["1656643", "1656646", ...],
  "spl_set_id": ["1af01887-b69d-444b-91ed-ebfe12784440"],
  "generic_name": ["BASILIXIMAB"], ...
}
```

Duas consequências:

- **`spl_set_id` liga FAERS a DailyMed diretamente.** É o mesmo `setid` das bulas. Uma ponte
  por identificador, não por nome — muito mais confiável do que casar strings.
- **`rxcui` vem como uma lista longa**, com os RxCUI de *todas as apresentações* do produto
  (dose, forma, marca). **Não** é o ingrediente. Por isso ele é guardado como indício para
  auditoria, e o princípio ativo continua vindo do RxNorm.

### 3.5 Normalização RxNorm

O problema: `TACROLIMUS`, `Tacrolimus` e `PROGRAF` são o mesmo princípio ativo. Contados
separadamente, os números ficam errados.

A solução tem três peças:

**a) `farmaco_nomes`** reduz o universo a nomes distintos antes de qualquer chamada de rede.
Nesta base: 3.589 linhas de medicamento → **1.036 nomes distintos**. A API é chamada uma vez
por nome, não uma vez por linha.

**b) `rxnorm_mapping` é um modelo Python do dbt**, não um script separado. Assim a normalização
vira um **nó do grafo de dependências**: o dbt sabe que ela depende de `farmaco_nomes` e que
`dim_farmaco` depende dela. A ordem de execução deixa de ser responsabilidade de quem digita os
comandos, e a linhagem aparece inteira no `dbt docs`.

**c) A resolução tem três degraus**, em `src/pharma_pipeline/rxnorm.py`:

| Degrau | Endpoint | Resultado |
|---|---|---|
| busca normalizada | `/rxcui.json?search=2` | `exata` |
| busca aproximada | `/approximateTerm.json` | `aproximada` (score ≥ 50) |
| nada encontrado | — | `nao_mapeado`, com `rxcui` nulo |

**O terceiro degrau é uma decisão de projeto, não uma falha.** Um fármaco que o RxNorm não
conhece **continua no modelo**, marcado. Descartá-lo silenciaria justamente os produtos mais
irregulares — combinações, manipulados, importados — que são os que mais interessam à
farmacovigilância. Um evento adverso nunca desaparece por falta de vocabulário.

Dois freios operacionais:

- **cache em disco** (`.local/rxnorm/cache.json`), que guarda inclusive as buscas sem
  resultado — sem isso, todo nome desconhecido seria reconsultado em toda execução;
- **`RXNORM_MAX_LOOKUPS`**, teto de consultas novas por execução. Uma ingestão maior que o
  esperado não vira milhares de chamadas sem que alguém tenha decidido isso.

---

## 4. Dia 7 — Modelo dimensional na gold

### 4.1 O grão, e uma correção ao documento de fundação

O documento de fundação define o grão em palavras:

> *"Cada linha representa um evento adverso individual relatado à FDA, por um fármaco
> específico"*

…mas o exemplo de código usava `unique_key='report_id'`. **As duas coisas não podem ser
verdade ao mesmo tempo.** Um relato cita vários fármacos, então uma linha por relato não é uma
linha por fármaco.

Foi adotado o grão descrito em palavras, que é o mais fino:

> **Uma linha = um par fármaco–reação distinto dentro de um relato.**
> Chave lógica: `(safetyreportid, id_farmaco, id_reacao)` → `id_evento`.

Por que esse grão vale a pena: a pergunta central da farmacovigilância — *"quantos relatos
associam o fármaco X à reação Y?"* — vira um `COUNT` com dois filtros. Com uma linha por
relato, exigiria abrir um JSON em tempo de consulta.

### 4.2 O produto cartesiano é proposital — e precisa ser entendido

O FAERS lista medicamentos e reações como **duas listas independentes**. A fonte **não diz**
qual medicamento se liga a qual reação. Cruzar as duas listas dentro do relato é exatamente o
que a análise de desproporcionalidade (PRR, ROR) faz com notificação espontânea.

Consequência prática, medida nesta base:

```
507 relatos  →  18.998 linhas de fato   (fator ≈ 37×)
```

**Somar linhas não conta eventos clínicos.** A métrica correta é
`count(distinct safetyreportid)`. Por isso o fato carrega `qtd_medicamentos_relato` e
`qtd_reacoes_relato`: quem consulta enxerga o fator de multiplicação.

E, sempre: **nenhuma linha prova causalidade.** Um relato registra suspeita.

### 4.3 O esquema estrela

```
                        dim_data
                            │
   dim_farmaco ─────┐       │       ┌───── dim_reacao
                    ▼       ▼       ▼
                 fato_evento_adverso
                    ▲       ▲       ▲
   dim_fonte ───────┘       │       └───── dim_bula
                            │
                     (safetyreportid = dimensão degenerada)

   dim_farmaco ─────┐
   dim_fonte  ──────┼──────► fato_recall ◄────── dim_data
   dim_bula   ──────┘
```

Duas escolhas merecem explicação:

**DailyMed virou dimensão, não fato.** Ele descreve o *estado oficial* de um produto, não um
acontecimento. `dim_bula` guarda a versão corrente por `setid`; quando a bronze passar a
guardar histórico de versões, ela vira uma SCD tipo 2.

**RES virou fato.** Um recolhimento é uma ação, com data e classificação de risco. Sem
`fato_recall`, o modelo `stg_res` seria um beco sem saída na silver — um sinal claro de
problema de projeto.

### 4.4 `dim_reacao` sem código MedDRA

O documento de fundação previa `dim_reacao(codigo_meddra, ...)`. **Esse código não existe no
dado disponível:** o openFDA expõe o *texto* do termo (`reactionmeddrapt`) e a versão do
dicionário, mas não o número — o MedDRA é licenciado pelo ICH.

A identidade passou a ser uma chave substituta derivada do termo normalizado. Um campo
`codigo_meddra` sempre nulo daria a impressão de uma rastreabilidade que não temos.

Consequência honesta: **não há hierarquia MedDRA** (PT → HLT → SOC). Agrupar reações por órgão
ou sistema exigiria licença do dicionário.

### 4.5 O membro "Não informado"

Nem todo recall traz nome de substância. Sem uma linha para representar essa ausência, o fato
ficaria com chave estrangeira órfã — e o teste de integridade referencial falharia com razão.

A alternativa comum seria deixar a FK nula, mas isso obriga todo relatório a usar `LEFT JOIN` e
faz as linhas sumirem de qualquer `INNER JOIN`. Um **membro explícito** mantém a integridade
referencial e deixa "não informado" visível como categoria. Nesta base, **70 recalls** caem
nele.

### 4.6 Materialização incremental

Os dois fatos usam `incremental` com `delete+insert` por chave determinística:

```sql
{{ config(materialized='incremental', unique_key='id_evento',
          incremental_strategy='delete+insert') }}

{% if is_incremental() %}
where ingest_time >= (select coalesce(max(ingest_time), '1900-01-01') from {{ this }})
{% endif %}
```

O `>=` é proposital. Com `>`, uma carga interrompida no meio de um mesmo `ingest_time` perderia
as linhas restantes para sempre. Reler a borda é seguro porque a chave é determinística e o
MERGE é idempotente — **verificado**: rodar o fato duas vezes mantém 18.998 linhas e 18.998
chaves distintas.

---

## 5. Dia 8 — Testes de qualidade

### 5.1 Duas barreiras, com fronteiras diferentes

Testes do dbt e Great Expectations parecem redundantes. Não são:

| | dbt test | Great Expectations |
|---|---|---|
| **Onde roda** | dentro do DuckDB | na tabela Iceberg já gravada no MinIO |
| **Quando** | antes de publicar | depois de publicar |
| **Responde** | "o modelo está certo?" | "o que os consumidores enxergam está certo?" |
| **Se falha** | dado ruim não saiu do motor | dado ruim já está publicado |

A segunda barreira pega o que a primeira não vê: falha de conversão de tipo na escrita, UPSERT
em chave errada, publicação parcial por interrupção, ou uma tabela que ficou para trás porque o
`publish` daquela camada não foi executado. É a reconciliação pós-carga do Volume 6.

### 5.2 Testes genéricos próprios

O projeto escreve dois testes genéricos em vez de instalar `dbt_utils`, para não depender de
rede na instalação — e porque o mecanismo é curto o bastante para ser lido e entendido:

**`chave_composta_unica`** — o `unique` do dbt olha uma coluna por vez. Quando o grão é
composto, nenhuma coluna sozinha é única. Este é **o único teste que verifica de fato a frase
escrita no cabeçalho de cada modelo**.

```yaml
data_tests:
  - chave_composta_unica:
      arguments:
        colunas: [safetyreportid, id_farmaco, id_reacao]
```

**`data_plausivel`** — pega datas em 1900 (parsing errado) e no futuro (fuso trocado), com
folga de 2 dias porque as fontes publicam com precisão de dia.

### 5.3 Testes singulares — as regras do domínio

| Teste | O que protege |
|---|---|
| `assert_toda_linha_tem_procedencia` | Toda linha tem `fonte`, `event_time` e `ingest_time`. É o requisito *"toda resposta deve citar fonte e data"* virado código. |
| `assert_frescor_nao_negativo` | Latência negativa é fisicamente impossível e denuncia fuso trocado. Protege diretamente a métrica da Fase 4. |
| `assert_fato_reconcilia_com_silver` | Todo relato elegível chegou ao fato. Pega perda silenciosa de linhas em `JOIN`. |

### 5.4 As expectativas não passam por vazio

Uma suíte de qualidade que só foi vista passar não prova nada — ela poderia estar aprovando
tudo. Por isso `tests/test_quality.py` exercita os **dois lados**: fonte nula, chave duplicada,
data de 1900, latência negativa e fonte fora do domínio são todas verificadas como
**reprovadas**.

---

## 6. Como executar

### Pré-requisitos

MinIO no ar e camada bronze populada (Fase 2):

```powershell
.\scripts\day1\Start-MinIO.ps1
.\scripts\day5\Run-Batch-Lab.ps1     # se a bronze ainda estiver vazia
```

### Preparar o ambiente

```powershell
.\scripts\day6\Setup-Phase3.ps1
```

### Caminho por dia

```powershell
.\scripts\day6\Build-Silver.ps1      # silver + publicação
.\scripts\day7\Build-Gold.ps1        # gold + publicação
.\scripts\day8\Test-Quality.ps1      # as duas barreiras
```

### Tudo de uma vez, com prova de idempotência

```powershell
.\scripts\day8\Run-Phase3-Lab.ps1
```

### Comandos diretos

```powershell
.\.venv\Scripts\Activate.ps1

pharma-pipeline transform build                    # modelos + testes
pharma-pipeline transform build --select gold      # só a gold
pharma-pipeline transform build --full-refresh     # recria os incrementais
pharma-pipeline transform test                     # só os testes

pharma-pipeline publish silver
pharma-pipeline publish gold
pharma-pipeline publish gold.fato_evento_adverso --recreate

pharma-pipeline expectations
```

> **`--recreate` quando o schema mudar.** Se você adicionar uma coluna a um modelo, a tabela
> Iceberg existente tem outro schema e o commit falha. `--recreate` descarta e recria —
> perdendo o histórico de snapshots daquela tabela, o que é aceitável em desenvolvimento.

### Sem internet

```powershell
$env:RXNORM_OFFLINE = "1"
pharma-pipeline transform build
```

O cache já resolvido continua valendo; nomes novos ficam como `nao_mapeado`.

---

## 7. Como consultar o resultado

### Pela CLI

```powershell
pharma-pipeline tables          # todas as tabelas, com chave e grão

pharma-pipeline query gold.fato_evento_adverso `
  --columns safetyreportid,id_farmaco,id_reacao,gravidade,latencia_atualizacao_horas `
  --limit 5

pharma-pipeline snapshots gold.dim_bula

pharma-pipeline query gold.dim_bula --snapshot-id 4613336051689854230 --limit 3
pharma-pipeline query gold.dim_bula --as-of "2026-07-28T19:30:00Z" --limit 3
```

### Perguntas de negócio, em SQL

Abra o motor diretamente:

```powershell
.\.venv\Scripts\python.exe -c "import duckdb; duckdb.connect('.local/duckdb/pharma.duckdb', read_only=True).sql('...').show()"
```

**Quais fármacos concentram mais relatos graves?**

```sql
select f.nome_farmaco,
       count(distinct e.safetyreportid) as relatos,
       count(*)                         as pares_farmaco_reacao
from gold.fato_evento_adverso e
join gold.dim_farmaco f using (id_farmaco)
where e.suspeito_primario
  and e.gravidade
  and f.identidade_confiavel
group by 1
order by relatos desc
limit 10;
```

Note os três filtros: `suspeito_primario` descarta medicamentos concomitantes;
`identidade_confiavel` restringe a fármacos resolvidos no nível de ingrediente; e a contagem é
`distinct` de relatos, não de linhas.

**O RxNorm realmente unificou nomes?**

```sql
select
    rxnorm_nome,
    count(*)                            as nomes_reportados,
    list_sort(list(nome_normalizado))   as quais
from gold.dim_farmaco
where identidade_confiavel
group by id_ingrediente, rxnorm_nome
having count(*) > 1
order by nomes_reportados desc;
```

> Desde a Fase 4, a unificação por princípio ativo é um `group by id_ingrediente`, e não mais
> uma colapsagem embutida na chave — ver
> [contratos, `gold.dim_farmaco`](contratos-dados-fase-3.md#golddim_farmaco).

**Frescor observado por fonte** (prévia do Dia 10):

```sql
select fonte,
       round(avg(latencia_atualizacao_horas) / 24) as dias_medios,
       max(latencia_atualizacao_horas) / 24        as dias_maximo
from gold.fato_evento_adverso
group by fonte;
```

### Linhagem visual

```powershell
pharma-pipeline transform docs
.\.venv\Scripts\dbt.exe docs serve --project-dir transform --profiles-dir transform
```

---

## 8. O que os testes encontraram de verdade

Esta seção existe porque o valor de um teste só fica claro quando ele pega algo. Os três casos
abaixo são bugs reais desta implementação, encontrados na primeira execução completa:

**1. Chave duplicada no fato (49 casos).**
`unique_fato_evento_adverso_id_evento` falhou. Causa: o fato agrupava medicamentos por **nome**,
enquanto `dim_farmaco` agrupava por **identidade RxNorm**. Dois nomes distintos que resolvem
para o mesmo ingrediente viravam uma linha na dimensão e duas no fato.
*Correção:* resolver o RxCUI **antes** de agrupar.

**2. Integridade referencial quebrada (70 casos).**
`relationships_fato_recall_id_farmaco` falhou: recalls sem nome de substância apontavam para uma
chave inexistente. *Correção:* o membro "Não informado".

**3. A causa raiz comum.**
A expressão da identidade do fármaco estava **repetida em quatro modelos**. Foi centralizada na
macro `chave_identidade_farmaco`, que agora é a única definição da regra.

Sem os testes, os três defeitos teriam produzido números plausíveis e errados.

> **Epílogo, escrito na Fase 4.** A correção do item 1 — "resolver o RxCUI antes de agrupar" —
> resolvia a duplicação, mas deixava um problema mais profundo de pé: a chave passava a
> depender do RxCUI, que **muda entre execuções** conforme o cache do RxNorm cresce. Com o
> fato incremental, isso produziu 6.066 linhas órfãs assim que o volume real chegou.
>
> A correção definitiva foi tirar o enriquecimento da chave e deixá-lo como atributo. A lição
> que fica: *uma chave substituta só pode depender de dados que a linha de fato já carrega e
> que não mudam.* Ver [Fase 4, seção 8](fase-4.md#8-o-que-quebrou-de-verdade).

---

## 9. Resultados observados

Estado do lakehouse após a execução completa:

| Tabela | Linhas | Snapshots |
|---|---:|---:|
| `bronze.dailymed_spls` | 400 | 4 |
| `bronze.faers_events` | 507 | 3 |
| `bronze.res_recalls` | 199 | 2 |
| `silver.stg_dailymed` | 400 | 2 |
| `silver.stg_faers` | 507 | 1 |
| `silver.stg_faers_drugs` | 3.589 | 1 |
| `silver.stg_faers_reactions` | 2.973 | 1 |
| `silver.stg_res` | 199 | 1 |
| `silver.farmaco_nomes` | 1.036 | 4 |
| `silver.rxnorm_mapping` | 1.036 | 2 |
| `gold.dim_farmaco` | 1.032 | 2 |
| `gold.dim_reacao` | 833 | 1 |
| `gold.dim_data` | 18.993 | 1 |
| `gold.dim_fonte` | 3 | 3 |
| `gold.dim_bula` | 400 | 2 |
| `gold.fato_evento_adverso` | 18.998 | 1 |
| `gold.fato_recall` | 199 | 1 |

**Qualidade da normalização RxNorm** (1.036 nomes):

| Correspondência | Nomes |
|---|---:|
| exata | 356 |
| aproximada | 1 |
| não mapeado | 510 antes de completar o cache |

Dos resolvidos, **578 identidades são de nível ingrediente** (`identidade_confiavel`). Os não
mapeados são majoritariamente combinações com separador `\`, textos de forma farmacêutica e
produtos de higiene — categorias que o RxNorm legitimamente não indexa como ingrediente.

**O que foi comprovado por execução:**

- 142 nós do dbt (17 modelos + 1 seed + 124 testes) — todos passaram;
- 37 testes unitários Python, `ruff check` e `ruff format` limpos;
- 14 scripts PowerShell com sintaxe válida;
- **determinismo:** reexecutar o dbt sem mudança de dado produz resultado idêntico;
- **idempotência:** republicar as 14 tabelas sem mudança devolve `unchanged: true` em todas,
  sem criar snapshot;
- **propagação seletiva:** ao ingerir 200 bulas novas, apenas `stg_dailymed`, `dim_bula`,
  `dim_farmaco` e `dim_fonte` mudaram; os fatos de FAERS e RES permaneceram intocados;
- **time travel na gold:** `dim_bula` tem 2 snapshots — 400 linhas no atual, 200 no anterior;
- **incremental sem duplicata:** rodar o fato de novo mantém 18.998 linhas / 18.998 chaves.

---

## 10. Solução de problemas

**`Catalog write-write conflict`**
Dois nós leram a mesma `source` Iceberg em paralelo. Referencie o modelo `src_*` correspondente
em vez da `source` direta.

**`Column 'event_time' has an unsupported type: timestamp[us, tz=America/Sao_Paulo]`**
O DuckDB exporta timestamps no fuso da sessão; o Iceberg exige UTC. Já tratado por
`normalize_arrow`; se reaparecer em um caminho novo, passe o Arrow por essa função.

**Publicação falha com incompatibilidade de schema**
O modelo ganhou ou perdeu colunas. Use `pharma-pipeline publish <tabela> --recreate`.

**`Banco DuckDB nao encontrado`**
Rode `pharma-pipeline transform` antes de `publish`.

**A ingestão retorna 0 linhas mesmo com dados na janela**
O `dlt` restaura o watermark a partir do **destino**, não apenas do diretório local. Apagar
`.local/dlt/<pipeline>` não zera o estado. Use um `--pipeline-suffix` novo.

**O RxNorm demora muito na primeira execução**
Esperado: são ~1.000 nomes distintos, com intervalo entre chamadas. O cache torna as execuções
seguintes quase instantâneas. Para pular: `--exclude rxnorm_mapping` ou `RXNORM_OFFLINE=1`.

**PowerShell acusa `NativeCommandError` sem erro real**
Com `$ErrorActionPreference = "Stop"`, qualquer linha de stderr de um executável vira registro
de erro. Os logs informativos do Great Expectations já são silenciados por `quality.py`.

---

## 11. Limites conscientes desta fase

- **DailyMed continua sendo só o índice.** `dim_bula` tem título, versão e data, mas não
  indicações, contraindicações nem boxed warnings — eles vivem no XML completo, ainda não
  ingerido. `laboratorio` e `produto_nome` são extraídos do título por regex, best-effort.
- **A ponte FAERS → DailyMed tem baixa cobertura nesta amostra**: 527 `spl_set_id` distintos no
  FAERS contra 400 bulas ingeridas, resultando em 18 eventos ligados. É consequência do tamanho
  da amostra de bulas, não do modelo.
- **Sem hierarquia MedDRA**, pelo motivo de licenciamento já explicado.
- **A silver é reconstruída inteira a cada execução** (materialização `table`). Correto e
  simples neste volume; em volume real, `stg_faers_drugs` também deveria ser incremental.
- **`metricas_frescor` por fonte é da Fase 4.** Aqui o frescor existe em nível de linha
  (`latencia_ingestao_horas`, `latencia_atualizacao_horas`); a agregação por fonte, com limiar
  e alerta, é o Dia 10.
- **Catálogo SQLite e credenciais root do MinIO** continuam sendo limitações herdadas da Fase 2,
  registradas em [decisoes-arquitetura-fase-2.md](decisoes-arquitetura-fase-2.md).

---

## Próximo passo

[Fase 4 — Orquestração (Dias 9–10)](foundation.md#fase-4--orquestração-dias-9-10): o Airflow
passa a coordenar ingestão → transformação → testes, e o staleness gap vira métrica formal com
alerta.

> Os dados são públicos e não provam causalidade clínica. Sinais em FAERS devem ser descritos
> como associações que exigem investigação, nunca como prova de que um medicamento causou um
> evento.
