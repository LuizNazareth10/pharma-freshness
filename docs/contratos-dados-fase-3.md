# Contratos de dados da Fase 3 — silver e gold

Um contrato de dados registra **o que uma linha significa**, qual chave a identifica e quais
campos são obrigatórios. Sem ele, duas pessoas usam a mesma tabela com interpretações
diferentes e chegam a números diferentes — ambos "corretos".

Este documento cobre as camadas derivadas. Os contratos da bronze estão em
[contratos-dados-fase-2.md](contratos-dados-fase-2.md).

---

## Regra geral: o grão vem antes das colunas

Cada modelo deste projeto começa com uma frase declarando seu grão, e essa frase é verificável:
o teste `chave_composta_unica` a transforma em código. Um modelo cujo grão não pode ser
escrito em uma frase é um modelo que ainda não foi entendido.

O grão também está registrado em código, no `LakeTable` de `src/pharma_pipeline/contracts.py`,
e viaja junto com os dados: ele é gravado nas propriedades do snapshot Iceberg a cada
publicação.

```powershell
pharma-pipeline tables      # lista chave e grão de todas as tabelas
```

---

## Envelope de procedência

Toda tabela derivada carrega os campos herdados da bronze:

| Campo | Significado |
|---|---|
| `fonte` | `dailymed`, `faers` ou `res` |
| `event_time` | Relógio da fonte, em UTC |
| `ingest_time` | Instante em que o pipeline capturou o registro, em UTC |
| `source_url` | URL rastreável da origem |
| `extraction_id` | Tentativa de extração que produziu a linha |

Este envelope é o requisito de domínio *"toda resposta deve citar fonte e data"* materializado.
Ele é verificado em dois lugares: no teste singular `assert_toda_linha_tem_procedencia`
(antes de publicar) e nas expectativas do Great Expectations (depois de publicar).

**Nenhum campo `publish_time` é gravado nas linhas.** O instante da publicação vive nas
propriedades do snapshot Iceberg, que é o lugar próprio para metadado de commit. Se ele fosse
uma coluna, cada republicação alteraria todas as linhas e a idempotência seria impossível de
provar.

---

## Camada silver

### `silver.stg_dailymed`

**Grão:** uma linha por `setid`, no estado mais recente conhecido.

| Campo | Papel |
|---|---|
| `setid` | Chave estável do conjunto de versões da bula |
| `spl_version` | Versão corrente |
| `titulo_original` | Título como o DailyMed publica |
| `produto_nome` | Nome extraído do título — **best-effort** |
| `laboratorio` | Texto entre colchetes no fim do título — **best-effort** |
| `published_date` | Dia de publicação desta versão |

> `produto_nome` e `laboratorio` vêm de expressão regular sobre o padrão
> `"PRODUTO (INGREDIENTES) FORMA [LABORATÓRIO]"`. O texto canônico vive no XML completo da
> bula, que esta fase ainda não ingere. Trate os dois como aproximação, não como cadastro.

### `silver.stg_faers`

**Grão:** uma linha por `safetyreportid`.

| Campo | Papel |
|---|---|
| `safetyreportid` | Chave do relato |
| `receivedate` | Primeiro recebimento pela FDA; base do `event_time` |
| `receiptdate` | Recebimento da informação mais recente; watermark da fonte |
| `grave` | Indicador de gravidade do relato |
| `paciente_sexo` / `paciente_idade` | Demografia, quando informada |
| `qtd_medicamentos` / `qtd_reacoes` | Tamanho dos arrays originais |

**Este modelo não contém medicamentos nem reações.** Um relato cita vários de cada, e a fonte
não diz qual se liga a qual. Misturar os dois arrays aqui produziria um grão ambíguo.

`qtd_medicamentos` e `qtd_reacoes` existem para tornar o fator de multiplicação do fato
visível já na silver.

### `silver.stg_faers_drugs`

**Grão:** uma linha por (`safetyreportid`, `drug_seq`) — a posição original no array `drug`.

| Campo | Papel |
|---|---|
| `drug_seq` | Posição no array, a partir de 1 |
| `produto_relatado` | `medicinalproduct`, como o notificador escreveu |
| `substancia_ativa` | `activesubstance.activesubstancename` |
| `caracterizacao_codigo` | 1 suspeito, 2 concomitante, 3 interagente |
| `suspeito_primario` | `caracterizacao_codigo = 1` |
| `openfda_spl_set_id` | **Ponte direta para o `setid` do DailyMed** |
| `openfda_rxcui` | Lista de RxCUI de *apresentações* — não é o ingrediente |
| `nome_normalizado` | Forma canônica levada ao RxNorm |

Dois pontos que mudam como a tabela deve ser usada:

- **`drug_seq` é posicional, não semântico.** O FAERS não numera medicamentos. A posição é o
  único identificador estável de um item dentro do relato.
- **A repetição é real e preservada.** O mesmo medicamento aparece mais de uma vez no mesmo
  relato quando há registros de dosagem diferentes — 570 pares nesta base. Contar linhas aqui
  **não** conta medicamentos distintos.

`nome_normalizado` prefere, nesta ordem: substância ativa → nome genérico do openFDA → produto
relatado. A substância vem primeiro porque o RxNorm normaliza para ingrediente.

### `silver.stg_faers_reactions`

**Grão:** uma linha por (`safetyreportid`, `reaction_seq`).

| Campo | Papel |
|---|---|
| `reaction_seq` | Posição no array `reaction` |
| `reacao_termo_original` | Termo preferencial MedDRA, como veio |
| `reacao_normalizada` | Termo canônico; identidade da reação |
| `meddra_versao` | Versão do dicionário informada |
| `desfecho_codigo` | 1 a 6; **5 = fatal** |
| `desfecho_fatal` | `desfecho_codigo = 5` |

**Sem código numérico MedDRA** — o openFDA expõe apenas o texto. Ver a seção de `dim_reacao`.

### `silver.stg_res`

**Grão:** uma linha por `recall_number`.

| Campo | Papel |
|---|---|
| `recall_number` | Chave da ação de recolhimento |
| `event_id` | Agrupa recalls relacionados — **não é único por linha** |
| `classificacao_nivel` | 1 = Class I (mais grave), 2, 3 |
| `situacao` | `Ongoing`, `Terminated`, etc. |
| `openfda_*` | Identificadores harmonizados pela FDA |
| `nome_normalizado` | Forma canônica levada ao RxNorm; **pode ser nulo** |

> **Armadilha documentada:** usar `event_id` como chave perderia registros. Vários produtos
> podem pertencer a uma mesma ação de recall.

### `silver.farmaco_nomes`

**Grão:** uma linha por `nome_normalizado` distinto observado em qualquer fonte.

| Campo | Papel |
|---|---|
| `nome_normalizado` | Nome canônico a resolver |
| `ocorrencias` | Quantas linhas de origem usaram este nome |
| `fontes` | Lista das fontes onde ele aparece |
| `rxcui_openfda_sugerido` | Indício para auditoria, **não** identidade |

Este modelo existe para separar *descobrir o que normalizar* (SQL, barato) de *consultar a API*
(rede, lento). Sem ele, a normalização seria chamada uma vez por linha de fato em vez de uma
vez por nome distinto.

### `silver.rxnorm_mapping`

**Grão:** uma linha por `nome_normalizado` distinto.

| Campo | Papel |
|---|---|
| `rxcui` | Identificador RxNorm; **nulo quando não mapeado** |
| `rxnorm_nome` | Nome canônico segundo o RxNorm |
| `rxnorm_tty` | Tipo de termo: `IN`, `MIN`, `PIN`, `SCD`, `BN`… |
| `tipo_correspondencia` | `exata`, `aproximada` ou `nao_mapeado` |
| `score` | Pontuação, quando a correspondência foi aproximada |
| `nivel_ingrediente` | `rxnorm_tty` ∈ {IN, MIN, PIN} |
| `consultado_em` | Quando o RxNav foi consultado; **nulo** se ainda não foi |

**Contrato explícito: nomes sem correspondência permanecem na tabela.** Um fármaco que o
vocabulário não conhece não pode desaparecer do modelo.

`consultado_em` nulo significa "ainda não consultado" (modo offline ou teto atingido), e não
"consultado sem resultado" — esse segundo caso tem `tipo_correspondencia = 'nao_mapeado'` com
data preenchida.

---

## Camada gold

### `gold.dim_farmaco`

**Grão:** uma identidade de fármaco.

A regra de identidade é a decisão central do modelo, definida na macro
`chave_identidade_farmaco`:

| Situação | Identidade |
|---|---|
| RxCUI resolvido | `rxcui:<código>` — nomes diferentes colapsam em uma linha |
| Sem RxCUI, com nome | `nome:<nome normalizado>` — permanece, marcado |
| Sem nome algum | `nao_informado` — o membro explícito |

| Campo | Papel |
|---|---|
| `id_farmaco` | Chave substituta determinística |
| `nome_farmaco` | Nome de exibição |
| `mapeado_rxnorm` | Houve RxCUI? |
| `identidade_confiavel` | Houve RxCUI **e** ele é de nível ingrediente |
| `nomes_originais` | Nomes de origem que colapsaram nesta identidade |
| `qtd_nomes_originais` | Quantos foram |

> **Ao consultar:** para comparar volumes entre fármacos, filtre por `identidade_confiavel`.
> Um RxCUI de apresentação (SCD/SBD/BN) identifica um produto, não o princípio ativo, e
> misturá-los produz contagens incomparáveis.

`nomes_originais` existe para tornar a normalização auditável: dá para explicar por que dois
nomes viraram a mesma linha.

### `gold.dim_reacao`

**Grão:** um termo preferencial MedDRA distinto, normalizado.

| Campo | Papel |
|---|---|
| `id_reacao` | Chave substituta derivada do termo normalizado |
| `reacao_normalizada` | Termo canônico; identidade |
| `reacao` | Grafia mais frequente, para exibição |
| `meddra_versao` | Versão do dicionário observada |

**Não existe `codigo_meddra`.** O openFDA expõe o texto do termo, não o número — o MedDRA é
licenciado pelo ICH. Um campo sempre nulo daria a impressão de uma rastreabilidade inexistente.

Consequência: **sem hierarquia MedDRA** (PT → HLT → SOC). Agrupar reações por sistema
orgânico exigiria licença.

### `gold.dim_data`

**Grão:** um dia. Chave `id_data` no formato `YYYYMMDD`.

Gerada, e não derivada dos fatos: assim dias sem evento existem como zero em vez de sumirem do
relatório.

### `gold.dim_fonte`

**Grão:** um sistema de origem.

Atributos vêm do seed `seeds/fonte_referencia.csv` porque são conhecimento do projeto, não
dado observado — nenhuma API informa a própria cadência esperada.

| Campo | Papel |
|---|---|
| `cadencia_esperada` | Diária ou semanal — **base do alerta de frescor da Fase 4** |
| `ultimo_ingest_time` | Última captura observada daquela fonte |

Sem uma expectativa declarada, não há como dizer que uma fonte está atrasada; só dá para dizer
quando ela chegou.

### `gold.dim_bula`

**Grão:** uma linha por `setid`, na versão corrente.

| Campo | Papel |
|---|---|
| `id_bula` | Chave substituta |
| `setid` | Chave natural do DailyMed |
| `spl_version` / `published_date` | Estado corrente da bula |
| `id_farmaco` | Ligação ao vocabulário RxNorm; **nulo** quando o produto não foi resolvido |

Aqui `id_farmaco` nulo é informação útil (o parsing do título falhou), e não uma categoria de
negócio — por isso esta dimensão **não** aponta para o membro "Não informado".

Vira SCD tipo 2, com `valido_de`/`valido_ate`, quando a bronze passar a guardar histórico de
versões.

### `gold.fato_evento_adverso`

> **GRÃO: um par fármaco–reação distinto dentro de um relato enviado à FDA.**
> Chave lógica: `(safetyreportid, id_farmaco, id_reacao)` → `id_evento`.

| Grupo | Campos |
|---|---|
| Chaves estrangeiras | `id_farmaco`, `id_reacao`, `id_data_recebimento`, `id_fonte`, `id_bula` |
| Dimensão degenerada | `safetyreportid`, `safetyreportversion` |
| Atributos do par | `caracterizacao_codigo`, `suspeito_primario`, `desfecho_codigo`, `desfecho_fatal`, `gravidade` |
| Contexto | `pais_ocorrencia`, `paciente_sexo`, `paciente_idade`, `qtd_entradas_medicamento`, `qtd_medicamentos_relato`, `qtd_reacoes_relato` |
| Frescor | `event_time`, `ingest_time`, `latencia_ingestao_horas`, `latencia_atualizacao_horas` |

#### Como contar corretamente

O FAERS **não liga** um medicamento específico a uma reação específica. As linhas são o produto
cartesiano das duas listas dentro do relato — a unidade usada em análise de
desproporcionalidade (PRR, ROR).

```
507 relatos  →  18.998 linhas   (fator ≈ 37×)
```

| Pergunta | Métrica correta |
|---|---|
| Quantos relatos envolvem o fármaco X? | `count(distinct safetyreportid)` |
| Quantos pares fármaco–reação? | `count(*)` |
| Quantos eventos clínicos? | **A fonte não responde isso.** |

`qtd_entradas_medicamento` registra quantas entradas originais do array foram consolidadas
naquela identidade de fármaco dentro do relato.

Quando o mesmo medicamento aparece repetido com caracterizações diferentes, vence o **menor**
código: se foi suspeito em alguma entrada, o par é tratado como suspeito.

#### As duas latências

Esta distinção importa mais do que parece:

| Campo | Parte de | Responde |
|---|---|---|
| `latencia_ingestao_horas` | `receivedate` (primeiro recebimento) | Qual a **idade do caso** quando o capturamos |
| `latencia_atualizacao_horas` | `receiptdate` (informação mais recente) | Quanto tempo levamos para capturar a **última novidade** |

Um relato antigo revisado hoje produz `latencia_ingestao_horas` de milhares de horas — nesta
base, média de ~943 dias. **Isso é característica da fonte, não atraso do pipeline.** A métrica
honesta de staleness gap para a Fase 4 é `latencia_atualizacao_horas`.

Reportar apenas a primeira daria um número verdadeiro sobre a pergunta errada.

#### O que este fato não afirma

**Nenhuma linha prova causalidade.** Um relato registra suspeita. Não há denominador de
exposição, a notificação é espontânea e a qualidade dos relatos varia. Toda leitura deve usar
linguagem de *sinal potencial* e *associação observada*.

### `gold.fato_recall`

> **GRÃO: uma ação de recolhimento, identificada por `recall_number`.**

| Grupo | Campos |
|---|---|
| Chaves estrangeiras | `id_farmaco`, `id_data_relatorio`, `id_fonte`, `id_bula` |
| Dimensão degenerada | `recall_number`, `event_id` |
| Atributos | `classificacao`, `classificacao_nivel`, `situacao`, `empresa`, `produto_descricao`, `motivo` |
| Frescor | `report_date`, `recall_initiation_date`, `dias_ate_relatorio`, `latencia_ingestao_horas` |

`event_id` é atributo, nunca chave. Recalls sem nome de substância apontam para o membro
"Não informado" de `dim_farmaco` — 70 linhas nesta base.

Um recall pode ocorrer por contaminação, esterilidade, rotulagem ou desvio de fabricação, e não
apenas por sinal clínico. Cruzá-lo com o FAERS mostra **ação regulatória** ao lado de **relato
de experiência**; são coisas distintas.

---

## Chaves substitutas

Todas usam `md5` sobre as colunas do grão, com separador `|` e marcação explícita de nulo,
através da macro `chave_hash`.

Duas propriedades importam:

- **Determinismo.** A mesma linha lógica gera a mesma chave em toda execução. Sem isso, o MERGE
  incremental criaria duplicatas em vez de atualizar.
- **Ausência de colisão por concatenação.** O separador impede que `('AB','C')` e `('A','BC')`
  produzam a mesma chave.

---

## Resumo de chaves

| Tabela | Chave de UPSERT | Grão |
|---|---|---|
| `silver.stg_dailymed` | `setid` | uma bula |
| `silver.stg_faers` | `safetyreportid` | um relato |
| `silver.stg_faers_drugs` | `safetyreportid`, `drug_seq` | um medicamento no relato |
| `silver.stg_faers_reactions` | `safetyreportid`, `reaction_seq` | uma reação no relato |
| `silver.stg_res` | `recall_number` | um recall |
| `silver.farmaco_nomes` | `nome_normalizado` | um nome distinto |
| `silver.rxnorm_mapping` | `nome_normalizado` | um nome distinto |
| `gold.dim_farmaco` | `id_farmaco` | uma identidade de fármaco |
| `gold.dim_reacao` | `id_reacao` | um termo MedDRA |
| `gold.dim_data` | `id_data` | um dia |
| `gold.dim_fonte` | `id_fonte` | uma fonte |
| `gold.dim_bula` | `id_bula` | uma bula |
| `gold.fato_evento_adverso` | `id_evento` | um par fármaco–reação num relato |
| `gold.fato_recall` | `id_recall` | uma ação de recolhimento |
