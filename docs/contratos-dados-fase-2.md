# Contratos de dados da Fase 2

Um contrato de dados registra o que uma linha significa, qual chave a identifica, qual data move o watermark e quais campos são obrigatórios. Ele evita que duas pessoas usem a mesma tabela com interpretações diferentes.

## Envelope comum da bronze

Todas as fontes carregam estes campos técnicos:

| Campo | Tipo | Significado |
|---|---|---|
| `event_time` | timestamp UTC | Relógio operacional escolhido na fonte: publicação (DailyMed), recebimento pela FDA (FAERS) ou relatório (RES). Como as APIs fornecem apenas o dia, usamos meia-noite UTC. |
| `ingest_time` | timestamp UTC | Instante em que nosso extrator recebeu o registro. Usa `datetime.now(UTC)`, não um horário local sem fuso. |
| `fonte` | string | `dailymed`, `faers` ou `res`. |
| `source_url` | string | URL rastreável da origem. |
| `extraction_id` | UUID string | Identifica a tentativa de extração que produziu a linha. |
| `raw_payload` | JSON serializado | Registro original completo, preservado para auditoria e reprocessamento. |

`event_time` e `ingest_time` não são sinônimos. A diferença entre eles será usada como medida de frescor. Para DailyMed, por exemplo, uma bula publicada em 24/07 e capturada em 27/07 tem aproximadamente três dias de atraso observado.

Aqui `event_time` não significa necessariamente quando um efeito clínico começou no paciente. No FAERS usamos `receivedate`, pois é o relógio disponível e consistente para medir quando a informação entrou na FDA. Dar nomes precisos aos relógios evita transformar “frescor de publicação” em “tempo até ocorrência clínica”.

## `bronze.dailymed_spls`

**Grão:** uma linha é o estado mais recente conhecido de um conjunto de bula SPL identificado por `setid`.

| Campo | Papel |
|---|---|
| `setid` | Chave primária estável do conjunto de versões da bula. |
| `spl_version` | Número da versão dentro do conjunto. Pode aumentar quando a bula é revisada. |
| `title` | Título oficial apresentado pelo DailyMed. |
| `published_date` | Dia em que essa versão foi publicada no DailyMed; watermark da fonte. |
| `event_time` | `published_date` convertida para timestamp UTC. |

Decisão importante: a chave real é `setid`, sem sublinhado. `set_id` no exemplo do documento de fundação era pseudocódigo. O índice `/spls.json` não contém o texto integral da bula. O XML completo continua acessível por `setid` e deverá ser ingerido em uma evolução posterior.

Com `UPSERT` por `setid`, uma nova versão atualiza o estado atual da tabela Iceberg. O Parquet bronze, porém, continua imutável e preserva as versões capturadas. No futuro, uma tabela histórica específica pode usar chave composta `(setid, spl_version)`.

## `bronze.faers_events`

**Grão:** uma linha é a versão mais recente exposta pelo openFDA de um relato de segurança identificado por `safetyreportid`.

| Campo | Papel |
|---|---|
| `safetyreportid` | Chave do relato; chave do UPSERT. |
| `safetyreportversion` | Versão reportada à FDA. |
| `receivedate` | Primeira data em que a FDA recebeu o relato; relógio usado em `event_time`. |
| `receiptdate` | Data em que a FDA recebeu a informação mais recente da versão exposta; watermark. |
| `serious` | Indicador técnico derivado do valor `"1"` da API. |
| `occurcountry` | País informado, quando existe. |
| `patient_payload` | Objeto `patient` completo serializado; contém arrays de drogas e reações. |

Um relato FAERS pode ter várias drogas e várias reações. Portanto, esta tabela não tem o grão “uma droga causou uma reação”. Essa explosão só deve ocorrer na silver com identificadores e regras explícitas. Relato também não equivale a causalidade: duplicidade, ausência de denominador de exposição e qualidade variável fazem parte das limitações da fonte.

## `bronze.res_recalls`

**Grão:** uma linha é um produto/registro de recall identificado pelo `recall_number` do Recall Enterprise System exposto no endpoint `drug/enforcement`.

| Campo | Papel |
|---|---|
| `recall_number` | Chave de rastreamento da FDA; chave do UPSERT. |
| `event_id` | Agrupa itens relacionados ao mesmo evento de recall. Não é necessariamente único por linha. |
| `report_date` | Data do relatório de enforcement; watermark. |
| `recall_initiation_date` | Quando a empresa/FDA iniciou a ação. |
| `classification` | Classe de risco I, II ou III, quando classificada. |
| `status` | Situação da ação, como `Ongoing` ou `Terminated`. |
| `recalling_firm` | Empresa responsável pelo recolhimento. |
| `product_description` | Descrição textual do produto/lotes. |
| `reason_for_recall` | Motivo publicado para o recall. |
| `openfda_payload` | Identificadores harmonizados do openFDA, como NDC, RxCUI e `spl_set_id`, quando disponíveis. |

O RES complementa o FAERS: FAERS descreve relatos de experiências adversas; RES descreve uma ação regulatória de recolhimento. Um recall pode acontecer por contaminação, esterilidade, rotulagem ou desvio de fabricação, não apenas por um sinal clínico.

## Chaves, cursor e atualização

| Fonte | Chave de UPSERT | Cursor/watermark | Cadência esperada |
|---|---|---|---|
| DailyMed | `setid` | `published_date` | diária |
| FAERS | `safetyreportid` | `receiptdate` | conforme atualização do endpoint openFDA |
| RES | `recall_number` | `report_date` | semanal/segundo publicação FDA |

O cursor reduz o universo consultado; a chave garante idempotência. Um sem o outro é insuficiente:

- só cursor: reprocessamentos na borda do dia podem duplicar linhas;
- só chave: evita duplicatas na tabela final, mas baixa todo o histórico em cada execução;
- cursor + chave: reconsulta uma borda segura e faz UPSERT determinístico.

Para FAERS, `receiptdate` é uma decisão de correção, não preferência estética. `receivedate` permanece a data da primeira versão; se uma informação nova atualizar um relato antigo, filtrar somente pelo primeiro recebimento pode nunca reler essa chave. `receiptdate` avança com a informação mais recente, e o UPSERT substitui o estado de `safetyreportid`.

No RES, `report_date` pode permanecer antigo enquanto status ou detalhes evoluem. A extração reaplica uma janela móvel de 90 dias (`RES_LOOKBACK_DAYS`). Releituras com `raw_payload` idêntico não alteram `ingest_time` da primeira captura nem criam snapshot; payload alterado faz UPSERT.

## Nulabilidade e payload original

Campos opcionais não recebem valores inventados. `None`/nulo é diferente de `false`, zero ou texto vazio. O `raw_payload` permite revisar a decisão e promover novos campos sem chamar a API histórica novamente.

Na bronze promovemos somente campos necessários para identidade, frescor e inspeção. Transformações de negócio — normalização por RxNorm, explosão de drogas/reações, classificação analítica — pertencem à silver.
