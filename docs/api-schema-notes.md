# Notas de schema — DailyMed e FAERS

Estas notas descrevem o que foi **observado** em amostras das APIs em 27/07/2026. Uma amostra ajuda a formular hipóteses, mas não prova um contrato: campos opcionais podem aparecer ou desaparecer em outras respostas.

## DailyMed — lista de SPLs

Endpoint: `GET https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json`

| Caminho | Tipo observado | Pode faltar/ser nulo? | Interpretação |
|---|---|---:|---|
| `data` | array | não observado | Página de bulas SPL |
| `data[].setid` | string UUID | não observado | Identificador estável do conjunto da bula |
| `data[].spl_version` | integer | não observado | Versão do SPL |
| `data[].title` | string | não observado | Título completo da bula |
| `data[].published_date` | string | não observado | Data textual, por exemplo `Jul 24, 2026` |
| `metadata.total_elements` | integer | não observado | Total de registros |
| `metadata.elements_per_page` | integer | não observado | Tamanho efetivo da página |
| `metadata.current_page` | integer | não observado | Página atual, iniciada em 1 |
| `metadata.total_pages` | integer | não observado | Total de páginas |
| `metadata.next_page` | integer/string | sim, na última página | Próxima página |
| `metadata.previous_page` | integer/string | sim, na primeira página | Página anterior |
| `metadata.*_page_url` | string | sim nas extremidades | Links de navegação |
| `metadata.db_published_date` | string | não observado | Atualização da base exposta pelo serviço |

Observações:

- A paginação usa `pagesize` (máximo documentado: 100) e `page` (começa em 1).
- `published_date` chega como texto e exigirá parsing explícito em uma fase posterior.
- Algumas posições sem página anterior/próxima podem vir como a string `"null"`, e não necessariamente como JSON `null`. O pipeline futuro não deve confiar apenas na tipagem de uma amostra.

## openFDA FAERS — eventos adversos

Endpoint: `GET https://api.fda.gov/drug/event.json`

| Caminho | Tipo observado | Pode faltar/ser nulo? | Interpretação |
|---|---|---:|---|
| `meta.last_updated` | string `YYYY-MM-DD` | não observado | Atualização informada pelo dataset |
| `meta.results.skip` | integer | não observado | Quantos resultados foram pulados |
| `meta.results.limit` | integer | não observado | Tamanho da página |
| `meta.results.total` | integer | não observado | Total de resultados da consulta |
| `results` | array | não observado em HTTP 200 | Eventos retornados |
| `results[].safetyreportid` | string | não observado | Identificador do relato |
| `results[].safetyreportversion` | string numérica | sim | Versão do relato |
| `results[].serious` | string categórica | sim | Indicador de gravidade, não booleano nativo |
| `results[].receivedate` | string `YYYYMMDD` | sim | Data recebida pela autoridade |
| `results[].receiptdate` | string `YYYYMMDD` | sim | Data de recebimento do relato |
| `results[].transmissiondate` | string `YYYYMMDD` | sim | Data de transmissão |
| `results[].primarysource` | object | sim | Informações do notificador/origem |
| `results[].receiver` | object ou null | sim | Destinatário; nulo foi observado |
| `results[].patient` | object | sim | Bloco aninhado do paciente |
| `results[].patient.reaction` | array | sim | Uma ou mais reações MedDRA |
| `results[].patient.reaction[].reactionmeddrapt` | string | sim | Termo preferido da reação |
| `results[].patient.drug` | array | sim | Um ou mais medicamentos associados |
| `results[].patient.drug[].medicinalproduct` | string | sim | Nome informado do produto |
| `results[].patient.patientdeath` | object | sim | Bloco presente somente em alguns relatos |

Observações:

- A paginação usa `skip` + `limit`; a segunda página começa com `skip=limit`.
- O máximo documentado de `limit` por chamada é 1.000.
- Muitos números e indicadores chegam como strings. Converter `"1"` diretamente em booleano seria uma decisão de negócio prematura.
- Os arrays de drogas e reações tornam o JSON hierárquico: uma linha de relato pode conter vários itens de cada tipo.
- A ausência de um campo é diferente de um campo presente com valor `null`. Ambos precisam ser considerados na modelagem futura.

## Hipóteses a validar antes da ingestão

1. `setid` + `spl_version` identifica unicamente uma versão de bula?
2. `safetyreportid` sozinho é suficiente ou deve ser combinado com `safetyreportversion`?
3. Qual data do FAERS representa melhor o `event_time`: início do evento, recebimento ou transmissão?
4. Como explodir as relações muitos-para-muitos entre relato, medicamentos e reações sem mudar o grão acidentalmente?
5. Quais códigos categóricos precisam de tabelas de domínio oficiais?

Essas perguntas ficam abertas de propósito. O Dia 2 é exploração; decisões de ingestão e modelagem pertencem às fases seguintes.

