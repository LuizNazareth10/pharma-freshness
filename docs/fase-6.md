# Fase 6 — Fechamento (Dias 13–14)

> **Objetivo:** o projeto está documentado, reproduzível e pronto para a próxima fase (LLM).
> **Estado:** implementada.

Até a Fase 4 o pipeline roda sozinho e mede frescor. Falta a fatia que um consumidor
(humano ou LLM) consulta sem montar joins, e a prova de que o laboratório sobe do zero.

---

## Índice

1. [O que foi construído](#1-o-que-foi-construído)
2. [Dia 13 — Camada de consumo e documentação](#2-dia-13--camada-de-consumo-e-documentação)
3. [Dia 14 — Revisão de ponta a ponta](#3-dia-14--revisão-de-ponta-a-ponta)
4. [Como executar](#4-como-executar)
5. [Como consultar](#5-como-consultar)
6. [Limites conscientes](#6-limites-conscientes)

---

## 1. O que foi construído

| Peça | Onde | O que faz |
|---|---|---|
| `alertas_recentes` | `transform/models/gold/` | Eventos graves dos últimos 7 dias, denormalizados |
| `bulas_atualizadas` | `transform/models/gold/` | Bulas com `published_date` nos últimos 3 dias |
| Documentação de grão | `gold.yml` | Grão, citação e testes `not_null` em cada coluna-chave |
| `replace_on_publish` | `contracts.py` + `iceberg.replace_arrow` | Publica janelas móveis sem deixar chaves expiradas |
| Scripts | `scripts/day13`, `scripts/day14` | Docs + verificação de reprodutibilidade |

---

## 2. Dia 13 — Camada de consumo e documentação

### Por que serving separado do estrela

O esquema dimensional (`dim_*` + `fato_*`) é o contrato analítico. Uma LLM (e a maioria dos
painéis) não deve descobrir joins a cada pergunta. As tabelas de serving são **fatias
denormalizadas**, já filtradas, com fonte e datas citáveis em cada linha.

| Tabela | Janela | Grão | Fonte |
|---|---|---|---|
| `alertas_recentes` | `receivedate` ≤ 7 dias | um `id_evento` (par fármaco–reação) grave | FAERS |
| `bulas_atualizadas` | `published_date` ≤ 3 dias | uma bula (`id_bula` / `setid`) | DailyMed |

A janela usa o relógio da **fonte**, não o `ingest_time`. Assim "recente" descreve o mundo,
não o atraso do pipeline.

### REPLACE em vez de UPSERT

Quando a janela desliza, linhas saem do DuckDB. O UPSERT Iceberg só insere/atualiza — **não
apaga** chaves ausentes. Sem REPLACE, um alerta de 8 dias atrás continuaria "recente" no MinIO.

Por isso o contrato declara `replace_on_publish=True`. A publicação compara o lote inteiro com
o snapshot atual; conteúdo idêntico (mesmo dia, mesma janela) não cria snapshot — a prova de
idempotência do laboratório continua válida.

### Documentação e lineage

```powershell
pharma-pipeline transform docs          # gera o catalogo em .local/dbt
.\scripts\day13\Serve-Docs.ps1          # sobe o site local do dbt docs (porta 8082)
```

O lineage mostra `stg_*` → dims/fatos → serving. O grão de cada modelo está em `gold.yml`
(e nos comentários SQL do cabeçalho).

---

## 3. Dia 14 — Revisão de ponta a ponta

Critério de pronto: subir do zero e obter `metricas_frescor` preenchida + `dbt test` verde.

```powershell
.\scripts\day14\Verify-Reproducibility.ps1              # usa o estado atual
.\scripts\day14\Verify-Reproducibility.ps1 -FromScratch # docker down -v + sobe de novo
```

`-FromScratch` apaga volumes (MinIO e Postgres do Airflow). Só use se puder reingerir.

---

## 4. Como executar

Pré-requisito: Fases 2–4 já populadas (bronze + transform + publish ao menos uma vez).

```powershell
.\.venv\Scripts\Activate.ps1

# Materializa serving (+ testes dbt das novas tabelas)
pharma-pipeline transform build --select alertas_recentes bulas_atualizadas

# Ou o build completo
pharma-pipeline transform build

# Publica a gold (serving usa REPLACE; o resto, UPSERT)
pharma-pipeline publish gold

# Lineage
.\scripts\day13\Build-Serving.ps1
.\scripts\day13\Serve-Docs.ps1
```

---

## 5. Como consultar

```powershell
pharma-pipeline tables

pharma-pipeline query gold.alertas_recentes `
  --columns safetyreportid,farmaco,reacao,data_recebimento_fda,fonte,ingest_time --limit 5

pharma-pipeline query gold.bulas_atualizadas `
  --columns setid,farmaco,data_revisao,source_url,ingest_time --limit 5
```

Se a janela estiver vazia (nenhum relato grave / bula nos últimos dias no recorte do lab),
a tabela existe com 0 linhas — isso é resultado válido, não erro.

---

## 6. Limites conscientes

- **Sem histórico de versões de bula.** "Atualizada" = `published_date` recente na versão
  corrente. Histórico SPL vira SCD tipo 2 numa fase futura.
- **Grão de alerta = par fármaco–reação.** Somar linhas não conta casos clínicos; use
  `count(distinct safetyreportid)`.
- **Serving não entra no Great Expectations padrão.** Os contratos GE continuam nos fatos;
  os testes dbt cobrem a fatia de serving.
- **Fase 5 (Kafka/Flink) permanece opcional/pedagógica** no `foundation.md`; este fechamento
  não depende dela.
