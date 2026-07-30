from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError

from pharma_pipeline.config import Settings
from pharma_pipeline.contracts import LakeTable, contract_for, lake_table

# Colunas tecnicas que mudam a cada extracao e, por isso, nao indicam mudanca real do dado.
_VOLATILE_BRONZE_COLUMNS = ("ingest_time", "extraction_id")

# Maximo de linhas por chamada de `upsert`. O PyIceberg monta uma expressao booleana com uma
# comparacao por linha para localizar o que sera substituido; sem limite, dezenas de milhares
# de linhas produzem uma arvore profunda demais e o processo morre por estouro de pilha.
_LOTE_UPSERT = 2_000


@dataclass(frozen=True, slots=True)
class UpsertResult:
    """Resultado de um UPSERT em uma tabela Iceberg."""

    table_name: str
    input_rows: int
    candidate_rows: int
    rows_inserted: int
    rows_updated: int
    snapshot_before: int | None
    snapshot_after: int | None
    created_table: bool = False

    @property
    def snapshot_created(self) -> bool:
        return self.snapshot_after is not None and self.snapshot_after != self.snapshot_before

    @property
    def unchanged(self) -> bool:
        return not self.snapshot_created and self.rows_inserted == 0 and self.rows_updated == 0


@dataclass(frozen=True, slots=True)
class SyncResult:
    """Resultado da sincronizacao bronze Parquet -> tabela Iceberg."""

    source: str
    table_name: str
    parquet_files: int
    input_rows: int
    deduplicated_rows: int
    rows_inserted: int
    rows_updated: int
    snapshot_before: int | None
    snapshot_after: int | None

    @property
    def snapshot_created(self) -> bool:
        return self.snapshot_after is not None and self.snapshot_after != self.snapshot_before


def catalog_properties(settings: Settings) -> dict[str, str]:
    """Propriedades do catalogo Iceberg.

    Fonte unica de verdade: o CLI usa este dicionario e tambem o exporta como variaveis de
    ambiente para o `profiles.yml` do dbt, evitando que as duas configuracoes divirjam.
    """
    catalog_path = Path(settings.iceberg_catalog_path).resolve().as_posix()
    return {
        "uri": f"sqlite:///{catalog_path}",
        "warehouse": f"s3://{settings.minio_bucket}/{settings.iceberg_warehouse_prefix}",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": settings.minio_endpoint,
        "s3.access-key-id": settings.minio_user,
        "s3.secret-access-key": settings.minio_password,
        "s3.region": settings.aws_region,
        "s3.force-virtual-addressing": "false",
    }


def _catalog(settings: Settings) -> SqlCatalog:
    return SqlCatalog("local", **catalog_properties(settings))


def _s3(settings: Settings) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=settings.minio_user,
        secret=settings.minio_password,
        endpoint_url=settings.minio_endpoint,
        client_kwargs={"region_name": settings.aws_region},
        use_ssl=settings.minio_endpoint.startswith("https://"),
    )


def _identifier(source: str) -> str:
    return f"bronze.{contract_for(source).table_name}"


def _read_bronze(settings: Settings, source: str) -> tuple[pa.Table, list[str]]:
    contract = contract_for(source)
    fs = _s3(settings)
    pattern = f"{settings.minio_bucket}/bronze/{source}/**/{contract.table_name}/**/*.parquet"
    files = sorted(fs.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"Nenhum Parquet encontrado para {source} em bronze/{source}. Rode a ingestao primeiro."
        )

    tables: list[pa.Table] = []
    for path in files:
        with fs.open(path, "rb") as parquet_file:
            tables.append(pq.read_table(parquet_file))
    return pa.concat_tables(tables, promote_options="default"), files


def _without_dlt_internal_columns(data: pa.Table) -> pa.Table:
    keep = [name for name in data.column_names if not name.startswith("_dlt_")]
    return data.select(keep)


def normalize_arrow(data: pa.Table) -> pa.Table:
    """Alinha tipos do Arrow aos que o Iceberg aceita.

    Tres ajustes, todos motivados por diferencas reais entre o que o DuckDB exporta e o que o
    Iceberg aceita:

    - `large_string`/`large_binary` viram `string`/`binary`;
    - timestamps passam para microssegundos, a precisao do tipo Iceberg;
    - timestamps com fuso passam para UTC.

    O ultimo e o mais importante. O DuckDB exporta `timestamp with time zone` marcado com o
    fuso da sessao (por exemplo `America/Sao_Paulo`), e o Iceberg so aceita `timestamptz` em
    UTC. A conversao e apenas de rotulo: o Arrow guarda o instante como epoch, entao nenhum
    horario e deslocado -- o mesmo instante passa a ser lido em UTC, que e como o pipeline
    inteiro raciocina sobre tempo.
    """
    fields = []
    changed = False
    for field in data.schema:
        target = field.type
        if pa.types.is_large_string(target):
            target = pa.string()
        elif pa.types.is_large_binary(target):
            target = pa.binary()
        elif pa.types.is_timestamp(target):
            tz = "UTC" if target.tz is not None else None
            if target.unit != "us" or target.tz != tz:
                target = pa.timestamp("us", tz=tz)
        if target != field.type:
            changed = True
        fields.append(field.with_type(target))
    return data.cast(pa.schema(fields)) if changed else data


def _deduplicate(data: pa.Table, primary_key: str) -> pa.Table:
    """Mantem a versao de maior ingest_time por chave; adequado ao volume do laboratorio."""
    latest: dict[Any, dict[str, Any]] = {}
    for row in data.to_pylist():
        key = row[primary_key]
        previous = latest.get(key)
        if previous is None or row["ingest_time"] > previous["ingest_time"]:
            latest[key] = row
    return pa.Table.from_pylist(list(latest.values()), schema=data.schema)


def _compare_columns(data: pa.Table, join_cols: tuple[str, ...]) -> tuple[str, ...]:
    """Colunas que definem 'a linha mudou de verdade'.

    Na bronze o `raw_payload` e o registro original completo: se ele nao mudou, a fonte nao
    mudou, e reler o mesmo dado nao deve gerar snapshot. Nas camadas derivadas os modelos sao
    deterministicos, entao comparamos todas as colunas fora da chave.
    """
    if "raw_payload" in data.column_names:
        return ("raw_payload",)
    return tuple(
        name
        for name in data.column_names
        if name not in join_cols and name not in _VOLATILE_BRONZE_COLUMNS
    )


def _changed_rows(
    incoming: pa.Table,
    current: pa.Table,
    join_cols: tuple[str, ...],
    compare_cols: tuple[str, ...],
) -> pa.Table:
    """Retem chaves novas ou linhas realmente alteradas, ignorando releitura sem mudanca."""

    def key(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[column] for column in join_cols)

    def fingerprint(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[column] for column in compare_cols)

    current_index = {key(row): fingerprint(row) for row in current.to_pylist()}
    changed = [
        row for row in incoming.to_pylist() if current_index.get(key(row)) != fingerprint(row)
    ]
    return pa.Table.from_pylist(changed, schema=incoming.schema)


def _chaves(data: pa.Table, join_cols: tuple[str, ...]) -> set[tuple[Any, ...]]:
    """Conjunto das chaves presentes em uma tabela."""
    colunas = [data.column(nome).to_pylist() for nome in join_cols]
    return set(zip(*colunas, strict=True))


def _separar_novos_de_alterados(
    candidates: pa.Table,
    chaves_existentes: set[tuple[Any, ...]],
    join_cols: tuple[str, ...],
) -> tuple[pa.Table, pa.Table]:
    """Divide as linhas a gravar entre INSERCAO pura e ATUALIZACAO de chave existente.

    Por que a divisao existe
    ------------------------
    O `upsert` do PyIceberg precisa localizar as linhas que serao substituidas, e para isso
    constroi uma expressao booleana com uma comparacao POR LINHA da entrada. Com dezenas de
    milhares de linhas, essa arvore fica profunda demais e o interpretador estoura a pilha --
    o processo morre com STATUS_STACK_OVERFLOW (0xC00000FD), sem excecao e sem mensagem.

    Foi exatamente o que aconteceu ao publicar `silver.stg_faers_drugs` com 38.639 linhas.

    Linhas cuja chave ainda nao existe na tabela nao precisam localizar nada: um `append`
    resolve, sem expressao nenhuma. Como o caso normal de um pipeline em crescimento e
    "chegaram linhas novas", essa separacao elimina o problema na maior parte das execucoes --
    e ainda e mais rapida, porque `append` nao reescreve arquivo existente.

    O que sobra (chaves que realmente mudaram) segue pelo `upsert`, em lotes.
    """
    if not chaves_existentes:
        return candidates, candidates.slice(0, 0)

    colunas = [candidates.column(nome).to_pylist() for nome in join_cols]
    indices_novos: list[int] = []
    indices_alterados: list[int] = []
    for posicao, chave in enumerate(zip(*colunas, strict=True)):
        destino = indices_alterados if chave in chaves_existentes else indices_novos
        destino.append(posicao)

    # O tipo do indice precisa ser explicito: uma lista vazia viraria um array de tipo `null`,
    # que o `take` do Arrow nao aceita.
    def selecionar(indices: list[int]) -> pa.Table:
        return candidates.take(pa.array(indices, type=pa.int64()))

    return selecionar(indices_novos), selecionar(indices_alterados)


def _em_lotes(data: pa.Table, tamanho: int):
    """Fatia uma tabela em lotes, mantendo a profundidade da expressao do upsert sob controle."""
    for inicio in range(0, data.num_rows, tamanho):
        yield data.slice(inicio, tamanho)


class SchemaIncompativel(RuntimeError):
    """O modelo mudou de colunas e a tabela Iceberg publicada ficou para tras."""


def _exigir_schema_compativel(table: Any, incoming: pa.Table, identifier: str) -> None:
    """Falha cedo, e com instrucao, quando o modelo ganhou ou perdeu colunas.

    Sem esta checagem o erro so aparece la dentro do PyIceberg, como
    `ValueError: Could not find column: 'x'` -- uma mensagem que nao diz qual tabela, qual
    modelo nem o que fazer. Trocar um schema publicado exige uma decisao consciente, entao
    exigimos `--recreate` em vez de evoluir o schema por conta propria.
    """
    publicadas = {field.name for field in table.schema().fields}
    recebidas = set(incoming.column_names)

    novas = sorted(recebidas - publicadas)
    removidas = sorted(publicadas - recebidas)
    if not novas and not removidas:
        return

    detalhes = []
    if novas:
        detalhes.append(f"colunas novas no modelo: {', '.join(novas)}")
    if removidas:
        detalhes.append(f"colunas que sumiram do modelo: {', '.join(removidas)}")

    raise SchemaIncompativel(
        f"{identifier}: o schema do modelo nao bate com o da tabela Iceberg publicada "
        f"({'; '.join(detalhes)}). "
        f"Republique com `pharma-pipeline publish {identifier} --recreate` para descartar e "
        "recriar a tabela. Isso APAGA o historico de snapshots dela, portanto e uma decisao "
        "explicita, e nao um efeito colateral da publicacao."
    )


def upsert_arrow(
    settings: Settings,
    identifier: str,
    incoming: pa.Table,
    *,
    join_cols: tuple[str, ...],
    snapshot_properties: dict[str, str] | None = None,
    recreate: bool = False,
) -> UpsertResult:
    """Faz UPSERT idempotente de uma tabela Arrow em uma tabela Iceberg.

    Cria namespace e tabela quando ausentes. Linhas byte-identicas as ja publicadas nao geram
    commit, de modo que reexecutar o pipeline nao cria snapshots vazios.
    """
    incoming = normalize_arrow(incoming)
    missing = [column for column in join_cols if column not in incoming.column_names]
    if missing:
        raise ValueError(
            f"{identifier}: colunas de chave ausentes no resultado: {', '.join(missing)}."
        )

    catalog = _catalog(settings)
    namespace = identifier.split(".", 1)[0]
    catalog.create_namespace_if_not_exists(namespace)

    created = False
    if recreate and catalog.table_exists(identifier):
        catalog.drop_table(identifier)
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        table = catalog.create_table(
            identifier=identifier,
            schema=incoming.schema,
            properties={
                "format-version": "2",
                "write.parquet.compression-codec": "zstd",
                "write.metadata.delete-after-commit.enabled": "false",
            },
        )
        created = True

    if not created:
        _exigir_schema_compativel(table, incoming, identifier)

    before = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    compare_cols = _compare_columns(incoming, join_cols)
    candidates = incoming
    chaves_existentes: set[tuple[Any, ...]] = set()
    if before is not None:
        current = table.scan(selected_fields=join_cols + compare_cols).to_arrow()
        candidates = _changed_rows(incoming, current, join_cols, compare_cols)
        chaves_existentes = _chaves(current, join_cols)

    rows_inserted = 0
    rows_updated = 0
    if candidates.num_rows:
        propriedades = {
            "pipeline": "pharma-freshness",
            "committed-at-utc": datetime.now(UTC).isoformat(),
            **(snapshot_properties or {}),
        }
        novos, alterados = _separar_novos_de_alterados(candidates, chaves_existentes, join_cols)

        # Tudo numa transacao so: a publicacao de uma tabela produz UM snapshot, mesmo tendo
        # sido dividida em varias operacoes por questao de escala.
        with table.transaction() as transacao:
            if novos.num_rows:
                transacao.append(novos, snapshot_properties=propriedades)
                rows_inserted = novos.num_rows
            for lote in _em_lotes(alterados, _LOTE_UPSERT):
                resultado = transacao.upsert(
                    lote, join_cols=list(join_cols), snapshot_properties=propriedades
                )
                rows_inserted += resultado.rows_inserted
                rows_updated += resultado.rows_updated
        table.refresh()

    after = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    return UpsertResult(
        table_name=identifier,
        input_rows=incoming.num_rows,
        candidate_rows=candidates.num_rows,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        snapshot_before=before,
        snapshot_after=after,
        created_table=created,
    )


def replace_arrow(
    settings: Settings,
    identifier: str,
    incoming: pa.Table,
    *,
    join_cols: tuple[str, ...],
    snapshot_properties: dict[str, str] | None = None,
    recreate: bool = False,
) -> UpsertResult:
    """Substitui o conteudo inteiro de uma tabela Iceberg de forma idempotente.

    Usado por tabelas de JANELA MOVEL (serving): o UPSERT nao remove chaves que sairam da
    janela. Aqui o estado publicado passa a ser exatamente o lote lido do DuckDB.

    Se o conteudo for byte-equivalente ao snapshot atual (mesmas chaves e mesmos valores),
    nenhum commit e criado — a prova de idempotencia do mesmo dia continua valendo.
    """
    incoming = normalize_arrow(incoming)
    missing = [column for column in join_cols if column not in incoming.column_names]
    if missing:
        raise ValueError(
            f"{identifier}: colunas de chave ausentes no resultado: {', '.join(missing)}."
        )

    catalog = _catalog(settings)
    namespace = identifier.split(".", 1)[0]
    catalog.create_namespace_if_not_exists(namespace)

    created = False
    if recreate and catalog.table_exists(identifier):
        catalog.drop_table(identifier)
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        table = catalog.create_table(
            identifier=identifier,
            schema=incoming.schema,
            properties={
                "format-version": "2",
                "write.parquet.compression-codec": "zstd",
                "write.metadata.delete-after-commit.enabled": "false",
            },
        )
        created = True

    if not created:
        _exigir_schema_compativel(table, incoming, identifier)

    before = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    compare_cols = _compare_columns(incoming, join_cols)

    if before is not None:
        current = table.scan(selected_fields=join_cols + compare_cols).to_arrow()
        if _conteudo_identico(incoming, current, join_cols, compare_cols):
            return UpsertResult(
                table_name=identifier,
                input_rows=incoming.num_rows,
                candidate_rows=0,
                rows_inserted=0,
                rows_updated=0,
                snapshot_before=before,
                snapshot_after=before,
                created_table=False,
            )

    propriedades = {
        "pipeline": "pharma-freshness",
        "committed-at-utc": datetime.now(UTC).isoformat(),
        "write-mode": "replace",
        **(snapshot_properties or {}),
    }
    table.overwrite(incoming, snapshot_properties=propriedades)
    table.refresh()
    after = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    return UpsertResult(
        table_name=identifier,
        input_rows=incoming.num_rows,
        candidate_rows=incoming.num_rows,
        rows_inserted=incoming.num_rows,
        rows_updated=0,
        snapshot_before=before,
        snapshot_after=after,
        created_table=created,
    )


def _conteudo_identico(
    incoming: pa.Table,
    current: pa.Table,
    join_cols: tuple[str, ...],
    compare_cols: tuple[str, ...],
) -> bool:
    """True quando as duas tabelas tem o mesmo conjunto de (chave, fingerprint)."""
    if incoming.num_rows != current.num_rows:
        return False

    def assinatura(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[column] for column in join_cols + compare_cols)

    return {assinatura(row) for row in incoming.to_pylist()} == {
        assinatura(row) for row in current.to_pylist()
    }


def sync_bronze_to_iceberg(settings: Settings, source: str) -> SyncResult:
    """Le os Parquet imutaveis da bronze e publica o estado atual na tabela Iceberg."""
    contract = contract_for(source)
    raw, files = _read_bronze(settings, source)
    incoming = _deduplicate(_without_dlt_internal_columns(raw), contract.primary_key)
    result = upsert_arrow(
        settings,
        _identifier(source),
        incoming,
        join_cols=(contract.primary_key,),
        snapshot_properties={"source": source, "layer": "bronze"},
    )
    return SyncResult(
        source=source,
        table_name=result.table_name,
        parquet_files=len(files),
        input_rows=raw.num_rows,
        deduplicated_rows=incoming.num_rows,
        rows_inserted=result.rows_inserted,
        rows_updated=result.rows_updated,
        snapshot_before=result.snapshot_before,
        snapshot_after=result.snapshot_after,
    )


def resolve(identifier: str) -> LakeTable:
    return lake_table(identifier)


def list_snapshots(settings: Settings, identifier: str) -> list[dict[str, Any]]:
    table = _catalog(settings).load_table(resolve(identifier).identifier)
    rows = table.inspect.snapshots().to_pylist()
    for row in rows:
        row["committed_at"] = row["committed_at"].isoformat()
        row["summary"] = dict(row["summary"] or [])
    return rows


def query_table(
    settings: Settings,
    identifier: str,
    *,
    snapshot_id: int | None = None,
    as_of: datetime | None = None,
    limit: int = 10,
    columns: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    table = _catalog(settings).load_table(resolve(identifier).identifier)
    if as_of is not None:
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=UTC)
        snapshot = table.snapshot_as_of_timestamp(int(as_of.timestamp() * 1000))
        if snapshot is None:
            raise ValueError(f"Nao existe snapshot em ou antes de {as_of.isoformat()}.")
        snapshot_id = snapshot.snapshot_id
    selected_fields = columns or ("*",)
    return (
        table.scan(snapshot_id=snapshot_id, selected_fields=selected_fields, limit=limit)
        .to_arrow()
        .to_pylist()
    )


def read_table(settings: Settings, identifier: str) -> pa.Table:
    """Le a tabela publicada inteira; usado pelas validacoes de contrato."""
    return _catalog(settings).load_table(resolve(identifier).identifier).scan().to_arrow()


def row_count(settings: Settings, identifier: str, snapshot_id: int | None = None) -> int:
    table = _catalog(settings).load_table(resolve(identifier).identifier)
    return table.scan(snapshot_id=snapshot_id).count()


def table_exists(settings: Settings, identifier: str) -> bool:
    return _catalog(settings).table_exists(resolve(identifier).identifier)
