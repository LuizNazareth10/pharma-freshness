"""Testes do registro de tabelas do lakehouse.

O registro e a fonte de verdade sobre chave e grao de cada tabela publicada. Ele existe para
que o publicador nao precise adivinhar por qual coluna fazer UPSERT -- adivinhar errado
duplicaria linhas silenciosamente.
"""

import pytest

from pharma_pipeline.contracts import (
    CONTRACTS,
    LAKE_TABLES,
    PUBLISHABLE_LAYERS,
    lake_table,
    tables_in_layer,
)


def test_toda_fonte_de_ingestao_tem_tabela_bronze_correspondente() -> None:
    for contrato in CONTRACTS.values():
        tabela = lake_table(f"bronze.{contrato.table_name}")
        assert tabela.join_cols == (contrato.primary_key,)


def test_nome_curto_da_fonte_resolve_para_a_tabela_bronze() -> None:
    assert lake_table("faers").identifier == "bronze.faers_events"
    assert lake_table("bronze.faers_events").identifier == "bronze.faers_events"


def test_tabela_desconhecida_falha_com_contexto() -> None:
    with pytest.raises(ValueError, match="Tabela desconhecida"):
        lake_table("gold.nao_existe")


def test_toda_tabela_declara_chave_e_grao() -> None:
    for tabela in LAKE_TABLES.values():
        assert tabela.join_cols, f"{tabela.identifier} nao declara chave de UPSERT"
        assert tabela.grain.strip(), f"{tabela.identifier} nao declara grao"


def test_camadas_publicaveis_nao_incluem_a_bronze() -> None:
    """A bronze e publicada a partir dos Parquet imutaveis, nao do DuckDB."""
    assert "bronze" not in PUBLISHABLE_LAYERS
    assert set(PUBLISHABLE_LAYERS) == {"silver", "gold"}


def test_ordem_da_camada_respeita_dependencia_de_dimensao_antes_de_fato() -> None:
    """As dimensoes precisam existir antes dos fatos que apontam para elas."""
    nomes = [tabela.name for tabela in tables_in_layer("gold")]
    ultima_dimensao = max(indice for indice, nome in enumerate(nomes) if nome.startswith("dim_"))
    primeiro_fato = min(indice for indice, nome in enumerate(nomes) if nome.startswith("fato_"))
    assert ultima_dimensao < primeiro_fato


def test_tabelas_de_serving_declaram_replace_por_janela_movel() -> None:
    """Serving com janela movel precisa de REPLACE; UPSERT deixaria chaves expiradas."""
    assert lake_table("gold.alertas_recentes").replace_on_publish is True
    assert lake_table("gold.bulas_atualizadas").replace_on_publish is True
    assert lake_table("gold.fato_evento_adverso").replace_on_publish is False


def test_serving_vem_depois_dos_fatos() -> None:
    """As fatias de serving leem fatos e dimensoes; publicam por ultimo."""
    nomes = [tabela.name for tabela in tables_in_layer("gold")]
    assert nomes.index("alertas_recentes") > nomes.index("fato_evento_adverso")
    assert nomes.index("bulas_atualizadas") > nomes.index("dim_bula")


def test_camada_desconhecida_falha() -> None:
    with pytest.raises(ValueError, match="Camada desconhecida"):
        tables_in_layer("platinum")
