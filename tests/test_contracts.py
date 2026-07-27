import pytest

from pharma_pipeline.contracts import contract_for


def test_real_primary_keys_are_explicit() -> None:
    assert contract_for("dailymed").primary_key == "setid"
    assert contract_for("faers").primary_key == "safetyreportid"
    assert contract_for("faers").cursor_field == "receiptdate"
    assert contract_for("res").primary_key == "recall_number"


def test_unknown_source_fails_with_context() -> None:
    with pytest.raises(ValueError, match="Fonte desconhecida"):
        contract_for("unknown")
