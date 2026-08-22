import pytest


@pytest.fixture(scope="function", autouse=True)
def _disable_cooperative_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    # Golden files for native-query column lineage assume CLL completes. CI runners
    # can exceed the default 10s cooperative timeout once SchemaResolver does extra
    # case-normalization work per lookup.
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_lineage.SQL_LINEAGE_TIMEOUT_ENABLED", False
    )
