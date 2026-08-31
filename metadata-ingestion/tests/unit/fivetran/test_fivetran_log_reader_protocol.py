"""Pin the Protocol surface so refactors don't quietly drop methods
the source depends on."""

import inspect
from typing import get_type_hints

from datahub.ingestion.source.fivetran.fivetran_log_db_reader import (
    FivetranLogDbReader,
)
from datahub.ingestion.source.fivetran.log_reader import (
    FivetranConnectorReader,
    FivetranJobsReader,
    FivetranLineageReader,
)


def test_protocol_has_required_members():
    members = {name for name, _ in inspect.getmembers(FivetranConnectorReader)}
    assert "get_allowed_connectors_list" in members
    assert "get_user_email" in members
    # `fivetran_log_database` is intentionally NOT on the Protocol — its
    # semantics differed across implementations (real value vs sentinel).
    # The DB-mode default-database resolution lives on the concrete
    # `FivetranLogDbReader` class instead.


def test_protocol_method_signatures_stable():
    # If we ever rename or change the shape of these, existing
    # implementations will break — pin the contract.
    hints = get_type_hints(FivetranConnectorReader.get_user_email)
    # `Optional[str]` return — emails may be missing.
    assert hints.get("return") is not None


def test_fivetran_log_db_reader_satisfies_protocol():
    # Runtime-checkable Protocol: structural matching via hasattr.
    # Verifies the existing class hasn't drifted from the contract.
    for name in (
        "get_allowed_connectors_list",
        "get_user_email",
    ):
        assert hasattr(FivetranLogDbReader, name), f"missing {name}"


def test_jobs_reader_protocol_has_required_members():
    members = {name for name, _ in inspect.getmembers(FivetranJobsReader)}
    assert "fetch_jobs_for_connectors" in members


def test_fivetran_log_db_reader_satisfies_jobs_reader_protocol():
    # Hybrid mode passes a `FivetranLogDbReader` instance where a
    # `FivetranJobsReader` is expected; this confirms the structural
    # match is still in place after refactors.
    assert hasattr(FivetranLogDbReader, "fetch_jobs_for_connectors")


def test_lineage_reader_protocol_has_required_members():
    members = {name for name, _ in inspect.getmembers(FivetranLineageReader)}
    assert "fetch_lineage_for_connectors" in members


def test_fivetran_log_db_reader_satisfies_lineage_reader_protocol():
    # REST-primary hybrid uses `FivetranLogDbReader` as a
    # `FivetranLineageReader` to recover column lineage when Fivetran's
    # Metadata API is plan-restricted.
    assert hasattr(FivetranLogDbReader, "fetch_lineage_for_connectors")
