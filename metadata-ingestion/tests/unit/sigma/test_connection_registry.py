from dataclasses import dataclass

from datahub.ingestion.source.sigma.connection_registry import (
    SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP,
    SigmaConnectionRegistry,
)


@dataclass
class _FakeReporter:
    connections_resolved: int = 0
    connections_with_partial_metadata: int = 0
    connections_unmappable_type: int = 0


def _build(raw: list, reporter: _FakeReporter = None) -> SigmaConnectionRegistry:
    if reporter is None:
        reporter = _FakeReporter()
    return SigmaConnectionRegistry.build(
        raw,
        reporter=reporter,
        type_to_platform_map=SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP,
    )


def test_registry_builds_from_full_metadata():
    raw = [
        {
            "connectionId": "conn-1",
            "name": "Prod Snowflake",
            "type": "snowflake",
            "host": "acme.snowflakecomputing.com",
            "account": "acme",
            "warehouse": "COMPUTE_WH",
        }
    ]
    registry = _build(raw)
    rec = registry.get("conn-1")
    assert rec is not None
    assert rec.datahub_platform == "snowflake"
    assert rec.host == "acme.snowflakecomputing.com"
    assert rec.account == "acme"
    assert rec.instance_hint == "COMPUTE_WH"
    assert rec.confidence == 1.0


def test_registry_partial_metadata_records_lower_confidence():
    # host missing → confidence 0.5
    raw = [
        {
            "connectionId": "conn-2",
            "name": "Partial Snowflake",
            "type": "snowflake",
        }
    ]
    reporter = _FakeReporter()
    registry = _build(raw, reporter)
    rec = registry.get("conn-2")
    assert rec is not None
    assert rec.confidence == 0.5
    assert reporter.connections_with_partial_metadata == 1


def test_registry_unmappable_type_keeps_record_at_zero_confidence():
    raw = [
        {
            "connectionId": "conn-3",
            "name": "Unknown Warehouse",
            "type": "oracle",
            "host": "oracle.example.com",
        }
    ]
    reporter = _FakeReporter()
    registry = _build(raw, reporter)
    rec = registry.get("conn-3")
    assert rec is not None
    assert rec.datahub_platform == ""
    assert rec.confidence == 0.0
    assert reporter.connections_unmappable_type == 1


def test_registry_get_returns_none_for_unknown_id():
    registry = _build([])
    assert registry.get("nonexistent") is None


def test_registry_is_resolvable_predicate():
    raw = [
        {
            "connectionId": "conn-ok",
            "name": "OK",
            "type": "snowflake",
            "host": "acme.snowflakecomputing.com",
        },
        {
            "connectionId": "conn-bad",
            "name": "Bad",
            "type": "oracle",
        },
    ]
    registry = _build(raw)
    assert registry.is_resolvable("conn-ok") is True
    assert registry.is_resolvable("conn-bad") is False
    assert registry.is_resolvable("nonexistent") is False


def test_registry_build_handles_empty_input():
    reporter = _FakeReporter()
    registry = _build([], reporter)
    assert registry.by_id == {}
    assert reporter.connections_resolved == 0
    assert reporter.connections_unmappable_type == 0


def test_registry_build_increments_reporter_counters():
    raw = [
        {
            "connectionId": "c1",
            "name": "Full",
            "type": "snowflake",
            "host": "acme.snowflakecomputing.com",
        },
        {
            "connectionId": "c2",
            "name": "Partial",
            "type": "bigquery",
            # host missing → partial
        },
        {
            "connectionId": "c3",
            "name": "Unknown",
            "type": "oracle",
        },
    ]
    reporter = _FakeReporter()
    _build(raw, reporter)
    # c1 (full) + c2 (partial) are both resolved (confidence > 0)
    assert reporter.connections_resolved == 2
    assert reporter.connections_with_partial_metadata == 1
    assert reporter.connections_unmappable_type == 1
