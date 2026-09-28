from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, cast

import pydantic
import pytest

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.external_dq.config import ExternalDQConfig
from datahub.ingestion.source.external_dq.contract import (
    RESULTS_COLUMNS,
    RULES_COLUMNS,
    ContractColumn,
    LogicalType,
)
from datahub.ingestion.source.external_dq.extractor import (
    ExternalDQExtractor,
    SelectColumn,
)
from datahub.ingestion.source.external_dq.mapper import ExternalDQMapper
from datahub.ingestion.source.external_dq.report import ExternalDQReport
from datahub.ingestion.source.external_dq.state import ExternalDQStateHandler
from datahub.ingestion.source.external_dq.types import DATABRICKS_TYPE_PROFILE
from datahub.ingestion.source.external_dq.validate import PhysicalColumn
from datahub.ingestion.source.state.stateful_ingestion_base import StateProviderWrapper
from datahub.metadata.schema_classes import AssertionRunEventClass
from tests.unit.external_dq._fixtures import T0, FakeStateProvider, result_raw, rule_raw

RULES, RESULTS = "main.governance.dq_rules", "main.governance.dq_results"
DBX = {
    LogicalType.STRING: "string",
    LogicalType.BOOLEAN: "boolean",
    LogicalType.INT64: "bigint",
    LogicalType.FLOAT64: "double",
    LogicalType.TIMESTAMP: "timestamp",
    LogicalType.ARRAY_STRING: "array<string>",
}


def _physical(contract: Sequence[ContractColumn]) -> List[PhysicalColumn]:
    return [
        PhysicalColumn(c.name, DBX[c.logical_type], i + 1)
        for i, c in enumerate(contract)
    ]


class FakeReader:
    def __init__(
        self, results: List[Dict[str, Any]], fail_after: Optional[int] = None
    ) -> None:
        self.tables = {
            RULES: _physical(RULES_COLUMNS),
            RESULTS: _physical(RESULTS_COLUMNS),
        }
        self.rules = [rule_raw()]
        self.results = results
        self.fail_after = fail_after
        self.reads = 0

    def describe(self, table: str) -> List[PhysicalColumn]:
        return self.tables[table]

    def read_rules(
        self, table: str, columns: Sequence[SelectColumn]
    ) -> Iterable[Mapping[str, Any]]:
        self.reads += 1
        return list(self.rules)

    def read_results(
        self, table: str, columns: Sequence[SelectColumn], since_millis: int
    ) -> Iterable[Mapping[str, Any]]:
        self.reads += 1
        rows = sorted(
            (r for r in self.results if r["executed_at"] >= since_millis),
            key=lambda r: (r["executed_at"], r["run_id"]),
        )
        for i, row in enumerate(rows):
            if self.fail_after is not None and i == self.fail_after:
                raise ConnectionError("warehouse connection dropped")
            yield row


class Locator:
    def dataset_urn(self, dataset_path: Sequence[str]) -> Optional[str]:
        return make_dataset_urn("databricks", ".".join(dataset_path))

    def field_urn(self, dataset_urn: str, column_path: str) -> str:
        return make_schema_field_urn(dataset_urn, column_path)


def run(reader: FakeReader, state: Optional[ExternalDQStateHandler] = None) -> tuple:
    source_report, report = SourceReport(), ExternalDQReport()
    config = ExternalDQConfig(enabled=True, rules_table=RULES, results_table=RESULTS)
    mapper = ExternalDQMapper(
        platform="databricks",
        platform_instance=None,
        rule_namespace="default",
        locator=Locator(),
        report=report,
        source_report=source_report,
    )
    extractor = ExternalDQExtractor(
        config=config,
        reader=reader,
        mapper=mapper,
        profile=DATABRICKS_TYPE_PROFILE,
        source_report=source_report,
        report=report,
        state=state,
        now_millis=lambda: T0 + 1_000,
    )
    workunits = list(extractor.get_workunits())
    run_events = [wu for wu in workunits if _is_run_event(wu)]
    return workunits, run_events, source_report, report


def _is_run_event(wu: MetadataWorkUnit) -> bool:
    return isinstance(wu.metadata.aspect, AssertionRunEventClass)  # type: ignore[union-attr]


def _handler(provider: FakeStateProvider) -> ExternalDQStateHandler:
    return ExternalDQStateHandler(
        state_provider=cast(StateProviderWrapper, provider),
        pipeline_name="p",
        run_id="r",
    )


def test_config_requires_both_tables_when_enabled() -> None:
    with pytest.raises(pydantic.ValidationError):
        ExternalDQConfig(enabled=True, rules_table=RULES)


def test_emits_definitions_and_run_events_and_warns_without_state() -> None:
    reader = FakeReader([result_raw(), result_raw(run_id="run-2", executed_at=T0 + 1)])
    workunits, run_events, source_report, report = run(reader)
    assert len(workunits) == 4  # info + status + 2 run events
    assert len(run_events) == 2
    assert report.run_events_emitted == 2
    assert any(w.title and "re-read" in w.title for w in source_report.warnings)


def test_contract_violation_fails_before_reading() -> None:
    reader = FakeReader([result_raw()])
    reader.tables[RESULTS] = [p for p in reader.tables[RESULTS] if p.name != "status"]
    workunits, _, source_report, _ = run(reader)
    assert workunits == [] and reader.reads == 0
    assert source_report.failures


def test_invalid_row_is_skipped_not_fatal() -> None:
    reader = FakeReader([result_raw(status="WARN"), result_raw(run_id="run-2")])
    _, run_events, _, report = run(reader)
    assert len(run_events) == 1 and report.results_skipped_invalid == 1


def test_second_run_emits_no_duplicate_run_events() -> None:
    rows = [result_raw()]
    first = FakeStateProvider()
    assert len(run(FakeReader(rows), _handler(first))[1]) == 1
    second = FakeStateProvider(last=first.current)
    _, run_events, _, report = run(FakeReader(rows), _handler(second))
    assert run_events == [] and report.results_already_emitted == 1


def test_read_failure_does_not_advance_watermark() -> None:
    first = FakeStateProvider()
    run(FakeReader([result_raw()]), _handler(first))
    second = FakeStateProvider(last=first.current)
    rows = [
        result_raw(),
        result_raw(run_id="run-2", executed_at=T0 + 5),
        result_raw(run_id="run-3", executed_at=T0 + 9),
    ]
    _, _, source_report, _ = run(FakeReader(rows, fail_after=2), _handler(second))
    assert source_report.failures
    assert second.current is not None
    assert second.current.state.watermarks == {RESULTS: T0}  # type: ignore[attr-defined]
