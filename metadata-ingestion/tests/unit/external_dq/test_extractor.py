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
)
from datahub.ingestion.source.external_dq.extractor import (
    ExternalDQExtractor,
    SelectColumn,
)
from datahub.ingestion.source.external_dq.mapper import ExternalDQMapper
from datahub.ingestion.source.external_dq.report import ExternalDQReport
from datahub.ingestion.source.external_dq.state import ExternalDQStateHandler, run_key
from datahub.ingestion.source.external_dq.types import DATABRICKS_TYPE_PROFILE
from datahub.ingestion.source.external_dq.validate import PhysicalColumn
from datahub.ingestion.source.state.stateful_ingestion_base import StateProviderWrapper
from datahub.metadata.schema_classes import AssertionRunEventClass
from tests.unit.external_dq._fixtures import (
    T0,
    FakeStateProvider,
    databricks_columns,
    result_raw,
    rule_raw,
)

RULES, RESULTS = "main.governance.dq_rules", "main.governance.dq_results"


class FakeReader:
    def __init__(
        self, results: List[Dict[str, Any]], fail_after: Optional[int] = None
    ) -> None:
        self.tables = {
            RULES: databricks_columns(RULES_COLUMNS),
            RESULTS: databricks_columns(RESULTS_COLUMNS),
        }
        self.rules = [rule_raw()]
        self.results = results
        self.fail_after = fail_after
        self.reads = 0
        self.count_error: Optional[Exception] = None

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

    def count_results_before(self, table: str, before_millis: int) -> int:
        if self.count_error is not None:
            raise self.count_error
        return sum(1 for r in self.results if r["executed_at"] < before_millis)


class Locator:
    def dataset_urn(self, dataset_path: Sequence[str]) -> Optional[str]:
        return make_dataset_urn("databricks", ".".join(dataset_path))

    def field_urn(self, dataset_urn: str, column_path: str) -> str:
        return make_schema_field_urn(dataset_urn, column_path)


def run(
    reader: FakeReader,
    state: Optional[ExternalDQStateHandler] = None,
    now: int = T0 + 1_000,
    **config_overrides: Any,
) -> tuple:
    source_report, report = SourceReport(), ExternalDQReport()
    config = ExternalDQConfig(
        enabled=True, rules_table=RULES, results_table=RESULTS, **config_overrides
    )
    mapper = ExternalDQMapper(
        platform="databricks",
        platform_instance=None,
        env="PROD",
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
        now_millis=lambda: now,
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


def test_in_run_duplicate_result_is_emitted_once() -> None:
    reader = FakeReader([result_raw(), result_raw()])
    _, run_events, _, report = run(reader)
    assert len(run_events) == 1
    assert report.results_already_emitted == 1


def test_second_run_emits_no_duplicate_run_events() -> None:
    rows = [result_raw()]
    first = FakeStateProvider()
    assert len(run(FakeReader(rows), _handler(first))[1]) == 1
    second = FakeStateProvider(last=first.current)
    _, run_events, _, report = run(FakeReader(rows), _handler(second))
    assert run_events == [] and report.results_already_emitted == 1


def test_raising_late_arrival_minutes_does_not_replay_published_results() -> None:
    rows = [
        result_raw(run_id="a", executed_at=T0 - 100 * 60_000),
        result_raw(run_id="b", executed_at=T0 - 70 * 60_000),
        result_raw(run_id="c"),
    ]
    first = FakeStateProvider()
    assert len(run(FakeReader(rows), _handler(first), late_arrival_minutes=60)[1]) == 3
    # The "arrived too late" warning tells operators to raise the window.
    second = FakeStateProvider(last=first.current)
    _, run_events, _, _ = run(
        FakeReader(rows), _handler(second), late_arrival_minutes=120
    )
    assert run_events == []


def test_watermark_is_capped_at_now_so_skewed_producers_keep_the_late_window() -> None:
    now = T0 + 1_000
    ahead = result_raw(run_id="ahead", executed_at=now + 30 * 60_000)
    first = FakeStateProvider()
    _, run_events, _, _ = run(FakeReader([ahead]), _handler(first))
    assert len(run_events) == 1
    assert _state(first).watermarks == {RESULTS: now}

    second = FakeStateProvider(last=first.current)
    late = result_raw(run_id="late", executed_at=now - 50 * 60_000)
    _, run_events, _, _ = run(FakeReader([ahead, late]), _handler(second))
    assert [e.metadata.aspect.runId for e in run_events] == ["late"]  # type: ignore[union-attr]


def test_first_run_window_never_extends_below_the_initial_lookback() -> None:
    lookback_start = T0 + 1_000 - 7 * 86_400_000
    first = FakeStateProvider()
    rows = [result_raw(run_id="in", executed_at=lookback_start + 10 * 60_000)]
    assert len(run(FakeReader(rows), _handler(first))[1]) == 1

    second = FakeStateProvider(last=first.current)
    rows.append(result_raw(run_id="older", executed_at=lookback_start - 20 * 60_000))
    _, run_events, _, report = run(FakeReader(rows), _handler(second))
    assert run_events == []
    assert report.results_missed_late == 1


def test_non_finite_float_result_is_skipped() -> None:
    _, run_events, _, report = run(FakeReader([result_raw(actual_value=float("nan"))]))
    assert run_events == [] and report.results_skipped_invalid == 1


def test_out_of_range_timestamp_is_skipped_not_fatal() -> None:
    # e.g. a producer that wrote microseconds where millis were expected.
    rows = [result_raw(executed_at=1_700_000_000_000_000)]
    _, run_events, _, report = run(FakeReader(rows))
    assert run_events == [] and report.results_skipped_invalid == 1


def test_read_failure_advances_only_to_emitted_rows() -> None:
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
    assert second.current.state.watermarks == {RESULTS: T0 + 5}  # type: ignore[attr-defined]


def test_future_dated_result_is_skipped_and_does_not_poison_watermark() -> None:
    state_provider = FakeStateProvider()
    rows = [
        result_raw(executed_at=T0 + 10**12),
        result_raw(run_id="run-2"),
    ]
    _, run_events, _, report = run(FakeReader(rows), _handler(state_provider))
    assert len(run_events) == 1
    assert report.results_skipped_future == 1
    assert state_provider.current is not None
    assert state_provider.current.state.watermarks == {RESULTS: T0}  # type: ignore[attr-defined]


def test_describe_failure_on_rules_carries_forward_watermark() -> None:
    first = FakeStateProvider()
    run(FakeReader([result_raw()]), _handler(first))
    second = FakeStateProvider(last=first.current)
    reader = FakeReader([result_raw()])
    original_describe = reader.describe

    def failing_describe(table: str) -> List[PhysicalColumn]:
        if table == RULES:
            raise ConnectionError("warehouse connection dropped")
        return original_describe(table)

    reader.describe = failing_describe  # type: ignore[method-assign]
    workunits, _, source_report, _ = run(reader, _handler(second))
    assert workunits == []
    assert source_report.failures
    assert second.committed_state().watermarks == {RESULTS: T0}


def test_rules_read_failure_carries_forward_watermark() -> None:
    first = FakeStateProvider()
    run(FakeReader([result_raw()]), _handler(first))
    second = FakeStateProvider(last=first.current)
    reader = FakeReader([result_raw()])

    def failing_read_rules(
        table: str, columns: Sequence[SelectColumn]
    ) -> Iterable[Mapping[str, Any]]:
        raise ConnectionError("warehouse connection dropped")

    reader.read_rules = failing_read_rules  # type: ignore[method-assign]
    _, run_events, source_report, _ = run(reader, _handler(second))
    assert run_events == []
    assert source_report.failures
    assert second.committed_state().watermarks == {RESULTS: T0}


OVERLAP_MS = ExternalDQConfig(enabled=False).late_arrival_minutes * 60_000


def _state(provider: FakeStateProvider) -> Any:
    assert provider.current is not None
    return provider.current.state


def test_retired_rule_results_are_recorded_but_not_published() -> None:
    first = FakeStateProvider()
    reader = FakeReader([result_raw()])
    reader.rules = [rule_raw(is_active=False)]
    _, run_events, _, report = run(reader, _handler(first))
    assert run_events == [] and report.results_skipped_retired == 1
    assert run_key("r1", "run-1") in _state(first).recent_keys[RESULTS]

    second = FakeStateProvider(last=first.current)
    reader = FakeReader([result_raw()])
    reader.rules = [rule_raw(is_active=False)]
    _, run_events, _, report = run(reader, _handler(second))
    assert run_events == [] and report.results_skipped_retired == 0


def test_unknown_rule_is_warned_once_per_rule() -> None:
    reader = FakeReader(
        [result_raw(rule_id="gone"), result_raw(rule_id="gone", run_id="run-2")]
    )
    _, run_events, source_report, _ = run(reader)
    assert run_events == []
    # SourceReport groups by (title, message), so the dedupe shows in the contexts.
    [entry] = [
        w
        for w in source_report.warnings
        if w.title == "External DQ results reference a rule that was not published"
    ]
    assert list(entry.context) == [f"{RESULTS}: rule_id=gone"]


HOUR = 3_600_000
DAY = 86_400_000


def _run_ids(run_events: List[MetadataWorkUnit]) -> List[str]:
    return [wu.metadata.aspect.runId for wu in run_events]  # type: ignore[union-attr]


def test_unresolved_rule_results_hold_the_window_until_the_rule_resolves() -> None:
    rows = [
        result_raw(),
        result_raw(rule_id="gone", run_id="g1", executed_at=T0 - 3 * HOUR),
    ]
    first = FakeStateProvider()
    _, run_events, _, _ = run(FakeReader(rows), _handler(first))
    assert _run_ids(run_events) == ["run-1"]

    # A later run lists the table again, so the rule resolves: the held result
    # is published once and the already-published one is not repeated.
    second = FakeStateProvider(last=first.current)
    reader = FakeReader(rows)
    reader.rules = [rule_raw(), rule_raw(rule_id="gone")]
    _, run_events, _, _ = run(reader, _handler(second))
    assert _run_ids(run_events) == ["g1"]


def test_results_for_invalid_or_duplicated_rule_rows_do_not_hold_the_window() -> None:
    # A rule row that is duplicated or fails validation is a producer error that
    # will not resolve on its own, unlike a table that was not listed this run.
    reader = FakeReader(
        [
            result_raw(rule_id="dup", run_id="d1", executed_at=T0 - 3 * HOUR),
            result_raw(rule_id="bad", run_id="b1", executed_at=T0 - 3 * HOUR),
            result_raw(),
        ]
    )
    reader.rules = [
        rule_raw(),
        rule_raw(rule_id="dup"),
        rule_raw(rule_id="dup", rule_name="copy"),
        rule_raw(rule_id="bad", rule_name="  "),
    ]
    first = FakeStateProvider()
    _, run_events, source_report, report = run(reader, _handler(first))
    assert _run_ids(run_events) == ["run-1"]
    assert report.results_skipped_invalid_rule == 2
    assert _state(first).next_starts == {RESULTS: T0 - 60 * 60_000}
    assert (
        "External DQ results skipped because their rule row is invalid or duplicated"
        in [w.title for w in source_report.warnings]
    )


def test_unresolved_hold_is_released_after_initial_lookback() -> None:
    rows = [
        result_raw(),
        result_raw(rule_id="gone", run_id="g1", executed_at=T0 - 3 * HOUR),
    ]
    first = FakeStateProvider()
    run(FakeReader(rows), _handler(first), initial_lookback_days=1)

    later = T0 + 1_000 + DAY
    rows.append(result_raw(run_id="run-2", executed_at=later - 1_000))
    second = FakeStateProvider(last=first.current)
    _, run_events, source_report, report = run(
        FakeReader(rows), _handler(second), now=later, initial_lookback_days=1
    )
    assert _run_ids(run_events) == ["run-2"]
    assert report.results_unresolved_expired == 1
    assert "Gave up waiting for an unpublished external DQ rule" in [
        w.title for w in source_report.warnings
    ]

    # The window has moved on, so the expired result is not read again.
    third = FakeStateProvider(last=second.current)
    reader = FakeReader(rows)
    reader.rules = [rule_raw(), rule_raw(rule_id="gone")]
    _, run_events, _, _ = run(reader, _handler(third), now=later)
    assert run_events == []


_LATE_TITLE = "External DQ results arrived too late to be read"


def test_late_result_is_detected_and_not_emitted() -> None:
    first = FakeStateProvider()
    run(FakeReader([result_raw()]), _handler(first))
    second = FakeStateProvider(last=first.current)
    late = result_raw(run_id="late", executed_at=T0 - OVERLAP_MS - 1)
    _, run_events, source_report, report = run(
        FakeReader([result_raw(), late]), _handler(second)
    )
    assert run_events == []
    assert report.results_missed_late == 1
    assert [w.title for w in source_report.warnings].count(_LATE_TITLE) == 1


def test_no_late_results_reports_nothing() -> None:
    first = FakeStateProvider()
    run(FakeReader([result_raw()]), _handler(first))
    second = FakeStateProvider(last=first.current)
    rows = [result_raw(), result_raw(run_id="run-2", executed_at=T0 + 5)]
    _, run_events, source_report, report = run(FakeReader(rows), _handler(second))
    assert len(run_events) == 1
    assert report.results_missed_late == 0
    assert _LATE_TITLE not in [w.title for w in source_report.warnings]


def test_late_count_failure_warns_and_still_ingests() -> None:
    reader = FakeReader([result_raw()])
    reader.count_error = ConnectionError("warehouse connection dropped")
    _, run_events, source_report, _ = run(reader, _handler(FakeStateProvider()))
    assert len(run_events) == 1
    assert not source_report.failures
    assert "Could not check for late external DQ results" in [
        w.title for w in source_report.warnings
    ]
