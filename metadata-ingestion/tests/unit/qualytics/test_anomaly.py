"""Tests for Qualytics check results and anomalies -> DataHub assertion run events.

This mapper owns two report counters and four skip branches, none of which were
covered before. Each branch encodes a refusal to fabricate data, so the tests assert
the *absence* of output as much as its presence.
"""

from typing import Any

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qualytics.anomaly import AnomalyMapper
from datahub.ingestion.source.qualytics.models import Anomaly, QualityCheck
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport
from datahub.metadata.schema_classes import AssertionResultClass, AssertionRunEventClass

DATASET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES.PUBLIC.ORDERS,PROD)"
ASSERTION = "urn:li:assertion:abc123"
ASSERTION_B = "urn:li:assertion:def456"


def _event(wu: MetadataWorkUnit) -> AssertionRunEventClass:
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(wu.metadata.aspect, AssertionRunEventClass)
    return wu.metadata.aspect


def _result(wu: MetadataWorkUnit) -> AssertionResultClass:
    result = _event(wu).result
    assert result is not None
    return result


def _mapper() -> tuple[AnomalyMapper, QualyticsSourceReport]:
    report = QualyticsSourceReport()
    return AnomalyMapper(report), report


def _check(**overrides: Any) -> QualityCheck:
    return QualityCheck.model_validate({"id": 100, "rule_type": "notNull", **overrides})


def _anomaly(**overrides: Any) -> Anomaly:
    return Anomaly.model_validate(
        {
            "id": 900,
            "uuid": "abc-123",
            "type": "record",
            "status": "Active",
            "created": "2026-09-08T10:00:00Z",
            "datastore": {
                "id": 1,
                "name": "ds",
                "store_type": "jdbc",
                "type": "snowflake",
            },
            "container": {"id": 10, "name": "ORDERS"},
            **overrides,
        }
    )


# --- check state: the only source of passes ----------------------------------------


def test_a_passing_check_produces_a_success_event() -> None:
    # Qualytics records anomalies, not successes. Without this the timeline would be
    # failures-only and every dataset would look permanently broken.
    mapper, _ = _mapper()

    [wu] = mapper.check_state_workunits(
        _check(is_passing=True, last_asserted="2026-09-08T10:00:00Z"),
        ASSERTION,
        DATASET,
    )

    assert _result(wu).type == "SUCCESS"
    assert _event(wu).asserteeUrn == DATASET


def test_a_never_evaluated_check_produces_no_verdict() -> None:
    # is_passing is None means Qualytics has not run it. Emitting SUCCESS or FAILURE
    # would invent a result the platform never reported.
    mapper, report = _mapper()

    assert list(mapper.check_state_workunits(_check(), ASSERTION, DATASET)) == []
    assert report.assertion_results_unevaluated == 1
    assert report.assertion_results_emitted == 0


def test_an_undated_verdict_is_counted_rather_than_stamped_with_now() -> None:
    # A result with no timestamp cannot be placed on a timeline, and dating it to the
    # run would misreport when the check actually failed.
    mapper, report = _mapper()

    assert (
        list(
            mapper.check_state_workunits(
                _check(is_passing=False, last_asserted="not a date"), ASSERTION, DATASET
            )
        )
        == []
    )
    assert report.assertion_results_undated == 1


# --- anomalies: the failure history ------------------------------------------------


def test_an_anomaly_fans_out_to_every_check_it_failed_with_distinct_run_ids() -> None:
    # One anomaly can breach several checks. DataHub keys a run event on
    # (assertionUrn, timestampMillis, runId), so colliding run ids would silently
    # collapse three failures into one.
    mapper, _ = _mapper()
    anomaly = _anomaly(
        anomalous_records_count=17,
        failed_checks=[
            {"quality_check": {"id": 100, "rule_type": "notNull"}, "message": "a"},
            {"quality_check": {"id": 101, "rule_type": "unique"}, "message": "b"},
        ],
    )

    events = list(
        mapper.anomaly_workunits(anomaly, {100: ASSERTION, 101: ASSERTION_B}, DATASET)
    )

    assert len(events) == 2
    run_ids = {_event(e).runId for e in events}
    assert len(run_ids) == 2
    assert all(_result(e).unexpectedCount == 17 for e in events)


def test_run_ids_are_stable_across_runs() -> None:
    # An unstable id re-inserts the same Qualytics result on every ingestion and piles
    # up duplicates on the assertion timeline.
    first, _ = _mapper()
    second, _ = _mapper()
    anomaly = _anomaly(
        failed_checks=[
            {"quality_check": {"id": 100, "rule_type": "notNull"}, "message": "m"}
        ]
    )

    a = next(iter(first.anomaly_workunits(anomaly, {100: ASSERTION}, DATASET)))
    b = next(iter(second.anomaly_workunits(anomaly, {100: ASSERTION}, DATASET)))

    assert _event(a).runId == _event(b).runId


def test_an_anomaly_naming_a_check_with_no_assertion_is_counted_not_guessed() -> None:
    # Happens when container_pattern filtered the check out, or its container's URN
    # did not resolve. Attaching the result to an invented URN would be worse.
    mapper, report = _mapper()
    anomaly = _anomaly(
        failed_checks=[
            {"quality_check": {"id": 999, "rule_type": "notNull"}, "message": "m"}
        ]
    )

    events = list(mapper.anomaly_workunits(anomaly, {100: ASSERTION}, DATASET))

    assert events == []
    assert report.anomalies_without_assertion == 1


def test_an_undated_anomaly_emits_nothing_and_is_counted() -> None:
    mapper, report = _mapper()
    anomaly = _anomaly(
        created="not a date",
        failed_checks=[
            {"quality_check": {"id": 100, "rule_type": "notNull"}, "message": "m"}
        ],
    )

    events = list(mapper.anomaly_workunits(anomaly, {100: ASSERTION}, DATASET))

    assert events == []
    assert report.assertion_results_undated == 1


def test_the_qualytics_message_survives_onto_the_result() -> None:
    # Without it a DataHub user sees "failed" and has to go to Qualytics to learn why.
    mapper, _ = _mapper()
    anomaly = _anomaly(
        failed_checks=[
            {
                "quality_check": {"id": 100, "rule_type": "notNull"},
                "message": "17 rows had a null AMOUNT",
                "suggested_value": "0",
            }
        ]
    )

    event = next(iter(mapper.anomaly_workunits(anomaly, {100: ASSERTION}, DATASET)))

    native = _result(event).nativeResults or {}
    assert native["message"] == "17 rows had a null AMOUNT"
    assert native["suggested_value"] == "0"
    assert native["qualytics_anomaly_uuid"] == "abc-123"
