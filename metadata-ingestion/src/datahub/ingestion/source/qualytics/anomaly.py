"""Map Qualytics check results and anomalies onto DataHub assertion run events.

An assertion with no results is a claim with no evidence -- the Validation tab shows
the check exists but never says whether it passed. This module supplies the evidence,
from two sources that answer different questions:

* **The check's own state** (``is_passing`` / ``last_asserted``) is the current
  verdict. One event per check, and the only source of *passes* -- Qualytics records
  anomalies, not successes, so without this the timeline would be failures-only and
  every dataset would look permanently broken.
* **Anomalies in the configured window** are the failure history. Each anomaly names
  the checks it failed, with Qualytics' own message and the count of offending records.

Both are needed. The check state alone gives no history; the anomalies alone give no
passes.
"""

from collections.abc import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qualytics.models import Anomaly, QualityCheck
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport
from datahub.ingestion.source.qualytics.timeutil import parse_timestamp_millis
from datahub.metadata.schema_classes import (
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
)


class AnomalyMapper:
    """Builds assertion run events from Qualytics check state and anomalies."""

    def __init__(self, report: QualyticsSourceReport) -> None:
        self.report = report

    @staticmethod
    def _run_id(prefix: str, identifier: str | int) -> str:
        """Deterministic run id.

        DataHub keys a run event on (assertionUrn, timestampMillis, runId), so a random
        or per-run id would re-insert the same Qualytics result on every ingestion and
        pile up duplicates on the timeline.
        """
        return f"qualytics-{prefix}-{identifier}"

    def check_state_workunits(
        self,
        check: QualityCheck,
        assertion_urn: str,
        dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """The check's current verdict, as of its last assertion.

        Yields nothing when Qualytics has never evaluated the check, or reports a
        verdict without saying when: an undated result cannot be placed on a timeline,
        and stamping it with "now" would misdate it.
        """
        if check.is_passing is None:
            # Qualytics has never evaluated this check. Legitimate, but counted so it
            # is distinguishable in the report from a mapping bug.
            self.report.assertion_results_unevaluated += 1
            return

        timestamp = parse_timestamp_millis(check.last_asserted)
        if timestamp is None:
            self.report.assertion_results_undated += 1
            return

        result_type = (
            AssertionResultTypeClass.SUCCESS
            if check.is_passing
            else AssertionResultTypeClass.FAILURE
        )
        native: dict[str, str] = {"qualytics_check_id": str(check.id)}
        if check.active_anomaly_count:
            native["active_anomaly_count"] = str(check.active_anomaly_count)

        yield self._event(
            assertion_urn=assertion_urn,
            dataset_urn=dataset_urn,
            timestamp=timestamp,
            run_id=self._run_id("check", check.id),
            result=AssertionResultClass(type=result_type, nativeResults=native),
        )

    def anomaly_workunits(
        self,
        anomaly: Anomaly,
        assertion_urns: dict[int, str],
        dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Failure events for every check this anomaly breached.

        One anomaly can fail several checks, so it fans out. A check with no assertion
        in this run -- archived since, or unparseable -- is skipped and counted rather
        than attached to a guessed URN.
        """
        timestamp = parse_timestamp_millis(anomaly.created)
        if timestamp is None:
            self.report.assertion_results_undated += 1
            return

        for failed in anomaly.failed_checks:
            check_id = failed.quality_check.id
            assertion_urn = assertion_urns.get(check_id)
            if assertion_urn is None:
                self.report.anomalies_without_assertion += 1
                continue

            native = {
                "qualytics_anomaly_id": str(anomaly.id),
                "qualytics_anomaly_uuid": anomaly.uuid,
                "anomaly_type": anomaly.type,
                "status": anomaly.status,
            }
            if failed.message:
                native["message"] = failed.message
            if failed.suggested_value:
                native["suggested_value"] = failed.suggested_value

            yield self._event(
                assertion_urn=assertion_urn,
                dataset_urn=dataset_urn,
                timestamp=timestamp,
                # Keyed on both ids: one anomaly failing three checks is three events,
                # and they must not collide with each other.
                run_id=self._run_id("anomaly", f"{anomaly.id}-{check_id}"),
                result=AssertionResultClass(
                    type=AssertionResultTypeClass.FAILURE,
                    # Qualytics counts offending records, which is DataHub's
                    # unexpectedCount -- not rowCount, which means the batch size.
                    unexpectedCount=anomaly.anomalous_records_count,
                    nativeResults=native,
                ),
            )

    def _event(
        self,
        *,
        assertion_urn: str,
        dataset_urn: str,
        timestamp: int,
        run_id: str,
        result: AssertionResultClass,
    ) -> MetadataWorkUnit:
        self.report.assertion_results_emitted += 1
        return MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionRunEventClass(
                timestampMillis=timestamp,
                runId=run_id,
                asserteeUrn=dataset_urn,
                status=AssertionRunStatusClass.COMPLETE,
                assertionUrn=assertion_urn,
                result=result,
            ),
        ).as_workunit()
