from typing import Any, List, cast
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import SecretStr, ValidationError

from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.langfuse.langfuse import LangfuseSource, _iso_to_millis
from datahub.ingestion.source.langfuse.langfuse_client import (
    LangfuseAuthenticationError,
    LangfuseObservation,
    LangfusePromptVersion,
    LangfuseScore,
)
from datahub.ingestion.source.langfuse.langfuse_config import (
    LangfuseConnectionConfig,
    LangfuseSourceConfig,
)
from datahub.metadata.schema_classes import (
    DataProcessInstancePropertiesClass,
    VersionPropertiesClass,
)


def _client(source: LangfuseSource) -> MagicMock:
    return cast(MagicMock, source.client)


def _mcpw(wu: MetadataWorkUnit) -> MetadataChangeProposalWrapper:
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    return wu.metadata


def _dpi_props(
    workunits: List[MetadataWorkUnit],
) -> List[DataProcessInstancePropertiesClass]:
    return [
        aspect
        for wu in workunits
        for aspect in [_mcpw(wu).aspect]
        if isinstance(aspect, DataProcessInstancePropertiesClass)
    ]


@pytest.fixture
def connection() -> LangfuseConnectionConfig:
    return LangfuseConnectionConfig(
        host="http://langfuse.test",
        public_key="pk-test",
        secret_key=SecretStr("sk-test"),
    )


@pytest.fixture
def source(connection: LangfuseConnectionConfig) -> LangfuseSource:
    config = LangfuseSourceConfig(connection=connection)
    with patch("datahub.ingestion.source.langfuse.langfuse.LangfuseClient"):
        src = LangfuseSource(ctx=PipelineContext(run_id="langfuse-test"), config=config)
    src.client = MagicMock()
    src._project_container_urn = "urn:li:container:test-project"
    return src


def _obs(**kwargs: Any) -> LangfuseObservation:
    defaults: dict[str, Any] = {
        "id": "obs-default",
        "trace_id": "trace-default",
        "type": "SPAN",
        "is_root_observation": False,
        "start_time": "2026-01-01T00:00:00Z",
        "end_time": "2026-01-01T00:00:01Z",
    }
    defaults.update(kwargs)
    return LangfuseObservation(**defaults)


class TestConfigValidation:
    def test_page_size_out_of_range_rejected(
        self, connection: LangfuseConnectionConfig
    ) -> None:
        with pytest.raises(ValidationError):
            LangfuseSourceConfig(connection=connection, page_size=0)
        with pytest.raises(ValidationError):
            LangfuseSourceConfig(connection=connection, page_size=1001)

    def test_host_without_scheme_rejected(self) -> None:
        with pytest.raises(ValidationError):
            LangfuseConnectionConfig(
                host="localhost:3000",
                public_key="pk-test",
                secret_key=SecretStr("sk-test"),
            )

    def test_default_window_spans_approximately_seven_days(
        self, connection: LangfuseConnectionConfig
    ) -> None:
        # Regression test: BaseTimeWindowConfig's own bare default is only
        # ~1 day (one bucket_duration back from now, floored to midnight),
        # not 7 days. LangfuseSourceConfig must pass start_time="-7d"
        # explicitly to get the documented/required default window.
        config = LangfuseSourceConfig(connection=connection)
        span = config.window.end_time - config.window.start_time
        assert span.days >= 6

    def test_scores_require_traces(self, connection: LangfuseConnectionConfig) -> None:
        with pytest.raises(ValidationError):
            LangfuseSourceConfig(
                connection=connection, include_traces=False, include_scores=True
            )

    def test_scores_allowed_when_traces_enabled(
        self, connection: LangfuseConnectionConfig
    ) -> None:
        config = LangfuseSourceConfig(
            connection=connection, include_traces=True, include_scores=True
        )
        assert config.include_scores is True


class TestIsoToMillis:
    def test_parses_zulu_suffix(self) -> None:
        assert _iso_to_millis("2026-01-01T00:00:00Z") == 1767225600000

    def test_returns_none_for_missing_value(self) -> None:
        assert _iso_to_millis(None) is None

    def test_returns_none_for_unparseable_value(self) -> None:
        assert _iso_to_millis("not-a-date") is None


class TestTraceReconstruction:
    def test_generation_and_non_generation_counts_are_split_correctly(
        self, source: LangfuseSource
    ) -> None:
        _client(source).iter_observations.return_value = [
            _obs(id="root-1", trace_id="t1", type="SPAN", is_root_observation=True),
            _obs(id="gen-1", trace_id="t1", type="GENERATION"),
            _obs(id="gen-2", trace_id="t1", type="GENERATION"),
            _obs(id="event-1", trace_id="t1", type="EVENT"),
        ]
        _client(source).iter_scores.return_value = []

        workunits = list(source._get_trace_workunits())

        props = _dpi_props(workunits)
        trace_props = next(p for p in props if p.customProperties["trace_id"] == "t1")
        assert trace_props.customProperties["generation_count"] == "2"
        assert trace_props.customProperties["non_generation_observation_count"] == "2"
        assert "partial_trace" not in trace_props.customProperties
        assert source.report.traces_scanned == 1
        assert source.report.generations_scanned == 2
        # Regression: this counter was declared but never incremented.
        assert source.report.non_generation_observations_skipped == 2

    def test_missing_root_observation_produces_partial_trace(
        self, source: LangfuseSource
    ) -> None:
        # Root observation started before the configured window; only a
        # generation-type child observation falls inside it.
        _client(source).iter_observations.return_value = [
            _obs(
                id="gen-1", trace_id="t2", type="GENERATION", is_root_observation=False
            ),
        ]
        _client(source).iter_scores.return_value = []

        workunits = list(source._get_trace_workunits())

        dpi_props = _dpi_props(workunits)
        trace_props = next(
            p for p in dpi_props if p.customProperties.get("trace_id") == "t2"
        )
        assert trace_props.customProperties["partial_trace"] == "true"

    def test_trace_name_pattern_filters_traces(self, source: LangfuseSource) -> None:
        source.config.trace_name_pattern = source.config.trace_name_pattern.__class__(
            deny=["^internal_.*"]
        )
        _client(source).iter_observations.return_value = [
            _obs(
                id="root-1",
                trace_id="t1",
                type="SPAN",
                is_root_observation=True,
                name="internal_healthcheck",
            ),
        ]
        _client(source).iter_scores.return_value = []

        workunits = list(source._get_trace_workunits())

        assert workunits == []
        assert source.report.traces_filtered == 1

    def test_all_dataprocessinstance_workunits_excluded_from_stale_removal(
        self, source: LangfuseSource
    ) -> None:
        # Trace/Generation workunits must never be considered by stale-entity
        # removal, since they are retrieved through a rolling window, not
        # fully enumerated.
        _client(source).iter_observations.return_value = [
            _obs(id="root-1", trace_id="t1", type="SPAN", is_root_observation=True),
            _obs(id="gen-1", trace_id="t1", type="GENERATION"),
        ]
        _client(source).iter_scores.return_value = []

        workunits = list(source._get_trace_workunits())

        assert len(workunits) > 0
        assert all(not wu.is_primary_source for wu in workunits)


class TestScoreAttachment:
    def test_trace_and_observation_scores_are_attached_with_data_type_preserved(
        self, source: LangfuseSource
    ) -> None:
        _client(source).iter_scores.return_value = [
            LangfuseScore(
                id="s1",
                name="helpfulness",
                value=0.9,
                data_type="NUMERIC",
                timestamp="2026-01-01T00:00:00Z",
                subject_kind="trace",
                subject_id="trace-1",
            ),
            LangfuseScore(
                id="s2",
                name="is_correct",
                value=True,
                data_type="BOOLEAN",
                timestamp="2026-01-01T00:00:00Z",
                subject_kind="observation",
                subject_id="obs-1",
            ),
        ]

        metrics_by_urn = source._build_score_metrics_map(
            source.config.window.start_time, source.config.window.end_time
        )

        assert source.report.scores_attached == 2
        assert source.report.scores_dropped_unattachable_subject == 0

        trace_urn = str(
            DataProcessInstance(id="trace-1", orchestrator=source.platform).urn
        )
        [metric] = metrics_by_urn[trace_urn]
        assert metric.name == "helpfulness"
        assert metric.value == "0.9"
        assert metric.description is not None
        assert "NUMERIC" in metric.description

    def test_session_and_experiment_scores_are_dropped_not_attached(
        self, source: LangfuseSource
    ) -> None:
        _client(source).iter_scores.return_value = [
            LangfuseScore(
                id="s3",
                name="engagement",
                value=1.0,
                data_type="NUMERIC",
                timestamp="2026-01-01T00:00:00Z",
                subject_kind="session",
                subject_id="session-1",
            ),
            LangfuseScore(
                id="s4",
                name="eval_score",
                value=1.0,
                data_type="NUMERIC",
                timestamp="2026-01-01T00:00:00Z",
                subject_kind="experiment",
                subject_id="run-1",
            ),
        ]

        metrics_by_urn = source._build_score_metrics_map(
            source.config.window.start_time, source.config.window.end_time
        )

        assert metrics_by_urn == {}
        assert source.report.scores_attached == 0
        assert source.report.scores_dropped_unattachable_subject == 2


class TestPromptVersionEmission:
    def test_emits_versioned_dataset_with_labels_as_aliases(
        self, source: LangfuseSource
    ) -> None:
        prompt = LangfusePromptVersion(
            name="greeting",
            version=2,
            prompt_type="text",
            prompt="Hello {{name}}",
            config={"temperature": 0.7},
            labels=["production", "latest"],
            tags=["customer-facing"],
        )
        version_set_urn = source._get_prompt_version_set_urn("greeting")

        workunits: List[MetadataWorkUnit] = list(
            source._emit_prompt_version(prompt, version_set_urn)
        )

        version_workunits = [
            wu
            for wu in workunits
            if isinstance(_mcpw(wu).aspect, VersionPropertiesClass)
        ]
        version_props = version_workunits[0].metadata
        assert isinstance(version_props, MetadataChangeProposalWrapper)
        assert isinstance(version_props.aspect, VersionPropertiesClass)
        assert version_props.aspect.version.versionTag == "2"
        assert version_props.aspect.sortId == "0000000002"
        assert {a.versionTag for a in version_props.aspect.aliases} == {
            "production",
            "latest",
        }
        assert "greeting.v2" in str(_mcpw(version_workunits[0]).entityUrn)

    def test_prompt_fetch_failure_is_skipped_not_fatal(
        self, source: LangfuseSource
    ) -> None:
        _client(source).iter_prompt_names.return_value = [
            {"name": "broken-prompt", "versions": [1]}
        ]
        _client(source).get_prompt_version.side_effect = requests.HTTPError("boom")

        workunits = list(source._get_prompt_workunits())

        assert workunits == []
        assert source.report.warnings


class TestConnection:
    def test_connection_success(self, connection: LangfuseConnectionConfig) -> None:
        with patch(
            "datahub.ingestion.source.langfuse.langfuse.LangfuseClient"
        ) as client_cls:
            client = client_cls.return_value
            client.get_health.return_value = {"status": "OK", "version": "4.38.0"}
            client.get_project.return_value = {"id": "proj-1", "name": "Test"}
            client.iter_prompt_names.return_value = iter([])

            report = LangfuseSource.test_connection(
                {
                    "connection": {
                        "host": "http://langfuse.test",
                        "public_key": "pk-test",
                        "secret_key": "sk-test",
                    }
                }
            )

        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True

    def test_connection_failure_on_bad_credentials(
        self, connection: LangfuseConnectionConfig
    ) -> None:
        with patch(
            "datahub.ingestion.source.langfuse.langfuse.LangfuseClient"
        ) as client_cls:
            client = client_cls.return_value
            client.get_health.side_effect = LangfuseAuthenticationError("bad creds")

            report = LangfuseSource.test_connection(
                {
                    "connection": {
                        "host": "http://langfuse.test",
                        "public_key": "pk-test",
                        "secret_key": "sk-test",
                    }
                }
            )

        assert report.basic_connectivity == CapabilityReport(
            capable=False, failure_reason="bad creds"
        )


class TestGracefulFailureHandling:
    """A failure mid-ingestion must be reported and the run must stop
    cleanly, not crash with an unhandled traceback."""

    def test_project_auth_failure_is_reported_not_raised(
        self, source: LangfuseSource
    ) -> None:
        _client(source).get_project.side_effect = LangfuseAuthenticationError(
            "bad creds"
        )

        workunits = list(source.get_workunits_internal())

        assert workunits == []
        assert source.report.failures

    def test_project_connection_failure_is_reported_not_raised(
        self, source: LangfuseSource
    ) -> None:
        _client(source).get_project.side_effect = requests.ConnectionError("refused")

        workunits = list(source.get_workunits_internal())

        assert workunits == []
        assert source.report.failures

    def test_observation_fetch_failure_is_reported_not_raised(
        self, source: LangfuseSource
    ) -> None:
        _client(source).iter_observations.side_effect = requests.ConnectionError("boom")
        _client(source).iter_scores.return_value = []

        workunits = list(source._get_trace_workunits())

        assert workunits == []
        assert source.report.failures

    def test_score_fetch_failure_is_reported_and_trace_ingestion_continues(
        self, source: LangfuseSource
    ) -> None:
        _client(source).iter_scores.side_effect = requests.ConnectionError("boom")
        _client(source).iter_observations.return_value = [
            _obs(id="root-1", trace_id="t1", type="SPAN", is_root_observation=True),
        ]

        # Scores fail, but trace ingestion should still proceed with no metrics.
        workunits = list(source._get_trace_workunits())

        assert len(workunits) > 0
        assert source.report.failures
