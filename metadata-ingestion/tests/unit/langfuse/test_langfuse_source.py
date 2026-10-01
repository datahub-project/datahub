from typing import Any, Dict, List, Optional, Set, cast
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import SecretStr, ValidationError

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
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    MLTrainingRunPropertiesClass,
    StatusClass,
    SubTypesClass,
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
    src._project_id = "proj-1"
    return src


def _serve_observations(
    source: LangfuseSource,
    roots: List[LangfuseObservation],
    generations: List[LangfuseObservation],
) -> None:
    """Mimics the server-side `isRootObservation` / `type` filters."""

    def iter_observations(
        *_: Any,
        observation_type: Optional[str] = None,
        is_root_observation: Optional[bool] = None,
    ) -> List[LangfuseObservation]:
        if is_root_observation:
            return roots
        if observation_type == "GENERATION":
            return generations
        raise AssertionError("Observations must be filtered on the server")

    _client(source).iter_observations.side_effect = iter_observations


def _aspects_for(workunits: List[MetadataWorkUnit], urn: str) -> Dict[str, List[Any]]:
    aspects: Dict[str, List[Any]] = {}
    for wu in workunits:
        mcpw = _mcpw(wu)
        if mcpw.entityUrn == urn:
            aspects.setdefault(type(mcpw.aspect).__name__, []).append(mcpw.aspect)
    return aspects


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
    def test_observations_filtered_on_server_and_root_generation_deduplicated(
        self, source: LangfuseSource
    ) -> None:
        root_generation = _obs(
            id="root-1", trace_id="t1", type="GENERATION", is_root_observation=True
        )
        _serve_observations(
            source,
            roots=[root_generation],
            # A root GENERATION is returned by both server-side queries.
            generations=[
                root_generation,
                _obs(id="gen-2", trace_id="t1", type="GENERATION"),
            ],
        )
        _client(source).iter_scores.return_value = []

        workunits = list(source._get_trace_workunits())

        call_kwargs = [
            c.kwargs for c in _client(source).iter_observations.call_args_list
        ]
        assert {"is_root_observation": True} in call_kwargs
        assert {"observation_type": "GENERATION"} in call_kwargs
        trace_props = next(
            p
            for p in _dpi_props(workunits)
            if p.customProperties.get("trace_id") == "t1"
            and "generation_count" in p.customProperties
        )
        assert trace_props.customProperties["generation_count"] == "2"
        assert source.report.traces_scanned == 1
        assert source.report.generations_scanned == 2

    def test_partial_trace_does_not_overwrite_full_trace_record(
        self, source: LangfuseSource
    ) -> None:
        # Regression: the trace root started before this run's window (an
        # earlier run already emitted the full Trace). Only a generation falls
        # inside the window; previously a trace record synthesized from that
        # generation overwrote the full Trace's properties and run events.
        _serve_observations(
            source,
            roots=[],
            generations=[
                _obs(
                    id="gen-1",
                    trace_id="t2",
                    type="GENERATION",
                    trace_name="checkout",
                    start_time="2026-01-02T00:00:00Z",
                )
            ],
        )
        _client(source).iter_scores.return_value = [
            LangfuseScore(
                id="s1",
                name="helpfulness",
                value=0.9,
                data_type="NUMERIC",
                timestamp="2026-01-02T00:00:00Z",
                subject_kind="trace",
                subject_id="t2",
            )
        ]

        workunits = list(source._get_trace_workunits())

        trace_aspects = _aspects_for(workunits, source._make_dpi_urn("t2"))
        assert DataProcessInstancePropertiesClass.__name__ not in trace_aspects
        assert DataProcessInstanceRunEventClass.__name__ not in trace_aspects
        assert MLTrainingRunPropertiesClass.__name__ not in trace_aspects
        assert SubTypesClass.__name__ in trace_aspects
        assert source.report.partial_traces == 1
        assert source.report.scores_attached == 0

        # The generation itself is still ingested and linked to its trace.
        generation_aspects = _aspects_for(workunits, source._make_dpi_urn("gen-1"))
        [relationships] = generation_aspects[
            DataProcessInstanceRelationshipsClass.__name__
        ]
        assert relationships.parentInstance == source._make_dpi_urn("t2")
        assert DataProcessInstancePropertiesClass.__name__ in generation_aspects

    def test_trace_name_pattern_filters_traces(self, source: LangfuseSource) -> None:
        source.config.trace_name_pattern = source.config.trace_name_pattern.__class__(
            deny=["^internal_.*"]
        )
        _serve_observations(
            source,
            roots=[
                _obs(
                    id="root-1",
                    trace_id="t1",
                    type="SPAN",
                    is_root_observation=True,
                    name="internal_healthcheck",
                ),
            ],
            generations=[],
        )
        _client(source).iter_scores.return_value = []

        workunits = list(source._get_trace_workunits())

        assert workunits == []
        assert source.report.traces_filtered == 1

    def test_dataprocessinstances_get_status_aspect(
        self, source: LangfuseSource
    ) -> None:
        # Regression: emitting DPI workunits with is_primary_source=False made
        # the pipeline skip their Status aspect. DPIs are already ignored by
        # stale-entity removal, so the flag was never needed.
        source.config.include_prompts = False
        _client(source).get_project.return_value = {"id": "proj-1", "name": "P"}
        _serve_observations(
            source,
            roots=[_obs(id="root-1", trace_id="t1", is_root_observation=True)],
            generations=[_obs(id="gen-1", trace_id="t1", type="GENERATION")],
        )
        _client(source).iter_scores.return_value = []

        workunits = list(source.get_workunits())

        urns_with_status: Set[str] = {
            str(_mcpw(wu).entityUrn)
            for wu in workunits
            if isinstance(_mcpw(wu).aspect, StatusClass)
        }
        assert source._make_dpi_urn("t1") in urns_with_status
        assert source._make_dpi_urn("gen-1") in urns_with_status


class TestDataProcessInstanceUrns:
    def test_same_native_id_does_not_collide_across_instances_or_projects(
        self, connection: LangfuseConnectionConfig
    ) -> None:
        def dpi_urn(platform_instance: Optional[str], project_id: str) -> str:
            config = LangfuseSourceConfig(
                connection=connection, platform_instance=platform_instance
            )
            with patch("datahub.ingestion.source.langfuse.langfuse.LangfuseClient"):
                src = LangfuseSource(
                    ctx=PipelineContext(run_id="langfuse-test"), config=config
                )
            src._project_id = project_id
            return src._make_dpi_urn("trace-1")

        urns = {
            dpi_urn("prod", "proj-1"),
            dpi_urn("staging", "proj-1"),
            dpi_urn("prod", "proj-2"),
            dpi_urn(None, "proj-1"),
        }
        assert len(urns) == 4
        assert dpi_urn("prod", "proj-1") == dpi_urn("prod", "proj-1")


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

        assert source.report.scores_dropped_unattachable_subject == 0

        [metric] = metrics_by_urn[source._make_dpi_urn("trace-1")]
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
        assert source.report.scores_dropped_unattachable_subject == 2

    def test_scores_attached_counts_only_emitted_scores(
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
                subject_id="t1",
            ),
            # Its trace is not in the window, so this score is never emitted.
            LangfuseScore(
                id="s2",
                name="helpfulness",
                value=0.1,
                data_type="NUMERIC",
                timestamp="2026-01-01T00:00:00Z",
                subject_kind="trace",
                subject_id="trace-outside-window",
            ),
        ]
        _serve_observations(
            source,
            roots=[_obs(id="root-1", trace_id="t1", is_root_observation=True)],
            generations=[],
        )

        workunits = list(source._get_trace_workunits())

        [training_run] = _aspects_for(workunits, source._make_dpi_urn("t1"))[
            MLTrainingRunPropertiesClass.__name__
        ]
        assert [m.name for m in training_run.trainingMetrics or []] == ["helpfulness"]
        assert source.report.scores_attached == 1


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
        # Any RequestException (not only HTTPError) skips just that version.
        _client(source).get_prompt_version.side_effect = requests.ConnectionError(
            "boom"
        )

        workunits = list(source._get_prompt_workunits())

        assert workunits == []
        assert source.report.warnings
        assert not source.report.failures

    def test_prompt_listing_failure_is_reported_as_failure_not_raised(
        self, source: LangfuseSource
    ) -> None:
        def failing_listing() -> Any:
            yield {"name": "greeting", "versions": []}
            raise requests.ConnectionError("connection reset mid-pagination")

        _client(source).iter_prompt_names.side_effect = failing_listing

        workunits = list(source._get_prompt_workunits())

        assert workunits == []
        # A failure (not a warning) so stale-entity removal does not
        # soft-delete prompts that simply were not listed.
        assert source.report.failures

    def test_version_set_urn_is_scoped_to_platform_instance(
        self, connection: LangfuseConnectionConfig
    ) -> None:
        def version_set_urn(platform_instance: Optional[str]) -> str:
            config = LangfuseSourceConfig(
                connection=connection, platform_instance=platform_instance
            )
            with patch("datahub.ingestion.source.langfuse.langfuse.LangfuseClient"):
                src = LangfuseSource(
                    ctx=PipelineContext(run_id="langfuse-test"), config=config
                )
            return str(src._get_prompt_version_set_urn("greeting"))

        assert version_set_urn("prod") != version_set_urn("staging")
        assert version_set_urn("prod") != version_set_urn(None)


class TestPromptLineage:
    def test_generation_input_points_at_emitted_prompt_dataset(
        self, source: LangfuseSource
    ) -> None:
        source.config.platform_instance = "prod"
        prompt = LangfusePromptVersion(
            name="greeting",
            version=2,
            prompt_type="text",
            prompt="Hello {{name}}",
            config=None,
        )
        prompt_dataset_urn = next(
            str(_mcpw(wu).entityUrn)
            for wu in source._emit_prompt_version(
                prompt, source._get_prompt_version_set_urn("greeting")
            )
        )
        generation = _obs(
            id="gen-1",
            trace_id="t1",
            type="GENERATION",
            prompt_name="greeting",
            prompt_version=2,
        )

        workunits = list(
            source._emit_generation(generation, source._make_dpi_urn("t1"), {})
        )

        [dpi_input] = _aspects_for(workunits, source._make_dpi_urn("gen-1"))[
            DataProcessInstanceInputClass.__name__
        ]
        assert dpi_input.inputs == [prompt_dataset_urn]

    def test_no_input_for_prompt_excluded_by_prompt_pattern(
        self, source: LangfuseSource
    ) -> None:
        source.config.prompt_pattern = source.config.prompt_pattern.__class__(
            deny=["^greeting$"]
        )
        generation = _obs(
            id="gen-1",
            trace_id="t1",
            type="GENERATION",
            prompt_name="greeting",
            prompt_version=2,
        )

        workunits = list(
            source._emit_generation(generation, source._make_dpi_urn("t1"), {})
        )

        aspects = _aspects_for(workunits, source._make_dpi_urn("gen-1"))
        assert DataProcessInstanceInputClass.__name__ not in aspects


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
        _serve_observations(
            source,
            roots=[
                _obs(id="root-1", trace_id="t1", type="SPAN", is_root_observation=True)
            ],
            generations=[],
        )

        # Scores fail, but trace ingestion should still proceed with no metrics.
        workunits = list(source._get_trace_workunits())

        assert len(workunits) > 0
        assert source.report.failures
