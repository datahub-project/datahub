import logging
from contextlib import AbstractContextManager, ExitStack
from datetime import timedelta
from typing import Callable, Dict, List, Optional, Tuple, Type, TypeVar, cast
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from google.api_core.exceptions import NotFound, PermissionDenied, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.cloud.aiplatform import ExperimentRun, PipelineJob, initializer
from google.cloud.aiplatform.metadata import constants as metadata_constants
from google.cloud.aiplatform.metadata.artifact import Artifact as MetadataArtifact
from google.cloud.aiplatform.metadata.context import Context as MetadataContext
from google.cloud.aiplatform.metadata.execution import Execution as MetadataExecution
from google.cloud.aiplatform.metadata.experiment_resources import Experiment
from google.cloud.aiplatform_v1.services.metadata_service import MetadataServiceClient
from google.cloud.aiplatform_v1.services.tensorboard_service import (
    TensorboardServiceClient,
)
from google.cloud.aiplatform_v1.types import (
    artifact as gca_artifact,
    context as gca_context,
    execution as gca_execution,
    pipeline_job as gca_pipeline_job,
)

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.vertexai.vertexai import VertexAISource
from datahub.ingestion.source.vertexai.vertexai_constants import (
    LEGACY_RUN_EXECUTIONS,
    LEGACY_RUN_EXECUTIONS_TITLE,
    LOCATIONS_SKIPPED_TITLE,
    REGION_DISCOVERY_FAILED,
    REGION_UNCHECKED_AFTER_DISCOVERY,
    REGION_UNCHECKED_AFTER_DISCOVERY_NONE_READ,
    REGION_UNCHECKED_TITLE,
    UNEXPECTED_DETAIL_ERROR,
    UNEXPECTED_ERROR,
    UNSCOPED_KEPT,
    UNSCOPED_KEPT_TITLE,
    DetailCalls,
    ResourceTypes,
)
from datahub.ingestion.source.vertexai.vertexai_scope import (
    ErrorKind,
    Scope,
    error_title,
)
from datahub.ingestion.source.vertexai.vertexai_state import (
    VertexAICheckpointState,
    VertexAIStateHandler,
)
from datahub.metadata.schema_classes import (
    DataProcessInstancePropertiesClass,
    MLTrainingRunPropertiesClass,
)
from tests.integration.vertexai.mock_vertexai import (
    MOCK_CREATE_TIME,
    gen_mock_experiment,
    gen_mock_experiment_run,
    gen_mock_model,
    gen_mock_model_evaluation,
    get_mock_pipeline_job,
)
from tests.unit.vertexai.test_vertexai_failure_scope import (
    _CONTAINER_URN,
    _EVERY_URN,
    _FLOW_URNS,
    _PROJECT_CONTAINER_URN,
    _UNRELATED_URNS,
    REGION,
    _aspects,
    _experiment_source,
    _iam_denied,
    _legacy_mock_run,
    _locations_client,
    _mock,
    _model_source,
    _pipeline_source,
    _read_experiment_runs,
    _read_models,
    _read_pipelines,
    _real_carry,
    _region_source,
    _run_regions,
    _service_disabled,
    _throttled,
    _warning_contexts,
    _written,
)

_EXPERIMENT_MODULE = "datahub.ingestion.source.vertexai.vertexai_experiment_extractor"


R = TypeVar("R", MetadataContext, MetadataExecution, MetadataArtifact)


def _anonymous_credentials() -> AbstractContextManager[object]:
    return patch.object(
        type(initializer.global_config),
        "credentials",
        new_callable=PropertyMock,
        return_value=AnonymousCredentials(),
    )


def _sdk_resource(cls: Type[R], proto: object) -> R:
    # Building an SDK resource asks for credentials, and none exist offline.
    with _anonymous_credentials():
        return cast(R, cls._construct_sdk_resource_from_gapic(proto))


def _store_path(kind: str, name: str) -> str:
    return f"projects/123/locations/{REGION}/metadataStores/default/{kind}/{name}"


def _run_node(display_name: str) -> MetadataContext:
    return _sdk_resource(
        MetadataContext,
        gca_context.Context(
            name=_store_path("contexts", display_name),
            display_name=display_name,
            schema_title="system.ExperimentRun",
            metadata={},
        ),
    )


def _legacy_node(
    display_name: str,
    metadata: Optional[Dict[str, object]] = None,
    resource_id: Optional[str] = None,
) -> MetadataExecution:
    fields = {} if metadata is None else {"metadata": metadata}
    return _sdk_resource(
        MetadataExecution,
        gca_execution.Execution(
            name=_store_path("executions", resource_id or display_name),
            display_name=display_name,
            schema_title="system.Run",
            **fields,
        ),
    )


def _metrics_artifact(metrics: Optional[Dict[str, float]] = None) -> MetadataArtifact:
    fields = {} if metrics is None else {"metadata": metrics}
    return _sdk_resource(
        MetadataArtifact,
        gca_artifact.Artifact(
            name=_store_path("artifacts", "m"),
            schema_title="system.Metrics",
            **fields,
        ),
    )


_WRITE_PREFIXES = ("create_", "update_", "add_", "batch_create_", "write_")
_READ_PREFIX = "get_"


def _sdk_experiment_with_backing_tensorboard() -> Experiment:
    experiment = Experiment.__new__(Experiment)
    experiment._metadata_context = _sdk_resource(
        MetadataContext,
        gca_context.Context(
            name=_store_path("contexts", "exp"),
            display_name="exp",
            schema_title="system.Experiment",
            metadata={
                metadata_constants._BACKING_TENSORBOARD_RESOURCE_KEY: (
                    f"projects/123/locations/{REGION}/tensorboards/1"
                )
            },
        ),
    )
    return experiment


def test_experiment_run_init_never_writes_to_tensorboard_or_metadata() -> None:
    source = _experiment_source()
    fired: List[str] = []

    def tripwire(name: str) -> Callable[..., None]:
        def fire(*a: object, **k: object) -> None:
            fired.append(name)
            raise AssertionError(name)

        return fire

    def not_found(*a: object, **k: object) -> None:
        raise NotFound("absent")

    experiment = _sdk_experiment_with_backing_tensorboard()
    with ExitStack() as stack:
        # Reads find no TensorBoard run, so a run that looked would go on to write.
        for client in (TensorboardServiceClient, MetadataServiceClient):
            for name in dir(client):
                if name.startswith(_WRITE_PREFIXES):
                    stack.enter_context(
                        patch.object(
                            client, name, tripwire(f"{client.__name__}.{name}")
                        )
                    )
                elif name.startswith(_READ_PREFIX):
                    stack.enter_context(patch.object(client, name, not_found))
        stack.enter_context(_anonymous_credentials())
        runs = _list_runs_from(source, [_run_node("run-1")], [], experiment)

        with (
            patch.object(ExperimentRun, "get_executions", return_value=[]),
            patch.object(ExperimentRun, "get_logged_custom_jobs", return_value=[]),
        ):
            workunits = _read_experiment_runs(source, runs)

    assert fired == []
    assert [r._run_name for r in runs] == ["run-1"]
    assert _aspects(workunits, DataProcessInstancePropertiesClass)


def test_run_init_unexpected_error_skips_only_that_run() -> None:
    source = _experiment_source()
    error = RuntimeError("boom")

    def init(
        self: ExperimentRun, node: MetadataContext, *a: object, **k: object
    ) -> None:
        if node.display_name == "run-1":
            raise error
        self._metadata_node = node

    nodes = [_run_node("run-1"), _run_node("run-2")]
    with patch.object(ExperimentRun, "_initialize_experiment_run", init):
        runs = _list_runs_from(source, nodes, [])

    assert len(runs) == 1
    assert runs[0]._metadata_node is nodes[1]
    assert [w.title for w in source.report.warnings] == [
        error_title(error, "Detail could not be read")
    ]
    assert any(
        f"item={gen_mock_experiment().name}/run-1" in c
        for c in _warning_contexts(source)
    )
    assert not source.report.failures


def _metrics_getter(
    failure: Exception, failing_run: str, metrics: Dict[str, float]
) -> Callable[[ExperimentRun], MetadataArtifact]:
    def get(self: ExperimentRun) -> MetadataArtifact:
        if self._metadata_node.display_name == failing_run:
            raise failure
        return _metrics_artifact(metrics)

    return get


def _run_legacy_metrics_read(
    failure: Exception,
) -> Tuple[VertexAISource, List[ExperimentRun]]:
    source = _experiment_source()
    nodes = [_legacy_node("run-1", {"lr": 0.1}), _legacy_node("run-2", {"lr": 0.1})]
    with patch.object(
        ExperimentRun,
        "_v1_get_metric_artifact",
        _metrics_getter(failure, "run-1", {"acc": 0.9}),
    ):
        return source, _list_runs_from(source, [], nodes)


def _list_runs_from(
    source: VertexAISource,
    contexts: List[MetadataContext],
    executions: List[MetadataExecution],
    experiment: Optional[Experiment] = None,
) -> List[ExperimentRun]:
    extractor = source.experiment_extractor
    with (
        patch(
            f"{_EXPERIMENT_MODULE}.rate_limited_gapic_list",
            side_effect=lambda cls, *a, **k: (
                contexts if cls is MetadataContext else executions
            ),
        ),
        patch.object(extractor, "_metadata_store_parent", return_value="p"),
    ):
        return extractor._list_experiment_runs_rate_limited(
            experiment or gen_mock_experiment()
        )


def _assert_first_written_without_metrics(
    source: VertexAISource, runs: List[ExperimentRun], title: str
) -> None:
    assert [r._run_name for r in runs] == ["run-1", "run-2"]
    extractor = source.experiment_extractor
    assert extractor._read_run_state(runs[0]).metrics == []
    assert [p.name for p in extractor._read_run_state(runs[0]).params] == ["lr"]
    assert [m.name for m in extractor._read_run_state(runs[1]).metrics] == ["acc"]
    assert [w.title for w in source.report.warnings] == [title]
    assert any(
        f"item={gen_mock_experiment().name}/run-1" in c
        for c in _warning_contexts(source)
    )
    assert not source.report.failures


@pytest.mark.parametrize(
    "error,title",
    [
        (NotFound("no metrics artifact"), "Detail could not be read"),
        (PermissionDenied("denied"), "Detail read refused"),
    ],
)
def test_legacy_metrics_lost_written_without_metrics(
    error: Exception, title: str
) -> None:
    source, runs = _run_legacy_metrics_read(error)
    _assert_first_written_without_metrics(source, runs, title)


def test_legacy_metrics_temporary_error_keeps_the_stored_run() -> None:
    source = _experiment_source()
    legacy = [_legacy_node("run-1", {"lr": 0.1})]
    current = [_run_node("run-2")]
    with patch.object(
        ExperimentRun,
        "_v1_get_metric_artifact",
        _metrics_getter(ServiceUnavailable("down"), "run-1", {}),
    ):
        runs = _list_runs_from(source, current, legacy)

    assert [r._run_name for r in runs] == ["run-2"]
    assert not source.report.failures


def test_stopped_legacy_metrics_does_not_stop_current_runs() -> None:
    source = _experiment_source()
    with patch.object(
        ExperimentRun,
        "_v1_get_metric_artifact",
        _metrics_getter(ServiceUnavailable("down"), "run-1", {}),
    ):
        legacy = _list_runs_from(source, [], [_legacy_node("run-1", {"lr": 0.1})])
    assert legacy == []

    runs = _list_runs_from(source, [_run_node("run-2")], [])

    assert [r._run_name for r in runs] == ["run-2"]


def test_legacy_run_without_metrics_is_named_by_display_name() -> None:
    source = _experiment_source()
    source._stopped.stop(
        Scope(
            project="proj-one",
            region=REGION,
            kind=DetailCalls.EXPERIMENT_RUN_LEGACY_METRICS,
        ),
        ErrorKind.REFUSED,
        PermissionDenied("denied"),
    )
    # The SDK names a legacy execution '{experiment}-{run}'; the run is 'legacy-1'.
    node = _legacy_node("legacy-1", {"lr": 0.1}, resource_id="exp-one-legacy-1")

    (run,) = _list_runs_from(source, [], [node])

    assert node.name == "exp-one-legacy-1"
    assert run._run_name == "legacy-1"
    state = source.experiment_extractor._read_run_state(run)
    assert [p.name for p in state.params] == ["lr"]


def test_legacy_run_without_metrics_has_the_fields_the_sdk_sets() -> None:
    source = _experiment_source()
    node = _legacy_node("run-1", {"lr": 0.1})
    experiment = gen_mock_experiment()
    built = ExperimentRun.__new__(ExperimentRun)
    source.experiment_extractor._set_run_without_metrics(built, node, experiment)

    real = ExperimentRun.__new__(ExperimentRun)
    with patch.object(
        ExperimentRun, "_v1_get_metric_artifact", return_value=_metrics_artifact({})
    ):
        real._initialize_experiment_run(node, experiment, lookup_tensorboard_run=False)

    assert vars(built).keys() == vars(real).keys()


def test_legacy_runs_share_one_info_and_record_no_loss(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = _experiment_source()
    legacy = [_legacy_mock_run(), _legacy_mock_run()]
    _mock(legacy[1]).name = "mock_experiment_run_2"
    current = gen_mock_experiment_run()
    _mock(current).name = "mock_experiment_run_3"

    with caplog.at_level(logging.INFO):
        workunits = _read_experiment_runs(source, [*legacy, current])

    for run in legacy:
        _mock(run.get_executions).assert_not_called()
        _mock(run.get_logged_custom_jobs).assert_not_called()
    _mock(current.get_logged_custom_jobs).assert_called()
    assert (
        len({wu.get_urn() for wu in workunits if "experiment_run" in wu.get_urn()}) == 3
    )
    assert not source.report.warnings
    (legacy_info,) = [
        i for i in source.report.infos if i.title == LEGACY_RUN_EXECUTIONS_TITLE
    ]
    assert len(legacy_info.context) == 2
    assert (
        len(
            [r for r in caplog.records if LEGACY_RUN_EXECUTIONS_TITLE in r.getMessage()]
        )
        == 1
    )
    assert "Report it" not in LEGACY_RUN_EXECUTIONS
    assert not source._stopped.lost_in("proj-one")
    assert not source.report.failures


def test_legacy_run_init_is_rate_limited_and_current_is_not() -> None:
    source = _experiment_source()
    limiter = MagicMock()
    source.experiment_extractor.rate_limiter = limiter

    def init(self: ExperimentRun, node: object, *a: object, **k: object) -> None:
        self._metadata_node = node  # type: ignore[assignment]

    with patch.object(ExperimentRun, "_initialize_experiment_run", init):
        _list_runs_from(source, [_run_node("run-1")], [])
        assert limiter.__enter__.call_count == 0
        _list_runs_from(source, [], [_legacy_node("run-2")])

    assert limiter.__enter__.call_count == 1


def test_legacy_run_kept_after_temporary_state_error_is_not_warned() -> None:
    source = _experiment_source()
    run = _legacy_mock_run()
    _mock(run.get_state).side_effect = _throttled()

    _read_experiment_runs(source, [run])

    assert LEGACY_RUN_EXECUTIONS_TITLE not in [i.title for i in source.report.infos]


def _experiment_context(
    display_name: str, with_metadata: bool, resource_id: str = ""
) -> MetadataContext:
    proto = gca_context.Context(
        name=_store_path("contexts", resource_id or display_name),
        display_name=display_name,
        schema_title="system.Experiment",
    )
    if with_metadata:
        proto.metadata = {}
    return _sdk_resource(MetadataContext, proto)


def _list_experiments(
    source: VertexAISource, contexts: List[MetadataContext]
) -> List[Experiment]:
    extractor = source.experiment_extractor
    with (
        patch(f"{_EXPERIMENT_MODULE}.rate_limited_gapic_list", return_value=contexts),
        patch.object(extractor, "_metadata_store_parent", return_value="p"),
    ):
        return extractor._list_experiments_rate_limited()


def test_experiment_without_metadata_struct_is_written() -> None:
    source = _experiment_source()

    experiments = _list_experiments(
        source,
        [
            _experiment_context("exp-one", with_metadata=True),
            _experiment_context("exp-two", with_metadata=False),
        ],
    )

    assert len(experiments) == 2
    assert not source.report.warnings
    assert not source.report.failures


def test_experiment_context_error_costs_only_that_experiment() -> None:
    source = _experiment_source()
    extractor = source.experiment_extractor
    first = _experiment_context("exp-one", with_metadata=True, resource_id="id-one")
    second = _experiment_context("exp-two", with_metadata=True)
    container_urn = extractor._experiment_key("id-one").as_urn()
    kept = _real_carry(source, previous=[container_urn])
    error = RuntimeError("boom")

    def check(ctx: MetadataContext) -> bool:
        if ctx is first:
            raise error
        return True

    with patch.object(extractor, "_is_experiment_context", side_effect=check):
        experiments = _list_experiments(source, [first, second])
    source._carry.flush()

    assert [e._metadata_context for e in experiments] == [second]
    assert [w.title for w in source.report.warnings] == [
        error_title(error, "Detail could not be read")
    ]
    assert any("item=id-one" in c for c in _warning_contexts(source))
    assert kept.urns == [container_urn]
    assert not source.report.failures


def _state_handler(
    prior: Optional[Dict[str, int]] = None,
) -> VertexAIStateHandler:
    source = MagicMock()
    source.state_provider.is_stateful_ingestion_configured.return_value = True
    source.state_provider.get_last_checkpoint.return_value = (
        MagicMock(state=VertexAICheckpointState(last_update_times=prior))
        if prior is not None
        else None
    )
    source.ctx.run_id = "test-run"
    source.ctx.pipeline_name = "test-pipeline"
    return VertexAIStateHandler(source=source, stateful_ingestion_config=None)


def test_state_running_maximum_never_filters_the_same_run() -> None:
    handler = _state_handler()

    handler.update_resource_timestamp(ResourceTypes.MODEL_EVALUATION, 5000)

    assert handler.get_last_update_time(ResourceTypes.MODEL_EVALUATION) is None


def test_older_timestamp_after_a_newer_one_keeps_the_maximum() -> None:
    handler = _state_handler()

    handler.update_resource_timestamp(ResourceTypes.MODEL, 5000)
    handler.update_resource_timestamp(ResourceTypes.MODEL, 3000)
    checkpoint = handler.create_checkpoint()

    assert checkpoint is not None
    assert checkpoint.state.last_update_times == {ResourceTypes.MODEL: 5000}


def test_prior_checkpoint_filters_and_new_checkpoint_holds_maximum() -> None:
    handler = _state_handler(
        {ResourceTypes.MODEL_EVALUATION: 3000, ResourceTypes.PIPELINE_JOB: 7000}
    )

    handler.update_resource_timestamp(ResourceTypes.MODEL_EVALUATION, 5000)
    checkpoint = handler.create_checkpoint()

    assert handler.get_last_update_time(ResourceTypes.MODEL_EVALUATION) == 3000
    assert checkpoint is not None
    assert checkpoint.state.last_update_times == {
        ResourceTypes.MODEL_EVALUATION: 5000,
        ResourceTypes.PIPELINE_JOB: 7000,
    }


_T0, _T1, _T2 = (MOCK_CREATE_TIME + timedelta(days=d) for d in (1, 2, 3))


def test_two_projects_write_the_older_model_after_a_newer_one() -> None:
    source = _model_source()
    source.model_extractor.state_handler = _state_handler(
        {ResourceTypes.MODEL: int(_T0.timestamp() * 1000)}
    )
    newer, older = gen_mock_model(1), gen_mock_model(2)
    _mock(newer).update_time = _T2
    _mock(older).update_time = _T1

    source._current_project_id = "proj-one"
    first = _read_models(source, [newer])
    source.model_extractor.reset_regional_caches()
    source._current_project_id = "proj-two"
    second = _read_models(source, [older])

    assert any("mock_prediction_model_1" in u for u in _written(first))
    assert any("mock_prediction_model_2" in u for u in _written(second))


def _two_project_evaluations(
    source: VertexAISource,
) -> Tuple[List[MetadataWorkUnit], List[MetadataWorkUnit]]:
    # The first project's evaluations are newer than the second's, so a read of
    # the running maximum would filter every evaluation of the second project.
    newer = [gen_mock_model_evaluation(n) for n in (1, 2, 3)]
    older = [gen_mock_model_evaluation(n) for n in (1, 2, 3)]
    for n, evaluation in enumerate(newer):
        _mock(evaluation).create_time = MOCK_CREATE_TIME + timedelta(days=20 + n)
    for n, evaluation in enumerate(older):
        _mock(evaluation).create_time = MOCK_CREATE_TIME + timedelta(days=n)
    model = gen_mock_model(1)

    source._current_project_id = "proj-one"
    first = _read_models(source, [model], evaluations=newer)
    source._current_project_id = "proj-two"
    second = _read_models(source, [model], evaluations=older)
    return first, second


@pytest.mark.parametrize("stateful", [False, True])
def test_two_projects_write_every_evaluation(stateful: bool) -> None:
    source = _model_source(include_models=False, include_evaluations=True)
    if stateful:
        source.model_extractor.state_handler = _state_handler()

    first, second = _two_project_evaluations(source)

    urns = [u for u in _written([*first, *second]) if ".model_evaluation." in u]
    assert len(urns) == 6
    for project in ("proj-one", "proj-two"):
        prefix = f"urn:li:mlModelDeployment:(urn:li:dataPlatform:vertexai,{project}."
        assert len([u for u in urns if u.startswith(prefix)]) == 3


def _spec_less_pipeline_job() -> PipelineJob:
    job = get_mock_pipeline_job()
    _mock(job).name = "no-spec"
    _mock(job).display_name = "no-spec"
    _mock(job).gca_resource = gca_pipeline_job.PipelineJob(
        name="projects/123/locations/us-central1/pipelineJobs/no-spec",
        display_name="no-spec",
    )
    return job


def _named_pipeline_job() -> PipelineJob:
    # Resource name, display name and name all differ, so a fallback to the
    # wrong one is visible in the flow URN.
    job = _spec_less_pipeline_job()
    _mock(job).name = "job-one"
    _mock(job).display_name = "display-one"
    _mock(job).resource_name = "projects/123/locations/us-central1/pipelineJobs/res-one"
    return job


def _flow_urns(workunits: List[MetadataWorkUnit]) -> List[str]:
    return [u for u in _written(workunits) if u.startswith("urn:li:dataFlow:")]


def _pipeline_run_urn(source: VertexAISource, job_name: str) -> str:
    return builder.make_data_process_instance_urn(
        source.pipeline_extractor.name_formatter.format_pipeline_run_id(job_name)
    )


def _run_urns(workunits: List[MetadataWorkUnit]) -> List[str]:
    return [
        w.get_urn()
        for w in workunits
        if w.get_urn().startswith("urn:li:dataProcessInstance:")
        and "pipeline_run" in w.get_urn()
    ]


def test_pipeline_without_spec_then_good_job_both_written() -> None:
    source = _pipeline_source()
    good = get_mock_pipeline_job()

    workunits = _read_pipelines(source, [_spec_less_pipeline_job(), good])

    assert set(_run_urns(workunits)) == {
        _pipeline_run_urn(source, "no-spec"),
        _pipeline_run_urn(source, good.name),
    }
    assert not source.report.warnings
    assert not any(
        u.startswith("urn:li:dataJob:") and "no-spec" in u for u in _written(workunits)
    )
    assert not source.report.failures


@pytest.mark.parametrize(
    "spec",
    [
        {"root": []},
        {"root": {"dag": {"tasks": []}}},
        {"root": {"dag": {"tasks": ""}}},
        {"root": {"dag": {"tasks": {"t1": {"dependentTasks": "t0"}}}}},
    ],
)
def test_malformed_spec_tasks_cost_only_that_job(spec: object) -> None:
    source = _pipeline_source()
    bad = _named_pipeline_job()
    _mock(bad).gca_resource.pipeline_spec = spec
    good = get_mock_pipeline_job()

    workunits = _read_pipelines(source, [bad, good])

    assert set(_run_urns(workunits)) == {_pipeline_run_urn(source, good.name)}
    assert [w.title for w in source.report.warnings] == [
        error_title(TypeError(), "Detail could not be read")
    ]
    assert [w.message for w in source.report.warnings] == [UNEXPECTED_DETAIL_ERROR]
    assert any(f"item={bad.name}" in c for c in _warning_contexts(source))
    assert not source.report.failures


def _read_with_failing_stable_id(
    source: VertexAISource, bad: PipelineJob, good: PipelineJob
) -> List[MetadataWorkUnit]:
    extractor = source.pipeline_extractor
    real_stable_id = extractor._get_stable_pipeline_id

    def stable_id(pipeline: PipelineJob) -> str:
        if pipeline.name == bad.name:
            raise RuntimeError("boom")
        return real_stable_id(pipeline)

    with patch.object(extractor, "_get_stable_pipeline_id", side_effect=stable_id):
        return _read_pipelines(source, [bad, good])


_PREVIOUS = [*_FLOW_URNS, _CONTAINER_URN, _PROJECT_CONTAINER_URN]


def _assert_broad_keep_names(source: VertexAISource, job: PipelineJob) -> None:
    (entry,) = [w for w in source.report.warnings if w.message == UNEXPECTED_ERROR]
    assert any(f"item={job.name}" in c for c in entry.context)


def test_stable_id_error_costs_only_that_job() -> None:
    source = _pipeline_source()
    bad = _named_pipeline_job()
    good = get_mock_pipeline_job()
    kept = _real_carry(source, previous=_PREVIOUS)

    workunits = _read_with_failing_stable_id(source, bad, good)
    source._carry.flush()

    assert set(_run_urns(workunits)) == {_pipeline_run_urn(source, good.name)}
    assert [w.message for w in source.report.warnings] == [
        UNEXPECTED_DETAIL_ERROR,
        UNEXPECTED_ERROR,
    ]
    _assert_broad_keep_names(source, bad)
    assert sorted(kept.urns) == sorted(_PREVIOUS)
    assert not source.report.failures


def test_stable_id_error_on_a_stopped_lookup_is_reported_and_kept() -> None:
    source = _pipeline_source()
    bad = _named_pipeline_job()
    good = get_mock_pipeline_job()
    kept = _real_carry(source, previous=_PREVIOUS)
    source._stopped.stop(
        Scope(project="proj-one", region=REGION, kind=DetailCalls.PIPELINE_LOOKUP),
        ErrorKind.TRANSIENT,
        ServiceUnavailable("stopped"),
    )

    workunits = _read_with_failing_stable_id(source, bad, good)
    source._carry.flush()

    assert not _run_urns(workunits)
    assert [w.message for w in source.report.warnings] == [UNEXPECTED_ERROR]
    _assert_broad_keep_names(source, bad)
    assert sorted(kept.urns) == sorted(_PREVIOUS)
    assert not source.report.failures


@pytest.mark.parametrize("info", [None, {"name": 5}])
def test_unusable_pipeline_info_falls_back_to_display_name(info: object) -> None:
    source = _pipeline_source()
    bad = _named_pipeline_job()
    _mock(bad).gca_resource.pipeline_spec = {"pipelineInfo": info}

    workunits = _read_pipelines(source, [bad])

    assert _flow_urns(workunits) == ["urn:li:dataFlow:(vertexai,display-one,PROD)"]
    assert set(_run_urns(workunits)) == {_pipeline_run_urn(source, "job-one")}
    assert not source.report.warnings
    assert not source.report.failures


def _written_legacy_runs(
    source: VertexAISource,
    nodes: List[MetadataExecution],
    extra_runs: Optional[List[ExperimentRun]] = None,
    artifact: Optional[MetadataArtifact] = None,
) -> List[MetadataWorkUnit]:
    with patch.object(
        ExperimentRun,
        "_v1_get_metric_artifact",
        return_value=(
            artifact if artifact is not None else _metrics_artifact({"acc": 0.9})
        ),
    ):
        runs = _list_runs_from(source, [], nodes)
    return _read_experiment_runs(source, [*runs, *(extra_runs or [])])


def test_legacy_run_without_metadata_struct_keeps_metrics() -> None:
    source = _experiment_source()

    workunits = _written_legacy_runs(source, [_legacy_node("run-1")])

    [props] = _aspects(workunits, MLTrainingRunPropertiesClass)
    assert props.hyperParams == []
    assert [m.name for m in props.trainingMetrics] == ["acc"]
    assert not source.report.warnings
    assert LEGACY_RUN_EXECUTIONS_TITLE in [i.title for i in source.report.infos]
    assert not source.report.failures
    assert not source._stopped.lost_in("proj-one")


def test_legacy_run_metrics_artifact_without_struct_has_no_metrics() -> None:
    source = _experiment_source()

    workunits = _written_legacy_runs(
        source, [_legacy_node("run-1", {"lr": 0.1})], artifact=_metrics_artifact()
    )

    assert not source.report.warnings
    assert LEGACY_RUN_EXECUTIONS_TITLE in [i.title for i in source.report.infos]
    [props] = _aspects(workunits, MLTrainingRunPropertiesClass)
    assert [p.name for p in props.hyperParams] == ["lr"]
    assert props.trainingMetrics == []
    assert not source.report.failures


def test_legacy_run_nested_param_renders_as_dict() -> None:
    source = _experiment_source()
    node = _legacy_node("run-1", {"lr": 0.1, "nested": {"a": 1}})

    workunits = _written_legacy_runs(source, [node])

    [props] = _aspects(workunits, MLTrainingRunPropertiesClass)
    assert {p.name: p.value for p in props.hyperParams} == {
        "lr": "0.1",
        "nested": "{'a': 1.0}",
    }


def test_executions_stop_does_not_hold_back_legacy_runs() -> None:
    source = _experiment_source()
    source._stopped.stop(
        Scope("proj-one", REGION, DetailCalls.EXPERIMENT_RUN_EXECUTIONS),
        ErrorKind.TRANSIENT,
        ServiceUnavailable("busy"),
    )

    workunits = _written_legacy_runs(
        source,
        [_legacy_node("run-1", {"lr": 0.1})],
        extra_runs=[gen_mock_experiment_run()],
    )

    assert len(_aspects(workunits, MLTrainingRunPropertiesClass)) == 1
    assert source.report.items_kept_after_stop == 1


def test_job_build_error_keeps_the_template_flow() -> None:
    root = get_mock_pipeline_job().gca_resource.pipeline_spec["root"]
    healthy = _named_pipeline_job()
    _mock(healthy).gca_resource.pipeline_spec = {
        "pipelineInfo": {"name": "tmpl"},
        "root": root,
    }
    twin_source = _pipeline_source()
    twin = _read_pipelines(twin_source, [healthy])
    folder_urn = twin_source.pipeline_extractor._get_pipeline_container("tmpl").as_urn()
    assert folder_urn in _written(twin)
    emitted = [*_flow_urns(twin), folder_urn]

    source = _pipeline_source()
    job = _named_pipeline_job()
    _mock(job).gca_resource.pipeline_spec = {
        "pipelineInfo": {"name": "tmpl"},
        "root": [],
    }
    other_urn = "urn:li:dataFlow:(vertexai,other,PROD)"
    kept = _real_carry(source, previous=[*emitted, other_urn])

    workunits = _read_pipelines(source, [job])
    source._carry.flush()

    assert not _run_urns(workunits)
    assert sorted(kept.urns) == sorted(emitted)
    assert [w.message for w in source.report.warnings] == [UNEXPECTED_DETAIL_ERROR]
    assert not source.report.failures


def test_discovery_failure_reads_listed_regions() -> None:
    source = _region_source(discover_regions=True, regions=[REGION, "europe-west4"])

    with _locations_client(_iam_denied()):
        run = _run_regions(source, {}, previous=_EVERY_URN)

    assert run.model_step_regions == [REGION, "europe-west4"]
    assert [w.message for w in source.report.warnings] == [
        REGION_DISCOVERY_FAILED,
        UNSCOPED_KEPT,
    ]
    assert sorted(run.kept.urns) == sorted(
        urn for urn in _EVERY_URN if urn not in _UNRELATED_URNS
    )
    assert not source.report.failures


def test_listed_typo_takes_region_unchecked() -> None:
    source = _region_source(discover_regions=True, regions=[REGION, "us-eas1"])

    with _locations_client(_iam_denied()):
        run = _run_regions(source, {}, previous=_EVERY_URN)

    assert run.model_step_regions == [REGION]
    assert REGION_DISCOVERY_FAILED in [w.message for w in source.report.warnings]
    assert [(f.title, f.message) for f in source.report.failures] == [
        (REGION_UNCHECKED_TITLE, REGION_UNCHECKED_AFTER_DISCOVERY)
    ]
    assert "regions=['us-eas1']" in "".join(
        c for f in source.report.failures for c in f.context
    )
    (kept_warning,) = [
        w for w in source.report.warnings if w.title == UNSCOPED_KEPT_TITLE
    ]
    assert [c for c in kept_warning.context if "proj-one" in c] == [
        "project=proj-one, resource=region_discovery"
    ]
    assert sorted(run.kept.urns) == sorted(
        urn for urn in _EVERY_URN if urn not in _UNRELATED_URNS
    )


def test_listed_global_is_skipped_with_the_usual_warning() -> None:
    source = _region_source(discover_regions=True, regions=[REGION, "global"])

    with _locations_client(_iam_denied()):
        run = _run_regions(source, {})

    assert run.model_step_regions == [REGION]
    skipped = [w for w in source.report.warnings if w.title == LOCATIONS_SKIPPED_TITLE]
    assert len(skipped) == 1
    assert "unsupported=['global']" in "".join(skipped[0].context)


def test_no_listed_region_readable_says_project_not_read() -> None:
    source = _region_source(discover_regions=True, regions=["global", "us-eas1"])

    with _locations_client(_iam_denied()):
        run = _run_regions(source, {})

    assert run.model_step_regions == []
    assert [f.message for f in source.report.failures] == [
        REGION_UNCHECKED_AFTER_DISCOVERY_NONE_READ
    ]


@pytest.mark.parametrize("listed", [True, False])
def test_api_off_during_discovery_reads_no_listed_region(listed: bool) -> None:
    source = _region_source(discover_regions=True, regions=[REGION])
    if not listed:
        source.config.project_ids = []
    source._carry = MagicMock()

    with _locations_client(_service_disabled()):
        regions = source._discover_regions_for_project("proj-one")

    assert regions == []
    assert REGION_DISCOVERY_FAILED not in [w.message for w in source.report.warnings]
    assert bool(source.report.failures) == listed
    source._carry.keep_project.assert_called_with("proj-one")


@pytest.mark.filterwarnings("ignore:.*region.*deprecated")
@pytest.mark.parametrize("discover_regions", [False, True])
def test_listed_regions_win_over_deprecated_region(discover_regions: bool) -> None:
    source = _region_source(
        discover_regions=discover_regions, regions=[REGION], region="europe-west4"
    )

    with _locations_client(_iam_denied()):
        run = _run_regions(source, {})

    assert run.model_step_regions == [REGION]
