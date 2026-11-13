import json
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import google.cloud.aiplatform as aiplatform
from google.api_core.exceptions import (
    NotFound,
    PermissionDenied,
    RetryError,
    ServiceUnavailable,
)
from google.rpc.error_details_pb2 import ErrorInfo
from pytest import Config

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.common.gcp_project_filter import GcpProject
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.vertexai.vertexai_experiment_extractor import (
    VertexAIExperimentExtractor,
)
from datahub.testing import mce_helpers
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_endpoint,
    gen_mock_experiment,
    gen_mock_experiment_run,
    gen_mock_model,
    gen_mock_model_evaluation,
    gen_mock_models,
    gen_mock_training_automl_job,
    gen_mock_training_custom_job,
    get_mock_pipeline_job,
)

GOLDEN_DIR = Path(__file__).parent / "failure_scope" / "run2"
REGION = "us-west2"
PIPELINE_NAME = "vertexai-failure-scope"

_EXTRACTOR_MODULES = [
    "datahub.ingestion.source.vertexai.vertexai_model_extractor",
    "datahub.ingestion.source.vertexai.vertexai_pipeline_extractor",
    "datahub.ingestion.source.vertexai.vertexai_training_extractor",
]
_EXPERIMENT_EXTRACTOR = (
    "datahub.ingestion.source.vertexai.vertexai_experiment_extractor"
    ".VertexAIExperimentExtractor"
)

# What a Google list call returns for (project, resource class), or raises.
ResourceFn = Callable[[str, Type], Any]


@dataclass
class TwoRuns:
    run1: List[Dict[str, Any]]
    run2: List[Dict[str, Any]]
    run2_report: StaleEntityRemovalSourceReport
    run2_output: Path
    soft_deleted: Set[str]


def _mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def refused_error() -> PermissionDenied:
    return PermissionDenied(
        "The caller lacks the required permission.",
        error_info=ErrorInfo(reason="IAM_PERMISSION_DENIED", domain="googleapis.com"),
    )


def enter_world(
    stack: ExitStack,
    resources: ResourceFn,
    current: Optional[Dict[str, str]] = None,
) -> None:
    """Patch every Google call the source makes so `resources` answers them.

    The project and region a call belongs to are the ones last passed to
    `aiplatform.init`, which is how the connector selects them. A caller that
    needs the region passes `current` and reads its "region" key.
    """
    current = {} if current is None else current

    def init(project: str, location: str, **_kw: Any) -> None:
        current["project"] = project
        current["region"] = location

    def gapic_list(cls: Type, _rate_limiter: Any, **_kw: Any) -> Any:
        return resources(current["project"], cls)

    stack.enter_context(patch("google.auth.default", return_value=(None, None)))
    stack.enter_context(patch("google.cloud.aiplatform.init", side_effect=init))
    for module in _EXTRACTOR_MODULES:
        stack.enter_context(
            patch(f"{module}.rate_limited_gapic_list", side_effect=gapic_list)
        )
    stack.enter_context(
        patch(
            f"{_EXPERIMENT_EXTRACTOR}._list_experiments_rate_limited",
            return_value=[gen_mock_experiment()],
        )
    )
    stack.enter_context(
        patch(
            f"{_EXPERIMENT_EXTRACTOR}._list_experiment_runs_rate_limited",
            return_value=[gen_mock_experiment_run()],
        )
    )
    stack.enter_context(
        patch("google.cloud.aiplatform.models.Model", return_value=gen_mock_model(1))
    )
    stack.enter_context(
        patch(
            "datahub.ingestion.source.vertexai.vertexai_training_extractor.Model",
            return_value=gen_mock_model(1),
        )
    )


def _read_records(path: Path) -> List[Dict[str, Any]]:
    return json.loads(path.read_text())


def _soft_deleted(records: List[Dict[str, Any]]) -> Set[str]:
    return {
        r["entityUrn"]
        for r in records
        if r.get("aspectName") == "status"
        and r["aspect"]["json"].get("removed") is True
    }


def run_twice(
    tmp_path: Path,
    healthy: Callable[[ExitStack], None],
    failing: Callable[[ExitStack], None],
    recipe_overrides: Optional[Dict[str, Any]] = None,
) -> TwoRuns:
    """Run the source twice against one stateful-ingestion state file."""
    state_file = tmp_path / "state.json"
    source_config: Dict[str, Any] = {
        "region": REGION,
        "use_ml_metadata_for_lineage": False,
        "extract_execution_metrics": False,
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "fail_safe_threshold": 100,
            "state_provider": {
                "type": "file",
                "config": {"filename": str(state_file)},
            },
        },
        **(recipe_overrides or {}),
    }

    def run(output: Path, enter: Callable[[ExitStack], None]) -> Pipeline:
        recipe = {
            "run_id": f"vertexai-failure-scope-{output.stem}",
            "pipeline_name": PIPELINE_NAME,
            "source": {"type": "vertexai", "config": source_config},
            "sink": {"type": "file", "config": {"filename": str(output)}},
        }
        with ExitStack() as stack:
            enter(stack)
            pipeline = Pipeline.create(recipe)
            pipeline.run()
        return pipeline

    run1_output = tmp_path / "run1.json"
    run2_output = tmp_path / "run2.json"
    run1_pipeline = run(run1_output, healthy)
    run1_pipeline.raise_from_status()
    run2_pipeline = run(run2_output, failing)

    run2_report = run2_pipeline.source.get_report()
    assert isinstance(run2_report, StaleEntityRemovalSourceReport)
    run2 = _read_records(run2_output)
    return TwoRuns(
        run1=_read_records(run1_output),
        run2=run2,
        run2_report=run2_report,
        run2_output=run2_output,
        soft_deleted=_soft_deleted(run2),
    )


REFUSED_PROJECT = "test-project-refused"
HEALTHY_PROJECT = "test-project-id"
_MODEL_URN_PREFIX = "urn:li:mlModel:(urn:li:dataPlatform:vertexai,"
_DEPLOYMENT_URN_PREFIX = "urn:li:mlModelDeployment:(urn:li:dataPlatform:vertexai,"


def _models_only(models_by_project: Dict[str, int], refused: Set[str]) -> ResourceFn:
    def resources(project: str, cls: Type) -> Any:
        if project in refused:
            raise refused_error()
        if cls is aiplatform.Model:
            return gen_mock_models(models_by_project[project])
        return []

    return resources


def test_project_scope(pytestconfig: Config, tmp_path: Path) -> None:
    models = {REFUSED_PROJECT: 2, HEALTHY_PROJECT: 2}
    runs = run_twice(
        tmp_path,
        healthy=lambda stack: enter_world(stack, _models_only(models, set())),
        failing=lambda stack: enter_world(
            stack,
            _models_only(
                {REFUSED_PROJECT: 2, HEALTHY_PROJECT: 1}, refused={REFUSED_PROJECT}
            ),
        ),
        recipe_overrides={
            "project_ids": [REFUSED_PROJECT, HEALTHY_PROJECT],
            "include_training_jobs": False,
            "include_pipelines": False,
            "include_experiments": False,
            "include_evaluations": False,
        },
    )

    gone_model = f"{_MODEL_URN_PREFIX}{HEALTHY_PROJECT}.model.mock_prediction_model_2_2"
    kept_model = f"{_MODEL_URN_PREFIX}{HEALTHY_PROJECT}.model.mock_prediction_model_1_1"
    run1_urns = {r["entityUrn"] for r in runs.run1}
    run2_urns = {r["entityUrn"] for r in runs.run2}
    assert any(urn.startswith(gone_model) for urn in run1_urns)
    assert not any(urn.startswith(gone_model) for urn in run2_urns)
    assert any(urn.startswith(kept_model) for urn in run2_urns)
    # The failed project pauses soft-deletion for the whole run, in every project.
    assert runs.soft_deleted == set()
    assert any(
        f"project={REFUSED_PROJECT}" in c
        for f in runs.run2_report.failures
        for c in f.context
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=runs.run2_output,
        golden_path=GOLDEN_DIR / "project_scope.json",
    )


def enter_discovery(stack: ExitStack, project_ids: List[str]) -> None:
    """Make Resource Manager discovery find `project_ids`, with no real client."""
    stack.enter_context(
        patch("datahub.ingestion.source.vertexai.vertexai.ProjectsClient")
    )
    stack.enter_context(
        patch(
            "datahub.ingestion.source.vertexai.vertexai.resolve_gcp_projects",
            return_value=[GcpProject(id=pid, name=pid) for pid in project_ids],
        )
    )


def test_project_scope_discovered_no_role(pytestconfig: Config, tmp_path: Path) -> None:
    discovered = [REFUSED_PROJECT, HEALTHY_PROJECT]

    def healthy(stack: ExitStack) -> None:
        enter_discovery(stack, discovered)
        enter_world(
            stack, _models_only({REFUSED_PROJECT: 2, HEALTHY_PROJECT: 2}, set())
        )

    def failing(stack: ExitStack) -> None:
        enter_discovery(stack, discovered)
        enter_world(
            stack,
            _models_only(
                {REFUSED_PROJECT: 2, HEALTHY_PROJECT: 1}, refused={REFUSED_PROJECT}
            ),
        )

    runs = run_twice(
        tmp_path,
        healthy=healthy,
        failing=failing,
        recipe_overrides={
            "include_training_jobs": False,
            "include_pipelines": False,
            "include_experiments": False,
            "include_evaluations": False,
        },
    )

    gone_model = f"{_MODEL_URN_PREFIX}{HEALTHY_PROJECT}.model.mock_prediction_model_2_2"
    assert any(urn.startswith(gone_model) for urn in runs.soft_deleted)
    assert not any(REFUSED_PROJECT in urn for urn in runs.soft_deleted)
    # Container URNs are GUIDs, so the check above cannot see them.
    assert not any(urn.startswith("urn:li:container:") for urn in runs.soft_deleted)
    assert not runs.run2_report.failures
    # One warning for the skipped project, not one per read it lost.
    (context,) = [c for w in runs.run2_report.warnings for c in w.context]
    assert f"project={REFUSED_PROJECT}" in context

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=runs.run2_output,
        golden_path=GOLDEN_DIR / "project_scope_discovered.json",
    )


REGION_ONE = "us-west2"
REGION_TWO = "europe-west4"
_TRAINING_CLASSES = (
    aiplatform.CustomJob,
    aiplatform.AutoMLTabularTrainingJob,
)


def _two_regions(training_lost_in: Optional[str]) -> Callable[[ExitStack], None]:
    """Region one holds the pipeline, a training job, a dataset and model 1;
    region two holds model 2."""
    current: Dict[str, str] = {}

    def resources(project: str, cls: Type) -> Any:
        region = current["region"]
        if cls in _TRAINING_CLASSES and region == training_lost_in:
            raise RetryError("retries exhausted", cause=ServiceUnavailable("busy"))
        if cls is aiplatform.Model:
            return [gen_mock_model(1 if region == REGION_ONE else 2)]
        if cls is aiplatform.PipelineJob and region == REGION_ONE:
            return [get_mock_pipeline_job()]
        if region == REGION_ONE and cls is aiplatform.CustomJob:
            return [gen_mock_training_custom_job()]
        if region == REGION_ONE and cls is aiplatform.AutoMLTabularTrainingJob:
            return [gen_mock_training_automl_job()]
        if region == REGION_ONE and cls is aiplatform.TabularDataset:
            return [gen_mock_dataset()]
        return []

    return lambda stack: enter_world(stack, resources, current)


def test_region_scope(pytestconfig: Config, tmp_path: Path) -> None:
    runs = run_twice(
        tmp_path,
        healthy=_two_regions(training_lost_in=None),
        failing=_two_regions(training_lost_in=REGION_ONE),
        recipe_overrides={
            "region": None,
            "regions": [REGION_ONE, REGION_TWO],
            "project_ids": [HEALTHY_PROJECT],
            "include_evaluations": False,
        },
    )

    model_one = f"{_MODEL_URN_PREFIX}{HEALTHY_PROJECT}.model.mock_prediction_model_1_1"
    model_two = f"{_MODEL_URN_PREFIX}{HEALTHY_PROJECT}.model.mock_prediction_model_2_2"
    run1_urns = {r["entityUrn"] for r in runs.run1}
    run2_urns = {r["entityUrn"] for r in runs.run2}
    assert any(urn.startswith(model_one) for urn in run1_urns)
    assert not any(urn.startswith(model_one) for urn in run2_urns)
    assert not any(urn.startswith(model_one) for urn in runs.soft_deleted)
    assert any(urn.startswith(model_two) for urn in run2_urns)
    assert any(urn.startswith("urn:li:dataFlow:") for urn in run2_urns)
    # The lost kind, the models of the held-back region and the dataset are all
    # kept, and nothing else is stale.
    assert any(u.startswith("urn:li:dataProcessInstance:") for u in run1_urns)
    assert any(u.startswith("urn:li:dataset:") for u in run1_urns)
    assert runs.soft_deleted == set()
    assert not runs.run2_report.failures
    assert any(
        f"region={REGION_ONE}, resource=training_jobs" in c
        for w in runs.run2_report.warnings
        for c in w.context
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=runs.run2_output,
        golden_path=GOLDEN_DIR / "region_scope.json",
    )


STALE_PIPELINE = "Stale Pipeline"


def _kind_loss_world(training_refused: bool) -> Callable[[ExitStack], None]:
    """One project with training jobs, a dataset and two pipelines. When
    `training_refused`, the training jobs are refused and the stale pipeline is
    gone."""

    def resources(project: str, cls: Type) -> Any:
        if cls in _TRAINING_CLASSES and training_refused:
            # A temporary loss: a lasting one would fail the run and pause
            # soft-deletion everywhere.
            raise RetryError("retries exhausted", cause=ServiceUnavailable("busy"))
        if cls is aiplatform.CustomJob:
            return [gen_mock_training_custom_job()]
        if cls is aiplatform.AutoMLTabularTrainingJob:
            return [gen_mock_training_automl_job()]
        if cls is aiplatform.TabularDataset:
            return [gen_mock_dataset()]
        if cls is aiplatform.PipelineJob:
            stale = get_mock_pipeline_job()
            _mock(stale).name = "mock_pipeline_job_stale"
            _mock(stale).display_name = STALE_PIPELINE
            return [get_mock_pipeline_job()] + ([] if training_refused else [stale])
        return []

    return lambda stack: enter_world(stack, resources)


def test_kind_loss_still_soft_deletes_stale_entities_of_other_kinds(
    tmp_path: Path,
) -> None:
    runs = run_twice(
        tmp_path,
        healthy=_kind_loss_world(training_refused=False),
        failing=_kind_loss_world(training_refused=True),
        recipe_overrides={
            "project_ids": [HEALTHY_PROJECT],
            "include_models": False,
            "include_experiments": False,
            "include_evaluations": False,
        },
    )

    stale_flow = f"urn:li:dataFlow:(vertexai,{STALE_PIPELINE},PROD)"
    assert any(r["entityUrn"].startswith("urn:li:dataset:") for r in runs.run1)
    assert not runs.run2_report.failures
    # The training jobs and the dataset are kept, but the pipeline that is gone
    # is soft-deleted all the same, with its task and its folder.
    stale_containers = {u for u in runs.soft_deleted if "urn:li:container:" in u}
    assert len(stale_containers) == 1
    assert runs.soft_deleted - stale_containers == {
        stale_flow,
        f"urn:li:dataJob:({stale_flow},reverse)",
    }


def _training_world(
    ml_metadata_refused: bool, output_model_found: bool
) -> Callable[[ExitStack], None]:
    custom_jobs = [gen_mock_training_custom_job(), gen_mock_training_custom_job()]
    _mock(custom_jobs[1]).name = "mock_training_job_2"

    def resources(project: str, cls: Type) -> Any:
        if cls is aiplatform.CustomJob:
            return custom_jobs
        if cls is aiplatform.AutoMLTabularTrainingJob:
            return [gen_mock_training_automl_job()]
        if cls is aiplatform.TabularDataset:
            return [gen_mock_dataset()]
        return []

    def enter(stack: ExitStack) -> None:
        enter_world(stack, resources)
        metadata_client = MagicMock()
        if ml_metadata_refused:
            metadata_client.list_executions.side_effect = refused_error()
        else:
            execution = Mock()
            execution.name = "mock-execution"
            execution.metadata = {"learning_rate": Mock(string_value="0.001")}
            metadata_client.list_executions.return_value = [execution]
        stack.enter_context(
            patch(
                "datahub.ingestion.source.vertexai.vertexai.MetadataServiceClient",
                return_value=metadata_client,
            )
        )
        if not output_model_found:
            stack.enter_context(
                patch(
                    "datahub.ingestion.source.vertexai.vertexai_training_extractor.Model",
                    side_effect=NotFound("output model gone"),
                )
            )

    return enter


def test_training_scope(pytestconfig: Config, tmp_path: Path) -> None:
    runs = run_twice(
        tmp_path,
        healthy=_training_world(ml_metadata_refused=False, output_model_found=True),
        failing=_training_world(ml_metadata_refused=True, output_model_found=False),
        recipe_overrides={
            "project_ids": [HEALTHY_PROJECT],
            "extract_execution_metrics": True,
            "include_models": False,
            "include_pipelines": False,
            "include_experiments": False,
            "include_evaluations": False,
        },
    )

    job_urns = {
        r["entityUrn"]
        for r in runs.run2
        if r["entityUrn"].startswith("urn:li:dataProcessInstance:")
    }
    assert len(job_urns) == 3
    assert any("hyperParams" in json.dumps(r) for r in runs.run1)
    assert not any("hyperParams" in json.dumps(r) for r in runs.run2)
    assert any(r["entityUrn"].startswith("urn:li:dataset:") for r in runs.run2)
    assert runs.soft_deleted == set()
    assert not runs.run2_report.failures
    contexts = [c for w in runs.run2_report.warnings for c in w.context]
    assert any(f"region={REGION}, resource=ml_metadata" in c for c in contexts)
    assert any("resource=automl_output_model" in c for c in contexts)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=runs.run2_output,
        golden_path=GOLDEN_DIR / "training_scope.json",
    )


def _model_world(
    endpoints_refused: bool, versions_throttled: bool
) -> Callable[[ExitStack], None]:
    """Region one holds models 1 and 2 and an endpoint serving model 1; region
    two holds models 3 and 4."""
    current: Dict[str, str] = {}
    models = [gen_mock_model(num) for num in (1, 2, 3, 4)]
    if versions_throttled:
        models[
            2
        ].versioning_registry.client.list_model_versions.side_effect = RetryError(
            "retries exhausted", cause=ServiceUnavailable("busy")
        )
    endpoint = gen_mock_endpoint()
    _mock(endpoint.list_models).return_value = [Mock(model=models[0].resource_name)]

    def resources(project: str, cls: Type) -> Any:
        region = current["region"]
        if cls is aiplatform.Model:
            return models[:2] if region == REGION_ONE else models[2:]
        if cls is aiplatform.Endpoint and region == REGION_ONE:
            if endpoints_refused:
                raise refused_error()
            return [endpoint]
        return []

    return lambda stack: enter_world(stack, resources, current)


def test_model_scope(pytestconfig: Config, tmp_path: Path) -> None:
    runs = run_twice(
        tmp_path,
        healthy=_model_world(endpoints_refused=False, versions_throttled=False),
        failing=_model_world(endpoints_refused=True, versions_throttled=True),
        recipe_overrides={
            "region": None,
            "regions": [REGION_ONE, REGION_TWO],
            "project_ids": [HEALTHY_PROJECT],
            "include_training_jobs": False,
            "include_pipelines": False,
            "include_experiments": False,
            "include_evaluations": False,
        },
    )

    def model_urn(num: int) -> str:
        name = f"{HEALTHY_PROJECT}.model.mock_prediction_model_{num}_{num},"
        return f"{_MODEL_URN_PREFIX}{name}"

    def written(records: List[Dict[str, Any]], urn: str) -> bool:
        return any(r["entityUrn"].startswith(urn) for r in records)

    def deployments(records: List[Dict[str, Any]], urn: str) -> List[str]:
        return [
            deployment
            for r in records
            if r["entityUrn"].startswith(urn) and r["aspectName"] == "mlModelProperties"
            for deployment in r["aspect"]["json"].get("deployments", [])
        ]

    assert deployments(runs.run1, model_urn(1))
    assert written(runs.run2, model_urn(1))
    assert written(runs.run2, model_urn(2))
    assert not deployments(runs.run2, model_urn(1))
    # The throttled model, and the rest of its region, keep their earlier entities.
    for num in (3, 4):
        assert written(runs.run1, model_urn(num))
        assert not written(runs.run2, model_urn(num))
    assert runs.soft_deleted == set()
    assert not runs.run2_report.failures
    contexts = [c for w in runs.run2_report.warnings for c in w.context]
    assert len([c for c in contexts if "resource=endpoint_list" in c]) == 1
    assert len([c for c in contexts if "resource=model_lookup" in c]) == 1

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=runs.run2_output,
        golden_path=GOLDEN_DIR / "model_scope.json",
    )


def _served_model_world(versions_throttled: bool) -> Callable[[ExitStack], None]:
    """One region holds models 1 and 2 and an endpoint that serves model 1. When
    `versions_throttled`, the version lookup of model 1 exhausts its retries."""
    current: Dict[str, str] = {}
    models = [gen_mock_model(num) for num in (1, 2)]
    if versions_throttled:
        models[
            0
        ].versioning_registry.client.list_model_versions.side_effect = RetryError(
            "retries exhausted", cause=ServiceUnavailable("busy")
        )
    endpoint = gen_mock_endpoint()
    _mock(endpoint.list_models).return_value = [Mock(model=models[0].resource_name)]

    def resources(project: str, cls: Type) -> Any:
        if cls is aiplatform.Model:
            return models
        if cls is aiplatform.Endpoint:
            return [endpoint]
        return []

    return lambda stack: enter_world(stack, resources, current)


def test_model_scope_kept_model_keeps_the_deployment_that_serves_it(
    tmp_path: Path,
) -> None:
    runs = run_twice(
        tmp_path,
        healthy=_served_model_world(versions_throttled=False),
        failing=_served_model_world(versions_throttled=True),
        recipe_overrides={
            "project_ids": [HEALTHY_PROJECT],
            "include_training_jobs": False,
            "include_pipelines": False,
            "include_experiments": False,
            "include_evaluations": False,
        },
    )

    deployment = f"{_DEPLOYMENT_URN_PREFIX}{HEALTHY_PROJECT}.endpoint."
    assert any(r["entityUrn"].startswith(deployment) for r in runs.run1)
    assert not any(r["entityUrn"].startswith(_MODEL_URN_PREFIX) for r in runs.run2)
    assert runs.soft_deleted == set()


def _model_list_world(not_found_once: bool) -> Callable[[ExitStack], None]:
    """Models 1 and 2, each with an evaluation, and an endpoint serving model 1.
    When `not_found_once`, the first model list answers 404."""
    models = gen_mock_models(2)
    endpoint = gen_mock_endpoint()
    _mock(endpoint.list_models).return_value = [Mock(model=models[0].resource_name)]
    pending = [NotFound("models not found")] if not_found_once else []

    def resources(project: str, cls: Type) -> Any:
        if cls is aiplatform.Model:
            if pending:
                raise pending.pop()
            return models
        if cls is aiplatform.Endpoint:
            return [endpoint]
        if cls is aiplatform.ModelEvaluation:
            return [gen_mock_model_evaluation()]
        return []

    return lambda stack: enter_world(stack, resources)


def test_model_list_not_found_keeps_the_models(tmp_path: Path) -> None:
    runs = run_twice(
        tmp_path,
        healthy=_model_list_world(not_found_once=False),
        failing=_model_list_world(not_found_once=True),
        recipe_overrides={"project_ids": [HEALTHY_PROJECT]},
    )

    assert any(r["entityUrn"].startswith(_MODEL_URN_PREFIX) for r in runs.run1)
    assert runs.soft_deleted == set()
    (context,) = [c for f in runs.run2_report.failures for c in f.context]
    assert f"project={HEALTHY_PROJECT}" in context
    assert "resource=models" in context


def _retry_budget_world(
    throttled: bool,
) -> Tuple[Callable[[ExitStack], None], List[Any]]:
    """Region one holds models 1 and 2, region two models 3 and 4. When
    `throttled`, the version lookup of model 1 exhausts its retries."""
    current: Dict[str, str] = {}
    models = [gen_mock_model(num) for num in (1, 2, 3, 4)]
    if throttled:
        models[
            0
        ].versioning_registry.client.list_model_versions.side_effect = RetryError(
            "retries exhausted", cause=ServiceUnavailable("busy")
        )

    def resources(project: str, cls: Type) -> Any:
        if cls is aiplatform.Model:
            return models[:2] if current["region"] == REGION_ONE else models[2:]
        return []

    return lambda stack: enter_world(stack, resources, current), models


def test_retry_budget(pytestconfig: Config, tmp_path: Path) -> None:
    healthy, _ = _retry_budget_world(throttled=False)
    failing, models = _retry_budget_world(throttled=True)
    runs = run_twice(
        tmp_path,
        healthy=healthy,
        failing=failing,
        recipe_overrides={
            "region": None,
            "regions": [REGION_ONE, REGION_TWO],
            "project_ids": [HEALTHY_PROJECT],
            "include_training_jobs": False,
            "include_pipelines": False,
            "include_experiments": False,
            "include_evaluations": False,
        },
    )

    def written(records: List[Dict[str, Any]], num: int) -> bool:
        prefix = (
            f"{_MODEL_URN_PREFIX}{HEALTHY_PROJECT}.model.mock_prediction_model_{num}_"
        )
        return any(r["entityUrn"].startswith(prefix) for r in records)

    # The throttled lookup and every later lookup in its region keep their
    # earlier entities; the other region is written in full.
    for num in (1, 2):
        assert written(runs.run1, num)
        assert not written(runs.run2, num)
    for num in (3, 4):
        assert written(runs.run2, num)
    versions = [m.versioning_registry.client.list_model_versions for m in models]
    assert versions[0].call_count == 1
    versions[1].assert_not_called()
    assert runs.soft_deleted == set()
    assert not runs.run2_report.failures
    contexts = [c for w in runs.run2_report.warnings for c in w.context]
    assert len([c for c in contexts if "resource=model_lookup" in c]) == 1

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=runs.run2_output,
        golden_path=GOLDEN_DIR / "retry_budget.json",
    )


MISSING_STORE_PROJECT = "test-project-no-store"
_EXPERIMENT_MODULE = "datahub.ingestion.source.vertexai.vertexai_experiment_extractor"


def _experiment_world(
    store_missing: bool, run_from_sdk: bool
) -> Callable[[ExitStack], None]:
    """Each project holds one experiment with one run. Where the Metadata Store
    is missing, the project answers 404 to the experiment list."""
    current: Dict[str, str] = {}
    list_experiments = VertexAIExperimentExtractor._list_experiments_rate_limited
    experiments = {
        HEALTHY_PROJECT: gen_mock_experiment(1),
        MISSING_STORE_PROJECT: gen_mock_experiment(2),
    }
    # Experiments share one incremental checkpoint across projects, so the
    # second project's experiment must be newer than the first one's.
    _mock(experiments[MISSING_STORE_PROJECT]).update_time += timedelta(days=1)
    run = gen_mock_experiment_run()
    if run_from_sdk:
        _mock(run.get_logged_custom_jobs).side_effect = TypeError(
            "not a custom job run"
        )

    def experiments_of_project(extractor: Any) -> Any:
        if store_missing and current["project"] == MISSING_STORE_PROJECT:
            with patch(
                f"{_EXPERIMENT_MODULE}.rate_limited_gapic_list",
                side_effect=NotFound("Requested Metadata Store default not found"),
            ):
                return list_experiments(extractor)
        return [experiments[current["project"]]]

    def enter(stack: ExitStack) -> None:
        enter_world(stack, lambda _project, _cls: [], current)
        stack.enter_context(
            patch(
                f"{_EXPERIMENT_EXTRACTOR}._list_experiments_rate_limited",
                new=experiments_of_project,
            )
        )
        stack.enter_context(
            patch(
                f"{_EXPERIMENT_EXTRACTOR}._list_experiment_runs_rate_limited",
                return_value=[run],
            )
        )
        stack.enter_context(
            patch(
                f"{_EXPERIMENT_EXTRACTOR}._metadata_store_parent",
                return_value="projects/p/locations/l/metadataStores/default",
            )
        )

    return enter


def test_experiment_scope(pytestconfig: Config, tmp_path: Path) -> None:
    runs = run_twice(
        tmp_path,
        healthy=_experiment_world(store_missing=False, run_from_sdk=False),
        failing=_experiment_world(store_missing=True, run_from_sdk=True),
        recipe_overrides={
            "project_ids": [HEALTHY_PROJECT, MISSING_STORE_PROJECT],
            "include_models": False,
            "include_training_jobs": False,
            "include_pipelines": False,
            "include_evaluations": False,
        },
    )

    def container_urn(records: List[Dict[str, Any]], experiment: str) -> str:
        return next(
            r["entityUrn"]
            for r in records
            if r["aspectName"] == "containerProperties"
            and r["aspect"]["json"]["name"] == experiment
        )

    def run_aspects(records: List[Dict[str, Any]], aspect: str) -> List[Any]:
        return [
            r["aspect"]["json"]
            for r in records
            if r["entityUrn"].startswith("urn:li:dataProcessInstance:")
            and r["aspectName"] == aspect
        ]

    def upstream_instances(records: List[Dict[str, Any]]) -> List[Any]:
        return [
            r
            for r in run_aspects(records, "dataProcessInstanceRelationships")
            if r.get("upstreamInstances")
        ]

    assert upstream_instances(runs.run1)
    assert run_aspects(runs.run2, "dataProcessInstanceProperties")
    assert not upstream_instances(runs.run2)
    # A project without a Metadata Store has no experiments, so its earlier one
    # is soft-deleted; the other project's experiment is read and kept.
    assert runs.soft_deleted == {container_urn(runs.run1, "mock_experiment_2")}
    assert not runs.run2_report.failures
    contexts = [c for w in runs.run2_report.warnings for c in w.context]
    assert len(contexts) == 1
    assert "resource=experiment_run_logged_jobs" in contexts[0]
    assert "Vertex AI Metadata Store not found" in [
        i.title for i in runs.run2_report.infos
    ]

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=runs.run2_output,
        golden_path=GOLDEN_DIR / "experiment_scope.json",
    )


def _pipeline_world(
    refused: Set[str], lookup_refused: bool
) -> Callable[[ExitStack], None]:
    """Both projects run the shared pipeline. The first also runs another, and
    the second runs one more whose own lookup is refused when `lookup_refused`.
    Where `refused` names a project, its pipeline list is refused."""

    def job(name: str, display_name: str) -> Any:
        pipeline_job = get_mock_pipeline_job()
        _mock(pipeline_job).name = name
        _mock(pipeline_job).display_name = display_name
        return pipeline_job

    def resources(project: str, cls: Type) -> Any:
        if cls is not aiplatform.PipelineJob:
            return []
        if project in refused:
            raise refused_error()
        jobs = [get_mock_pipeline_job()]
        if project == REFUSED_PROJECT:
            return jobs + [job("mock_pipeline_job_first", FIRST_ONLY_PIPELINE)]
        lookup = job("mock_pipeline_job_lookup", LOOKUP_REFUSED_PIPELINE)
        if lookup_refused:
            type(lookup).task_details = PropertyMock(side_effect=refused_error())
        return jobs + [lookup]

    return lambda stack: enter_world(stack, resources)


FIRST_ONLY_PIPELINE = "First Project Pipeline"
LOOKUP_REFUSED_PIPELINE = "Lookup Refused Pipeline"


def test_pipeline_scope(pytestconfig: Config, tmp_path: Path) -> None:
    runs = run_twice(
        tmp_path,
        healthy=_pipeline_world(refused=set(), lookup_refused=False),
        failing=_pipeline_world(refused={REFUSED_PROJECT}, lookup_refused=True),
        recipe_overrides={
            "project_ids": [REFUSED_PROJECT, HEALTHY_PROJECT],
            "include_models": False,
            "include_experiments": False,
            "include_evaluations": False,
        },
    )

    def of_type(records: List[Dict[str, Any]], entity_type: str) -> Set[str]:
        marker = f"urn:li:{entity_type}:"
        return {r["entityUrn"] for r in records if marker in r["entityUrn"]}

    # The refused project's own flow is not written again, and neither it, its
    # task nor any container is soft-deleted, though the other project read fine.
    first_flow = f"urn:li:dataFlow:(vertexai,{FIRST_ONLY_PIPELINE},PROD)"
    lookup_flow = f"urn:li:dataFlow:(vertexai,{LOOKUP_REFUSED_PIPELINE},PROD)"
    assert first_flow in of_type(runs.run1, "dataFlow")
    assert first_flow not in of_type(runs.run2, "dataFlow")
    # A refused lookup of one job costs only its task details: its flow is
    # still written, with the times the list returned.
    (lookup_info,) = [
        r["aspect"]["json"]
        for r in runs.run2
        if r["entityUrn"] == lookup_flow and r["aspectName"] == "dataFlowInfo"
    ]
    assert "update_time" in lookup_info["customProperties"]
    assert "duration" in lookup_info["customProperties"]
    assert runs.soft_deleted == set()
    # The list is refused on every run, so the listed project fails the run.
    contexts = [c for f in runs.run2_report.failures for c in f.context]
    assert any(
        f"project={REFUSED_PROJECT}" in c and "resource=pipelines" in c
        for c in contexts
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=runs.run2_output,
        golden_path=GOLDEN_DIR / "pipeline_scope.json",
    )


def run_once(
    tmp_path: Path, enter: Callable[[ExitStack], None], overrides: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], StaleEntityRemovalSourceReport]:
    output = tmp_path / "run.json"
    recipe = {
        "run_id": "vertexai-failure-scope-once",
        "pipeline_name": PIPELINE_NAME,
        "source": {
            "type": "vertexai",
            "config": {"region": REGION, **overrides},
        },
        "sink": {"type": "file", "config": {"filename": str(output)}},
    }
    with ExitStack() as stack:
        enter(stack)
        pipeline = Pipeline.create(recipe)
        pipeline.run()
    report = pipeline.source.get_report()
    assert isinstance(report, StaleEntityRemovalSourceReport)
    return _read_records(output), report


def test_first_run_with_experiments_refused_writes_models_and_fails(
    tmp_path: Path,
) -> None:
    refused = PermissionDenied(
        "The caller lacks the required permission.",
        error_info=ErrorInfo(
            reason="IAM_PERMISSION_DENIED",
            metadata={"permission": "aiplatform.metadataStores.get"},
        ),
    )

    def enter(stack: ExitStack) -> None:
        _model_list_world(not_found_once=False)(stack)
        stack.enter_context(
            patch(
                f"{_EXPERIMENT_EXTRACTOR}._list_experiments_rate_limited",
                side_effect=refused,
            )
        )

    records, report = run_once(
        tmp_path,
        enter,
        {
            "project_ids": [HEALTHY_PROJECT],
            "use_ml_metadata_for_lineage": False,
            "extract_execution_metrics": False,
            "include_evaluations": False,
        },
    )

    urns = {r["entityUrn"] for r in records}
    assert any(u.startswith(_MODEL_URN_PREFIX) for u in urns)
    assert any(u.startswith("urn:li:mlModelGroup:") for u in urns)
    assert any(u.startswith(_DEPLOYMENT_URN_PREFIX) for u in urns)
    (context,) = [c for f in report.failures for c in f.context]
    assert f"project={HEALTHY_PROJECT}" in context
    assert "resource=experiments" in context
    assert "permission=aiplatform.metadataStores.get" in context
    assert "switch=include_experiments" in context
