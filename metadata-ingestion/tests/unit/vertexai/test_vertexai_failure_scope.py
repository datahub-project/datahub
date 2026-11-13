import threading
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from google.api_core.exceptions import (
    Aborted,
    Cancelled,
    DeadlineExceeded,
    FailedPrecondition,
    GoogleAPIError,
    InvalidArgument,
    MethodNotImplemented,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    RetryError,
    ServiceUnavailable,
    Unauthenticated,
    Unauthorized,
    Unknown,
)
from google.auth.exceptions import RefreshError, TransportError
from google.cloud import aiplatform
from google.cloud.aiplatform import ModelEvaluation
from google.cloud.location import locations_pb2
from google.protobuf.struct_pb2 import Value
from google.rpc.error_details_pb2 import ErrorInfo

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.vertexai.vertexai import VertexAIConfig, VertexAISource
from datahub.ingestion.source.vertexai.vertexai_constants import (
    PLATFORM,
    DetailCalls,
    ResourceCategory,
    ResourceKinds,
)
from datahub.ingestion.source.vertexai.vertexai_ml_metadata_helper import (
    MLMetadataHelper,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    MLMetadataConfig,
    ModelUsageTracker,
    PipelineContainerKey,
    model_version_base,
)
from datahub.ingestion.source.vertexai.vertexai_read_ledger import ReadLedger
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    get_job_result_status,
)
from datahub.ingestion.source.vertexai.vertexai_scope import (
    CarryForward,
    DetailReader,
    DetailResult,
    ErrorKind,
    Scope,
    StoppedScopes,
    classify_error,
    read_list_item,
    urn_entity_name,
)
from datahub.ingestion.source.vertexai.vertexai_utils import (
    get_project_container,
    get_resource_category_container,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstanceRelationshipsClass,
    MLModelPropertiesClass,
    MLTrainingRunPropertiesClass,
    StatusClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.utilities.urns.urn import guess_entity_type
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_endpoint,
    gen_mock_experiment,
    gen_mock_experiment_run,
    gen_mock_model,
    gen_mock_model_evaluation,
    gen_mock_training_automl_job,
    gen_mock_training_custom_job,
    get_mock_pipeline_job,
)

ENV = "PROD"


def _mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


@pytest.mark.parametrize(
    "exc,kind",
    [
        (
            PermissionDenied(
                "x",
                error_info=ErrorInfo(
                    reason="SERVICE_DISABLED",
                    metadata={"service": "aiplatform.googleapis.com"},
                ),
            ),
            ErrorKind.PROJECT_WIDE,
        ),
        (
            PermissionDenied("x", error_info=ErrorInfo(reason="CONSUMER_INVALID")),
            ErrorKind.PROJECT_WIDE,
        ),
        (
            PermissionDenied("x", error_info=ErrorInfo(reason="IAM_PERMISSION_DENIED")),
            ErrorKind.REFUSED,
        ),
        (InvalidArgument("x"), ErrorKind.ITEM_LASTING),
        (NotFound("x"), ErrorKind.ITEM_LASTING),
        (ResourceExhausted("x"), ErrorKind.TRANSIENT),
        (ServiceUnavailable("x"), ErrorKind.TRANSIENT),
        (DeadlineExceeded("x"), ErrorKind.TRANSIENT),
        (RetryError("x", cause=ServiceUnavailable("y")), ErrorKind.TRANSIENT),
        (RefreshError("x"), ErrorKind.TRANSIENT),
        (TransportError("x"), ErrorKind.TRANSIENT),
        (TypeError("x"), ErrorKind.ITEM_LASTING),
        (NotImplementedError("x"), ErrorKind.ITEM_LASTING),
    ],
)
def test_primitives_classify_error(exc: BaseException, kind: ErrorKind) -> None:
    assert classify_error(exc) is kind


def test_primitives_scope_context_omits_missing_parts() -> None:
    assert (
        Scope("p1", region="us-central1", kind="job").context()
        == "project=p1, region=us-central1, resource=job"
    )


def test_primitives_stopped_scopes_keep_the_first_stop() -> None:
    stopped = StoppedScopes()
    scope = Scope("p1", "r1", "ml_metadata")
    first = _iam_denied()
    stopped.stop(scope, ErrorKind.REFUSED, first)
    stopped.stop(scope, ErrorKind.TRANSIENT, ServiceUnavailable("x"))
    assert stopped.stop_kind(scope) is ErrorKind.REFUSED
    assert stopped.stop_error(scope) is first


def _model_urn(name: str) -> str:
    return f"urn:li:mlModel:(urn:li:dataPlatform:{PLATFORM},{name},{ENV})"


def _dataset_urn(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{name},{ENV})"


def _job_urn(flow_urn: str, job_id: str) -> str:
    return f"urn:li:dataJob:({flow_urn},{job_id})"


def _flow_urn(flow_id: str) -> str:
    return f"urn:li:dataFlow:({PLATFORM},{flow_id},{ENV})"


def _carry_forward(
    previous: Optional[List[str]],
    has_current: bool = True,
    platform_instance: Optional[str] = None,
    report: Optional[VertexAISourceReport] = None,
) -> Tuple[CarryForward, GenericCheckpointState, MagicMock]:
    current = GenericCheckpointState()
    provider = MagicMock()
    if has_current:
        provider.get_current_checkpoint.return_value.state = current
    else:
        provider.get_current_checkpoint.return_value = None
    if previous is None:
        provider.get_last_checkpoint.return_value = None
    else:
        provider.get_last_checkpoint.return_value.state = GenericCheckpointState(
            urns=previous
        )
    carry = CarryForward(
        provider,
        PLATFORM,
        platform_instance=platform_instance,
        env=ENV,
        report=report or VertexAISourceReport(),
    )
    return carry, current, provider


def _deployment_urn(endpoint: str) -> str:
    return builder.make_ml_model_deployment_urn(
        platform=PLATFORM, deployment_name=f"proj-one.endpoint.{endpoint}", env=ENV
    )


def _instance_urn(name: str) -> str:
    return builder.make_data_process_instance_urn(name)


_CONTAINER_URN = "urn:li:container:0123456789abcdef0123456789abcdef"
_PROJECT_CONTAINER_URN = get_project_container("proj-one", PLATFORM, None, ENV).as_urn()
_GROUP_URN = builder.make_ml_model_group_urn(
    platform=PLATFORM, group_name="proj-one.model_group.m1", env=ENV
)
# Entities of the model step that a held-back or failed model step must keep.
_MODEL_STEP_URNS = [
    _model_urn("proj-one.model.m1_1"),
    _GROUP_URN,
    _deployment_urn("test-endpoint"),
    builder.make_ml_model_deployment_urn(
        platform=PLATFORM, deployment_name="proj-one.model_evaluation.ev1", env=ENV
    ),
]
# Process instances never reach a checkpoint, so none appear here.
_TRAINING_URNS = [_dataset_urn("proj-one.dataset.d1")]
_FLOW_URNS = [_flow_urn("pipe-a"), _job_urn(_flow_urn("pipe-a"), "t1")]
# What no model step or phase 1 loss has a reason to keep.
_UNRELATED_URNS = [_model_urn("proj-two.model.m9_1")]
_EVERY_URN = [
    *_MODEL_STEP_URNS,
    *_TRAINING_URNS,
    *_FLOW_URNS,
    *_UNRELATED_URNS,
    _CONTAINER_URN,
    _PROJECT_CONTAINER_URN,
]


def _real_carry(source: VertexAISource, previous: List[str]) -> GenericCheckpointState:
    """Gives the source and its extractors one real CarryForward that remembers
    `previous`; the returned state is what the run would keep."""
    carry, current, _ = _carry_forward(previous)
    source._carry = carry
    for extractor in (
        source.model_extractor,
        source.training_extractor,
        source.pipeline_extractor,
    ):
        extractor._carry = carry
    return current


def test_primitives_entity_name_prefix_handles_domain_scoped_project() -> None:
    urn = _model_urn("example.com:my-project.model.m1_1")
    assert urn_entity_name(urn) == "example.com:my-project.model.m1_1"

    carry, current, _ = _carry_forward([urn])
    carry.keep_kind("example", ["model"])
    carry.flush()
    assert current.urns == []
    carry.keep_kind("example.com:my-project", ["model"])
    carry.flush()
    assert current.urns == [urn]


def test_primitives_carry_forward_keeps_only_matching_project_and_kind() -> None:
    kept = _model_urn("p1.model.a")
    carry, current, _ = _carry_forward(
        [kept, _dataset_urn("p1.dataset.b"), _model_urn("p2.model.c")]
    )
    carry.keep_kind("p1", ["model"])
    carry.flush()
    assert current.urns == [kept]


def test_primitives_carry_forward_keep_project_includes_containers() -> None:
    category_urn = get_resource_category_container(
        "p1", PLATFORM, None, ENV, ResourceCategory.MODELS
    ).as_urn()
    other_project_urn = get_project_container("p2", PLATFORM, None, ENV).as_urn()
    project_urn = get_project_container("p1", PLATFORM, None, ENV).as_urn()
    carry, current, _ = _carry_forward([project_urn, category_urn, other_project_urn])
    carry.keep_project("p1")
    carry.flush()
    assert sorted(current.urns) == sorted([project_urn, category_urn])


def test_primitives_carry_forward_keep_flow_keeps_its_jobs() -> None:
    flow = _flow_urn("p1.experiment.e1")
    job = _job_urn(flow, "p1.experiment_run.r1")
    other_job = _job_urn(_flow_urn("p1.experiment.e2"), "p1.experiment_run.r2")
    carry, current, _ = _carry_forward([flow, job, other_job])
    carry.keep_flow(flow)
    carry.flush()
    assert sorted(current.urns) == sorted([flow, job])


def test_primitives_carry_forward_is_noop_without_checkpoint() -> None:
    carry, current, provider = _carry_forward(
        [_model_urn("p1.model.a")], has_current=False
    )
    carry.keep_kind("p1", ["model"])
    carry.flush()
    assert current.urns == []
    provider.get_last_checkpoint.assert_not_called()
    no_state = CarryForward(None, PLATFORM, report=VertexAISourceReport())
    no_state.keep_kind("p1", ["model"])
    no_state.flush()


def test_primitives_carry_forward_is_noop_without_last_checkpoint() -> None:
    carry, current, provider = _carry_forward(None)
    carry.keep_kind("p1", ["model"])
    carry.flush()
    assert current.urns == []
    provider.get_current_checkpoint.assert_called_with(
        StaleEntityRemovalHandler.compute_job_id(PLATFORM)
    )


def test_primitives_carry_forward_loads_last_checkpoint_once() -> None:
    carry, _, provider = _carry_forward([_model_urn("p1.model.a")])
    carry.keep_kind("p1", ["model"])
    carry.keep_urn(_model_urn("p1.model.a"))
    carry.flush()
    assert provider.get_last_checkpoint.call_count == 1


def test_primitives_carry_forward_keep_all_containers_keeps_only_containers() -> None:
    container = get_project_container("p1", PLATFORM, None, ENV).as_urn()
    other_container = get_project_container("p2", PLATFORM, None, ENV).as_urn()
    carry, current, _ = _carry_forward(
        [container, other_container, _model_urn("p1.model.a")]
    )
    carry.keep_all_containers("p1")
    carry.flush()
    assert sorted(current.urns) == sorted([container, other_container])


def test_primitives_carry_forward_keep_urn_keeps_only_that_urn() -> None:
    kept = _model_urn("p1.model.a")
    carry, current, _ = _carry_forward([kept, _model_urn("p1.model.b")])
    carry.keep_urn(kept)
    carry.flush()
    assert current.urns == [kept]


REGION = "us-central1"
_SERVICE = "aiplatform.googleapis.com"


def _service_disabled() -> PermissionDenied:
    return PermissionDenied(
        "API disabled",
        error_info=ErrorInfo(reason="SERVICE_DISABLED", metadata={"service": _SERVICE}),
    )


def _throttled() -> RetryError:
    return RetryError("retries exhausted", cause=ServiceUnavailable("busy"))


def _iam_denied() -> PermissionDenied:
    return PermissionDenied(
        "no role", error_info=ErrorInfo(reason="IAM_PERMISSION_DENIED")
    )


def _make_source(**config: object) -> VertexAISource:
    config.setdefault("project_ids", ["proj-one", "proj-two"])
    config.setdefault("regions", [REGION])
    return VertexAISource(
        ctx=PipelineContext(run_id="scope-test"),
        config=VertexAIConfig.model_validate(
            {
                "include_models": False,
                "include_evaluations": False,
                "use_ml_metadata_for_lineage": False,
                "extract_execution_metrics": False,
                **config,
            }
        ),
    )


def _run_projects(
    source: VertexAISource, errors: dict
) -> Tuple[List[MetadataWorkUnit], MetadataWorkUnit]:
    """Run the project loop; a project in `errors` raises its error when read."""
    healthy_wu = MagicMock(name="healthy-workunit")

    def phase1(resource_type: str) -> Iterator[MetadataWorkUnit]:
        # Every resource type fails, so the whole project is unreadable.
        error = errors.get(source._current_project_id)
        if error is not None:
            raise error
        if resource_type == "training_jobs":
            yield healthy_wu

    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai.get_vertexai_disable_parallelism",
            return_value=True,
        ),
        patch("datahub.ingestion.source.vertexai.vertexai.aiplatform.init"),
        patch.object(source, "_gen_project_workunits", return_value=iter([])),
        patch.object(source, "_fetch_phase1_resource", side_effect=phase1),
    ):
        return list(source.get_workunits_internal()), healthy_wu


def _failure_contexts(source: VertexAISource) -> List[str]:
    return [c for f in source.report.failures for c in f.context]


def test_project_refused_everywhere_listed_is_failure() -> None:
    source = _make_source()
    workunits, healthy_wu = _run_projects(source, {"proj-one": _iam_denied()})

    assert workunits == [healthy_wu]
    assert any("project=proj-one" in c for c in _failure_contexts(source))
    assert not any("project=proj-two" in c for c in _failure_contexts(source))


def test_project_service_disabled_listed_is_failure_naming_service() -> None:
    source = _make_source()
    _run_projects(source, {"proj-one": _service_disabled()})

    assert any(
        "project=proj-one" in c and f"service={_SERVICE}" in c
        for c in _failure_contexts(source)
    )
    assert not source.report.warnings


def test_project_service_disabled_discovered_is_warning_and_kept() -> None:
    source = _make_source(project_ids=["proj-one"])
    # A discovered project is one the recipe does not list.
    source.config.project_ids = []
    source._carry = MagicMock()
    _run_projects(source, {"proj-one": _service_disabled()})

    assert not source.report.failures
    assert any(
        f"service={_SERVICE}" in c for w in source.report.warnings for c in w.context
    )
    source._carry.keep_project.assert_called_once_with("proj-one")


def test_project_failure_still_reads_later_projects() -> None:
    source = _make_source()
    workunits, healthy_wu = _run_projects(source, {"proj-one": ValueError("boom")})

    assert workunits == [healthy_wu]
    assert any("project=proj-one" in c for c in _failure_contexts(source))


@contextmanager
def _locations_client(response: object) -> Iterator[MagicMock]:
    """Stand in for the region-discovery client, so no test reaches Google."""
    with patch(
        "datahub.ingestion.source.vertexai.vertexai.aiplatform_v1.PipelineServiceClient"
    ) as client_cls:
        if isinstance(response, Exception):
            client_cls.return_value.list_locations.side_effect = response
        else:
            client_cls.return_value.list_locations.return_value = response
        yield client_cls


def test_project_region_discovery_parses_list_locations_response() -> None:
    source = _make_source(discover_regions=True)
    response = locations_pb2.ListLocationsResponse(
        locations=[
            locations_pb2.Location(name="projects/proj-one/locations/us-central1"),
            locations_pb2.Location(name="projects/proj-one/locations/europe-west4"),
        ]
    )

    with _locations_client(response):
        regions = source._discover_regions_for_project("proj-one")

    assert regions == ["us-central1", "europe-west4"]


def test_project_region_discovery_uses_recipe_credentials() -> None:
    source = _make_source(discover_regions=True)
    source._credentials = MagicMock(name="recipe-credentials")

    with _locations_client(locations_pb2.ListLocationsResponse()) as client_cls:
        source._discover_regions_for_project("proj-one")

    assert client_cls.call_args.kwargs["credentials"] is source._credentials


# The single-region fallback is the deprecated `region` field.
@pytest.mark.filterwarnings("ignore:.*region.*deprecated")
def test_project_region_discovery_failure_falls_back_and_keeps_project() -> None:
    source = _make_source(discover_regions=True, region=REGION)
    source._carry = MagicMock()

    with _locations_client(_iam_denied()):
        regions = source._resolve_project_regions()

    assert regions["proj-one"] == [REGION]
    assert source.report.warnings
    assert not source.report.failures
    source._carry.keep_project.assert_any_call("proj-one")


def test_project_region_discovery_failure_without_fallback_is_project_failure() -> None:
    source = _make_source(discover_regions=True)

    with _locations_client(_iam_denied()):
        source._discover_regions_for_project("proj-one")

    assert any("project=proj-one" in c for c in _failure_contexts(source))


def test_project_region_discovery_service_disabled_discovered_is_warning() -> None:
    source = _make_source(project_ids=["proj-one"], discover_regions=True)
    source.config.project_ids = []
    source._carry = MagicMock()

    with _locations_client(_service_disabled()):
        source._discover_regions_for_project("proj-one")

    assert not source.report.failures
    assert any(
        f"service={_SERVICE}" in c for w in source.report.warnings for c in w.context
    )
    source._carry.keep_project.assert_called_with("proj-one")


def _no_role() -> PermissionDenied:
    return PermissionDenied(
        "no role",
        error_info=ErrorInfo(
            reason="IAM_PERMISSION_DENIED",
            metadata={"permission": "aiplatform.models.list"},
        ),
    )


def test_project_discovered_no_role_is_warning_and_kept() -> None:
    source = _make_source(project_ids=["proj-one"])
    source.config.project_ids = []
    source._carry = MagicMock()

    _run_projects(source, {"proj-one": _no_role()})

    assert not source.report.failures
    (context,) = _warning_contexts(source)
    assert "project=proj-one" in context
    assert "permission=aiplatform.models.list" in context
    source._carry.keep_project.assert_called_once_with("proj-one")


def test_project_discovered_throttled_is_still_failure() -> None:
    source = _make_source(project_ids=["proj-one"])
    source.config.project_ids = []

    _run_projects(source, {"proj-one": ResourceExhausted("quota")})

    assert any("project=proj-one" in c for c in _failure_contexts(source))


def test_project_region_discovery_refused_discovered_is_warning() -> None:
    source = _make_source(project_ids=["proj-one"], discover_regions=True)
    source.config.project_ids = []
    source._carry = MagicMock()

    with _locations_client(_no_role()):
        source._discover_regions_for_project("proj-one")

    assert not source.report.failures
    assert source.report.warnings
    source._carry.keep_project.assert_called_with("proj-one")


REGIONS = ["us-central1", "europe-west4"]
_PIPELINE_URN = "urn:li:dataFlow:(vertexai,pipeline-a,PROD)"
_EXPERIMENT_URN = "urn:li:container:experiment-a"


def _wu(urn: str) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=urn, aspect=StatusClass(removed=False)
    ).as_workunit()


@dataclass
class RegionRun:
    workunits: List[MetadataWorkUnit]
    model_step_regions: List[Optional[str]]
    carry: CarryForward
    kept: GenericCheckpointState


def _run_regions(
    source: VertexAISource,
    errors: Dict[Tuple[str, str], Exception],
    parallel: bool = False,
    real_containers: bool = False,
    real_pending_lineage: bool = False,
    yield_output: bool = True,
    model_errors: Optional[Dict[str, Exception]] = None,
    previous: Optional[List[str]] = None,
) -> RegionRun:
    """Run every region; `errors` maps (region, phase 1 kind) to what that read
    raises, and `model_errors` maps a region to what its model listing raises.
    With `previous`, a real CarryForward remembers those URNs and `kept` holds
    the ones the run carried forward; otherwise the carry is a mock."""
    kept = GenericCheckpointState()
    if previous is None:
        source._carry = MagicMock()
    else:
        kept = _real_carry(source, previous)
    model_step_regions: List[Optional[str]] = []

    def reader(kind: str, wu: Optional[MetadataWorkUnit]) -> MagicMock:
        def read(*_args: object) -> Iterator[MetadataWorkUnit]:
            error = errors.get((source._current_region or "", kind))
            if error is not None:
                raise error
            if wu is not None:
                yield wu

        return MagicMock(side_effect=read)

    def model_step() -> Iterator[MetadataWorkUnit]:
        model_step_regions.append(source._current_region)
        error = (model_errors or {}).get(source._current_region or "")
        if error is not None:
            raise error
        return iter([])

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "datahub.ingestion.source.vertexai.vertexai.get_vertexai_disable_parallelism",
                return_value=not parallel,
            )
        )
        stack.enter_context(
            patch("datahub.ingestion.source.vertexai.vertexai.aiplatform.init")
        )
        if not real_containers:
            stack.enter_context(
                patch.object(source, "_gen_project_workunits", return_value=iter([]))
            )
        if not real_pending_lineage:
            stack.enter_context(
                patch.object(
                    source.model_extractor,
                    "emit_pending_lineage_updates",
                    return_value=iter([]),
                )
            )
        stack.enter_context(
            patch.object(
                source.training_extractor,
                "get_workunits",
                reader("training_jobs", None),
            )
        )
        stack.enter_context(
            patch.object(
                source.pipeline_extractor,
                "get_workunits",
                reader("pipelines", _wu(_PIPELINE_URN) if yield_output else None),
            )
        )
        stack.enter_context(
            patch.object(
                source.experiment_extractor,
                "get_experiment_workunits",
                reader("experiments", _wu(_EXPERIMENT_URN) if yield_output else None),
            )
        )
        stack.enter_context(
            patch.object(
                source.experiment_extractor,
                "get_experiment_run_workunits",
                return_value=iter([]),
            )
        )
        stack.enter_context(
            patch.object(source.model_extractor, "get_model_workunits", model_step)
        )
        stack.enter_context(
            patch.object(
                source.model_extractor,
                "get_evaluation_workunits",
                return_value=iter([]),
            )
        )
        workunits = list(source.get_workunits_internal())
    return RegionRun(workunits, model_step_regions, source._carry, kept)


def _region_source(**config: object) -> VertexAISource:
    config.setdefault("include_models", True)
    config.setdefault("regions", REGIONS)
    return _make_source(project_ids=["proj-one"], **config)


@dataclass
class SourceRun:
    workunits: List[MetadataWorkUnit]
    model_step_regions: List[Optional[str]]


def _run_source(source: VertexAISource, *patches: Any) -> SourceRun:
    """Runs the whole source with `patches` standing in for its Google calls.
    The model step is replaced by a recorder of the regions it ran in."""
    model_step_regions: List[Optional[str]] = []

    def model_step() -> Iterator[MetadataWorkUnit]:
        model_step_regions.append(source._current_region)
        return iter([])

    with ExitStack() as stack:
        for patched in (
            patch("datahub.ingestion.source.vertexai.vertexai.aiplatform.init"),
            patch.object(source, "_gen_project_workunits", return_value=iter([])),
            patch.object(source.model_extractor, "get_model_workunits", model_step),
            patch.object(
                source.model_extractor,
                "emit_pending_lineage_updates",
                return_value=iter([]),
            ),
            *patches,
        ):
            stack.enter_context(patched)
        workunits = list(source.get_workunits_internal())
    return SourceRun(workunits, model_step_regions)


def test_region_parallel_worker_failure_keeps_sibling_output() -> None:
    source = _region_source()
    run = _run_regions(
        source,
        {(REGIONS[0], "training_jobs"): RetryError("x", cause=ServiceUnavailable("y"))},
        parallel=True,
    )

    urns = {wu.get_urn() for wu in run.workunits}
    assert {_PIPELINE_URN, _EXPERIMENT_URN} <= urns
    assert not source.report.failures
    assert any(
        f"region={REGIONS[0]}, resource=training_jobs" in c
        for w in source.report.warnings
        for c in w.context
    )


def test_region_temporary_training_loss_holds_back_models() -> None:
    source = _region_source()

    run = _run_regions(
        source,
        {(REGIONS[0], "training_jobs"): _throttled()},
        previous=_EVERY_URN,
    )

    assert run.model_step_regions == [REGIONS[1]]
    assert sorted(run.kept.urns) == sorted(
        [*_TRAINING_URNS, *_MODEL_STEP_URNS, _CONTAINER_URN, _PROJECT_CONTAINER_URN]
    )


def test_region_item_loss_does_not_hold_back_models() -> None:
    source = _region_source(
        include_pipelines=False, include_experiments=False, regions=[REGIONS[0]]
    )

    def gapic_list(cls: type, *_args: object, **_kw: object) -> list:
        return [gen_mock_training_automl_job()] if "AutoML" in cls.__name__ else []

    run = _run_source(
        source,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_training_extractor.rate_limited_gapic_list",
            side_effect=gapic_list,
        ),
        patch(
            "datahub.ingestion.source.vertexai.vertexai_training_extractor.Model",
            side_effect=NotFound("model gone"),
        ),
    )

    assert run.model_step_regions == [REGIONS[0]]
    contexts = _warning_contexts(source)
    assert contexts
    assert all("resource=automl_output_model" in c for c in contexts)


def test_region_experiments_loss_never_holds_back_models() -> None:
    source = _region_source()

    run = _run_regions(
        source,
        {(REGIONS[0], "experiments"): _throttled()},
        previous=_EVERY_URN,
    )

    # Experiment links never reach written models, so every region's models
    # are written; only the experiments' own folders are kept.
    assert run.model_step_regions == REGIONS
    assert sorted(run.kept.urns) == sorted([_CONTAINER_URN, _PROJECT_CONTAINER_URN])


def test_region_failure_keeps_whole_kind_for_project() -> None:
    source = _region_source()

    run = _run_regions(
        source,
        {(REGIONS[1], "pipelines"): _throttled()},
        previous=_EVERY_URN,
    )

    assert run.model_step_regions == [REGIONS[0]]
    assert sorted(run.kept.urns) == sorted(
        [*_FLOW_URNS, *_MODEL_STEP_URNS, _CONTAINER_URN, _PROJECT_CONTAINER_URN]
    )


def test_region_keep_model_step_keeps_models_deployments_evaluations() -> None:
    source = _region_source()
    kept = _real_carry(source, _EVERY_URN)

    source._keep_model_step("proj-one")
    source._carry.flush()

    assert sorted(kept.urns) == sorted(
        [*_MODEL_STEP_URNS, _CONTAINER_URN, _PROJECT_CONTAINER_URN]
    )


def test_region_every_kind_failed_listed_project_is_failure() -> None:
    source = _region_source()
    errors: Dict[Tuple[str, str], Exception] = {
        (region, kind): _iam_denied()
        for region in REGIONS
        for kind in ("training_jobs", "experiments", "pipelines")
    }
    run = _run_regions(
        source,
        errors,
        real_containers=True,
        model_errors={region: _iam_denied() for region in REGIONS},
    )

    # With real containers, any emitted container would show up here.
    assert run.workunits == []
    assert not source.report.warnings
    assert any("project=proj-one" in c for c in _failure_contexts(source))
    _mock(run.carry.keep_project).assert_called_with("proj-one")
    # The project itself, then each lost read: every one repeats on every run.
    lost = source.report.lost_reads["proj-one"]
    assert all("loss=lasting" in c for c in lost)
    assert "region=" not in lost[0]
    # Experiments are read once per project, in the first region.
    expected = [(REGIONS[0], "experiments")] + [
        (region, kind) for region in REGIONS for kind in ("training_jobs", "models")
    ]
    for region, kind in expected:
        assert any(f"region={region}, resource={kind}" in c for c in lost)


def test_region_every_kind_refused_discovered_project_is_warning() -> None:
    source = _region_source()
    source.config.project_ids = []
    errors: Dict[Tuple[str, str], Exception] = {
        (region, kind): _no_role()
        for region in REGIONS
        for kind in ("training_jobs", "experiments", "pipelines")
    }
    run = _run_regions(
        source, errors, model_errors={region: _no_role() for region in REGIONS}
    )

    assert not source.report.failures
    assert len(_warning_contexts(source)) == 1
    _mock(run.carry.keep_project).assert_called_with("proj-one")


def test_region_one_kind_succeeds_project_not_unreadable() -> None:
    source = _region_source(include_models=False)
    errors: Dict[Tuple[str, str], Exception] = {
        (region, kind): _iam_denied()
        for region in REGIONS
        for kind in ("training_jobs", "experiments")
    }
    run = _run_regions(source, errors)

    assert "Project could not be read" not in {f.title for f in source.report.failures}
    assert _PIPELINE_URN in {wu.get_urn() for wu in run.workunits}


def test_region_held_back_model_gets_no_lineage_only_write() -> None:
    source = _region_source()
    held_back = _model_urn("proj-one.model.m1_1")
    source.model_usage_tracker.track_model_usage(
        held_back, _job_urn(_flow_urn("f"), "t")
    )
    run = _run_regions(
        source,
        {(REGIONS[0], "training_jobs"): _throttled()},
        real_pending_lineage=True,
    )

    assert run.model_step_regions == [REGIONS[1]]
    assert held_back not in {wu.get_urn() for wu in run.workunits}
    assert source.model_usage_tracker.get_model_usage(held_back)


def test_region_partly_readable_project_warns_once_per_lost_read() -> None:
    source = _region_source(include_models=False)
    source.config.project_ids = []
    errors: Dict[Tuple[str, str], Exception] = {
        (region, "training_jobs"): _iam_denied() for region in REGIONS
    }
    _run_regions(source, errors)

    assert not source.report.failures
    contexts = [c for w in source.report.warnings for c in w.context]
    assert len(contexts) == len(REGIONS)
    for region in REGIONS:
        assert any(f"region={region}, resource=training_jobs" in c for c in contexts)


def test_region_empty_healthy_project_still_emits_containers() -> None:
    source = _region_source(include_models=False)
    run = _run_regions(source, {}, real_containers=True, yield_output=False)

    assert not source.report.failures
    assert not source.report.warnings
    assert run.workunits


def test_region_carry_forward_keep_all_flows_keeps_flows_and_tasks_only() -> None:
    flow = _flow_urn("pipeline-a")
    job = _job_urn(flow, "task-a")
    container = get_project_container("p1", PLATFORM, None, ENV).as_urn()
    carry, current, _ = _carry_forward([flow, job, container, _model_urn("p1.model.a")])
    carry.keep_all_flows("p1")
    carry.flush()
    assert sorted(current.urns) == sorted([flow, job])


def test_region_unknown_phase1_read_is_refused_not_dropped() -> None:
    source = _region_source()
    with pytest.raises(ValueError):
        source._keep_phase1_kind("proj-one", "not_a_read")


def test_flush_with_many_exact_keeps_uses_a_set_not_closures() -> None:
    previous = [_model_urn(f"p1.model.m{i}") for i in range(200)]
    kept = previous[::2]
    carry, current, _ = _carry_forward(previous)

    for urn in kept:
        carry.keep_urn(urn)
    carry.keep_urn("urn:li:mlModel:(urn:li:dataPlatform:vertexai,not-previous,PROD)")

    carry.flush()

    assert set(current.urns) == set(kept)


def test_primitives_carry_forward_thread_keep_applies_on_flush() -> None:
    kept = _model_urn("p1.model.a")
    carry, current, _ = _carry_forward([kept])
    worker = threading.Thread(target=carry.keep_urn, args=(kept,))
    worker.start()
    worker.join()
    assert current.urns == []
    carry.flush()
    assert current.urns == [kept]


_TRAINING_MODULE = "datahub.ingestion.source.vertexai.vertexai_training_extractor"


def _training_source(**config: object) -> VertexAISource:
    source = _make_source(
        project_ids=["proj-one"],
        include_pipelines=False,
        include_experiments=False,
        **config,
    )
    source._current_project_id = "proj-one"
    source._current_region = REGION
    source.training_extractor._carry = MagicMock()
    return source


def _custom_jobs(count: int) -> List[Any]:
    jobs = []
    for index in range(count):
        job = gen_mock_training_custom_job()
        _mock(job).name = f"mock_training_job_{index}"
        jobs.append(job)
    return jobs


def _attach_ml_metadata(source: VertexAISource, client: MagicMock) -> None:
    source._ml_metadata_helper = MLMetadataHelper(
        metadata_client=client,
        config=MLMetadataConfig(project_id="proj-one", region=REGION),
        uri_parser=source.uri_parser,
        report=source.report,
        stopped_scopes=source._stopped,
        scope=source._current_scope,
    )


def _read_training(
    source: VertexAISource,
    lists: Dict[str, Any],
    model: Optional[Exception] = None,
) -> List[MetadataWorkUnit]:
    """Run the training extractor; `lists` maps a class name to its jobs or error."""

    def gapic_list(cls: type, *_args: object, **_kw: object) -> list:
        listed = lists.get(cls.__name__, [])
        if isinstance(listed, Exception):
            raise listed
        return listed

    with ExitStack() as stack:
        stack.enter_context(
            patch(f"{_TRAINING_MODULE}.rate_limited_gapic_list", side_effect=gapic_list)
        )
        if model is not None:
            stack.enter_context(patch(f"{_TRAINING_MODULE}.Model", side_effect=model))
        return list(source.training_extractor.get_workunits())


def _aspects(workunits: List[MetadataWorkUnit], aspect_type: type) -> List[Any]:
    return [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


def test_training_mlmd_refused_writes_jobs_without_metrics_and_warns_per_job() -> None:
    source = _training_source(extract_execution_metrics=True)
    client = MagicMock()
    client.list_executions.side_effect = _iam_denied()
    _attach_ml_metadata(source, client)

    workunits = _read_training(source, {"CustomJob": _custom_jobs(2)})

    runs = _aspects(workunits, MLTrainingRunPropertiesClass)
    assert len(runs) == 2
    assert all(not run.hyperParams and not run.trainingMetrics for run in runs)
    assert client.list_executions.call_count == 2
    assert not source.report.failures
    contexts = _warning_contexts(source)
    assert len(contexts) == 2
    assert all(
        f"region={REGION}" in c and "extract_execution_metrics" in c for c in contexts
    )


def test_training_mlmd_transient_keeps_job_previous_version() -> None:
    source = _training_source(extract_execution_metrics=True)
    client = MagicMock()
    client.list_executions.side_effect = RetryError(
        "retries exhausted", cause=ServiceUnavailable("busy")
    )
    _attach_ml_metadata(source, client)
    jobs = _custom_jobs(2)

    workunits = _read_training(source, {"CustomJob": jobs})

    assert workunits == []
    assert client.list_executions.call_count == 1


def test_training_automl_output_model_not_found_warns_and_writes_job_without_link() -> (
    None
):
    source = _training_source()
    workunits = _read_training(
        source,
        {
            "AutoMLTabularTrainingJob": [gen_mock_training_automl_job()],
            "TabularDataset": [gen_mock_dataset()],
        },
        model=NotFound("model gone"),
    )

    assert _aspects(workunits, MLTrainingRunPropertiesClass)
    assert _aspects(workunits, DataProcessInstanceInputClass)
    assert not _aspects(workunits, DataProcessInstanceOutputClass)
    (context,) = _warning_contexts(source)
    assert "project=proj-one" in context
    assert "resource=automl_output_model" in context
    assert not source.report.failures


def test_training_automl_output_model_refused_is_warning_not_failure() -> None:
    source = _training_source()
    workunits = _read_training(
        source,
        {"AutoMLTabularTrainingJob": [gen_mock_training_automl_job()]},
        model=_iam_denied(),
    )

    assert _aspects(workunits, MLTrainingRunPropertiesClass)
    assert not source.report.failures
    assert len(_warning_contexts(source)) == 1


def test_training_dataset_list_refused_keeps_datasets_writes_jobs_without_input() -> (
    None
):
    source = _training_source()
    workunits = _read_training(
        source,
        {
            "AutoMLTabularTrainingJob": [gen_mock_training_automl_job()],
            "TabularDataset": _iam_denied(),
        },
        model=NotFound("model gone"),
    )

    assert _aspects(workunits, MLTrainingRunPropertiesClass)
    assert not _aspects(workunits, DataProcessInstanceInputClass)
    _mock(source.training_extractor._carry.keep_kind).assert_called_once_with(
        "proj-one", [ResourceKinds.DATASET]
    )
    assert not source.report.failures
    contexts = _warning_contexts(source)
    assert len([c for c in contexts if "resource=training_datasets" in c]) == 1


def test_training_list_type_refused_is_whole_kind_loss() -> None:
    source = _training_source()
    with pytest.raises(PermissionDenied):
        _read_training(source, {"CustomJob": _iam_denied()})


def test_training_list_not_found_is_whole_kind_loss() -> None:
    source = _training_source(include_models=True, regions=[REGION])
    kept = _real_carry(source, [*_TRAINING_URNS, *_MODEL_STEP_URNS])

    def gapic_list(cls: type, *_args: object, **_kw: object) -> list:
        if cls.__name__ == "CustomJob":
            raise NotFound("no custom jobs here")
        return []

    run = _run_source(
        source,
        patch(f"{_TRAINING_MODULE}.rate_limited_gapic_list", side_effect=gapic_list),
    )

    # A lasting loss writes the models and fails the listed project.
    assert run.model_step_regions == [REGION]
    assert set(_TRAINING_URNS) <= set(kept.urns)
    assert any("resource=training_jobs" in c for c in _failure_contexts(source))


def _read_detail(
    read: Any, stopped: Optional[StoppedScopes] = None
) -> Tuple[Any, VertexAISourceReport, StoppedScopes]:
    report = VertexAISourceReport()
    stopped = stopped or StoppedScopes()
    result = DetailReader(report, stopped, lambda: Scope("proj-one", REGION))(
        "ml_metadata", read, item="item-1"
    )
    return result, report, stopped


def test_primitives_read_detail_project_wide_error_is_raised() -> None:
    with pytest.raises(PermissionDenied):
        _read_detail(MagicMock(side_effect=_service_disabled()))


def test_primitives_read_detail_item_lasting_warns_without_stopping() -> None:
    result, report, stopped = _read_detail(MagicMock(side_effect=TypeError("bad")))

    assert result.value is None
    assert not result.keep_previous
    assert len(report.warnings) == 1
    assert (
        stopped.stop_kind(Scope(project="proj-one", region=REGION, kind="ml_metadata"))
        is None
    )


def test_primitives_read_detail_warning_names_the_stopped_read() -> None:
    _, report, _ = _read_detail(MagicMock(side_effect=_iam_denied()))

    contexts = [c for w in report.warnings for c in w.context]
    assert len(contexts) == 1
    assert "resource=ml_metadata" in contexts[0]
    assert "item=item-1" in contexts[0]


@pytest.mark.parametrize(
    "stop_kind,keep_previous",
    [(ErrorKind.TRANSIENT, True), (ErrorKind.REFUSED, False)],
)
def test_primitives_read_detail_stopped_scope_skips_read(
    stop_kind: ErrorKind, keep_previous: bool
) -> None:
    stopped = StoppedScopes()
    stopped.stop(
        Scope(project="proj-one", region=REGION, kind="ml_metadata"),
        stop_kind,
        ServiceUnavailable("x"),
    )
    read = MagicMock()

    result, report, _ = _read_detail(read, stopped)

    read.assert_not_called()
    assert result.value is None
    assert result.keep_previous == keep_previous
    assert not report.warnings


def _run_training_regions(
    source: VertexAISource, mlmd_errors: Dict[str, Exception]
) -> Tuple[List[Optional[str]], Dict[str, MagicMock]]:
    """Two regions with custom jobs; `mlmd_errors` maps a region to what its
    ML Metadata list raises."""
    clients: Dict[str, MagicMock] = {}

    def make_client(client_options: Dict[str, str], **_kw: object) -> MagicMock:
        region = client_options["api_endpoint"].split("-aiplatform")[0]
        client = MagicMock()
        if region in mlmd_errors:
            client.list_executions.side_effect = mlmd_errors[region]
        else:
            client.list_executions.return_value = []
        clients[region] = client
        return client

    def gapic_list(cls: type, *_args: object, **_kw: object) -> list:
        return _custom_jobs(1) if cls.__name__ == "CustomJob" else []

    run = _run_source(
        source,
        patch(
            "datahub.ingestion.source.vertexai.vertexai.MetadataServiceClient",
            side_effect=make_client,
        ),
        patch(f"{_TRAINING_MODULE}.rate_limited_gapic_list", side_effect=gapic_list),
    )
    return run.model_step_regions, clients


def _training_regions_source() -> VertexAISource:
    return _make_source(
        project_ids=["proj-one"],
        regions=REGIONS,
        include_models=True,
        include_pipelines=False,
        include_experiments=False,
        extract_execution_metrics=True,
    )


def test_listed_project_with_every_item_kept_after_a_stop_warns() -> None:
    source = _make_source(
        project_ids=["proj-one"],
        regions=[REGION],
        include_models=False,
        include_evaluations=False,
        include_pipelines=False,
        include_experiments=False,
        extract_execution_metrics=True,
    )

    _run_training_regions(source, {REGION: _throttled()})

    # The lists were read, so a temporary loss of every item is not a project
    # failure (C14); the summary keeps the gap visible.
    assert not source.report.failures
    assert source.report.warnings
    (lost,) = source.report.lost_reads["proj-one"]
    assert f"region={REGION}, resource=ml_metadata" in lost
    assert "loss=temporary" in lost


def test_lost_reads_lists_each_lost_read_per_project_in_full() -> None:
    report = VertexAISourceReport()
    stopped = StoppedScopes()
    ledger = ReadLedger(report=report, stopped=stopped)
    ledger.record_read("proj-one", REGION, "training_jobs", error=_iam_denied())
    read_one = DetailReader(report, stopped, lambda: Scope("proj-one", REGION))
    # More refusals than the report keeps as samples.
    for number in range(12):
        read_one(
            DetailCalls.EVALUATION_LOOKUP,
            MagicMock(side_effect=_iam_denied()),
            item=f"model-{number}",
        )
    DetailReader(report, stopped, lambda: Scope("proj-two", REGION))(
        DetailCalls.ENDPOINT_LIST, MagicMock(side_effect=_throttled())
    )

    ledger.record_lost_reads("proj-one")
    ledger.record_lost_reads("proj-two")

    # One entry per lost read and region, not per item.
    lists, evaluations = sorted(
        report.lost_reads["proj-one"], key=lambda c: "evaluation" in c
    )
    assert "resource=training_jobs" in lists and "loss=lasting" in lists
    assert "switch=include_training_jobs" in lists
    assert "resource=evaluation_lookup" in evaluations
    assert "loss=lasting" in evaluations
    (endpoints,) = report.lost_reads["proj-two"]
    assert "resource=endpoint_list" in endpoints and "loss=temporary" in endpoints


def _lose_details(call_kind: str, items: int) -> VertexAISource:
    source = _training_source()
    read = DetailReader(source.report, source._stopped, source._current_scope)
    for number in range(items):
        item = f"item-{number}" if items > 1 else None
        read(call_kind, MagicMock(side_effect=NotFound("gone")), item=item)
    return source


def _lose_runs_of_two_experiments() -> VertexAISource:
    source = _training_source()
    for experiment in ("experiment-1", "experiment-2"):
        read_list_item(
            Scope("proj-one", REGION, item=experiment),
            MagicMock(side_effect=NotFound("gone")),
            report=source.report,
            stopped=source._stopped,
            call_kind="experiments",
        )
    return source


def _ml_metadata(client: MagicMock, **config: object) -> VertexAISource:
    source = _training_source()
    helper = MLMetadataHelper(
        metadata_client=client,
        config=MLMetadataConfig(project_id="proj-one", region=REGION, **config),
        uri_parser=MagicMock(),
        report=source.report,
        stopped_scopes=source._stopped,
        scope=source._current_scope,
    )
    helper.get_job_lineage_metadata(MagicMock(display_name="job"))
    return source


def _lose_executions() -> VertexAISource:
    client = MagicMock()
    client.list_executions.side_effect = NotFound("no store")
    return _ml_metadata(client)


def _hit_the_execution_cache_cap() -> VertexAISource:
    source = _training_source()
    client = MagicMock()
    client.list_executions.return_value = [_m1_execution()]
    MLMetadataHelper(
        metadata_client=client,
        config=MLMetadataConfig(
            project_id="proj-one", region=REGION, max_execution_search_limit=1
        ),
        uri_parser=MagicMock(),
        report=source.report,
        stopped_scopes=source._stopped,
        scope=source._current_scope,
    )._load_execution_cache()
    return source


def _break_an_automl_config() -> VertexAISource:
    source = _training_source()
    job = MagicMock()
    job.name = "job-1"
    job.to_dict.return_value = {"inputDataConfig": "not-an-object"}
    source.training_extractor._parse_automl_config(job)
    return source


def _fail_the_ml_metadata_client() -> VertexAISource:
    source = _make_source(
        project_ids=["proj-one"],
        include_training_jobs=False,
        include_pipelines=False,
        include_experiments=False,
        use_ml_metadata_for_lineage=True,
    )
    source._project_to_regions = {"proj-one": [REGION]}
    source._current_project_id = "proj-one"
    with (
        patch("datahub.ingestion.source.vertexai.vertexai.aiplatform.init"),
        patch(
            "datahub.ingestion.source.vertexai.vertexai.MetadataServiceClient",
            side_effect=InvalidArgument("bad endpoint"),
        ),
    ):
        list(source._gen_project_resource_workunits("proj-one"))
    return source


def _fail_region_discovery() -> VertexAISource:
    source = _make_source(project_ids=["proj-one"], discover_regions=True)
    source.config.region = REGION
    source._carry = MagicMock()
    with _locations_client(_iam_denied()):
        source._discover_regions_for_project("proj-one")
    return source


# Every warning that loses data, with the read and the switch its entry names.
_LOSSES: Dict[str, Tuple[Callable[[], VertexAISource], str, Optional[str]]] = {
    "endpoint_list": (lambda: _lose_details("endpoint_list", 1), "endpoint_list", None),
    "dataset_list": (
        lambda: _lose_details("training_datasets", 1),
        "training_datasets",
        None,
    ),
    "every_evaluation": (
        lambda: _lose_details("evaluation_lookup", 3),
        "evaluation_lookup",
        "include_evaluations",
    ),
    "every_model_version": (
        lambda: _lose_details("model_lookup", 2),
        "model_lookup",
        None,
    ),
    "runs_of_two_experiments": (_lose_runs_of_two_experiments, "experiments", None),
    "executions": (_lose_executions, "ml_metadata", None),
    "execution_cache_cap": (
        _hit_the_execution_cache_cap,
        "ml_metadata_execution_cache",
        "ml_metadata_max_execution_search_limit",
    ),
    "automl_job_config": (_break_an_automl_config, "automl_job_config", None),
    "ml_metadata_client": (_fail_the_ml_metadata_client, "ml_metadata", None),
    "region_discovery": (_fail_region_discovery, "region_discovery", None),
}


@pytest.mark.filterwarnings("ignore:.*region.*deprecated")
@pytest.mark.parametrize("case", sorted(_LOSSES))
def test_every_warning_that_loses_data_is_in_lost_reads(case: str) -> None:
    lose, read, switch = _LOSSES[case]
    source = lose()

    source._ledger.record_lost_reads("proj-one")

    assert source.report.warnings
    # One entry per read and region, however many items it cost.
    (lost,) = source.report.lost_reads["proj-one"]
    assert f"resource={read}," in lost
    assert lost.endswith("loss=lasting")
    if switch:
        assert f"switch={switch}," in lost


def test_lost_reads_keep_one_entry_per_read_and_region() -> None:
    report = VertexAISourceReport()
    stopped = StoppedScopes()
    ledger = ReadLedger(report=report, stopped=stopped)
    lookup = Scope("proj-one", REGION, kind="model_lookup")
    stopped.lost(lookup, ErrorKind.ITEM_LASTING, NotFound("first"))
    stopped.lost(lookup, ErrorKind.REFUSED, _iam_denied())
    ((_, first, _),) = stopped.lost_in("proj-one")
    assert first is ErrorKind.ITEM_LASTING
    stopped.stop(lookup, ErrorKind.TRANSIENT, _throttled())
    # One experiment's runs gone, and the experiments list itself lost.
    stopped.lost(
        Scope("proj-one", REGION, kind="experiments"),
        ErrorKind.ITEM_LASTING,
        NotFound("gone"),
    )
    ledger.record_read("proj-one", REGION, "experiments", error=_throttled())

    ledger.record_lost_reads("proj-one")

    # A stop wins over an earlier loss, and the list read over its items.
    experiments, lookups = sorted(report.lost_reads["proj-one"])
    assert "resource=model_lookup" in lookups and "loss=temporary" in lookups
    assert "resource=experiments" in experiments
    assert "loss=temporary" in experiments


def test_a_project_that_raises_still_lists_its_lost_reads() -> None:
    source = _training_source()
    source._carry = MagicMock()

    def raise_after_a_loss(_project_id: str) -> Iterator[MetadataWorkUnit]:
        DetailReader(source.report, source._stopped, source._current_scope)(
            "endpoint_list", MagicMock(side_effect=NotFound("gone"))
        )
        raise _service_disabled()
        yield

    with patch.object(
        source, "_gen_workunits_for_project", side_effect=raise_after_a_loss
    ):
        list(source.get_workunits_internal())

    lost = source.report.lost_reads["proj-one"]
    assert any("resource=endpoint_list" in c for c in lost)


def test_training_transient_stop_keeps_the_jobs_and_writes_the_models() -> None:
    source = _training_regions_source()
    kept = _real_carry(source, _EVERY_URN)

    model_step_regions, clients = _run_training_regions(
        source, {REGIONS[0]: _throttled()}
    )

    # The kept custom jobs name no dataset, and the models' links are rebuilt
    # from the job listing, so the models are written and nothing is kept.
    assert model_step_regions == REGIONS
    assert kept.urns == []
    assert clients[REGIONS[0]].list_executions.call_count == 1
    assert clients[REGIONS[1]].list_executions.call_count >= 1
    assert not source.report.failures
    contexts = [c for w in source.report.warnings for c in w.context]
    assert any(f"region={REGIONS[0]}, resource=ml_metadata" in c for c in contexts)


def test_training_refused_stop_does_not_hold_back_models() -> None:
    source = _training_regions_source()

    model_step_regions, clients = _run_training_regions(
        source, {REGIONS[0]: _iam_denied()}
    )

    assert model_step_regions == REGIONS
    assert clients[REGIONS[0]].list_executions.call_count == 1
    assert clients[REGIONS[1]].list_executions.call_count >= 1


def test_training_job_lookup_transient_keeps_job_before_other_calls() -> None:
    source = _training_source(extract_execution_metrics=True)
    client = MagicMock()
    _attach_ml_metadata(source, client)
    jobs = _custom_jobs(2)

    with patch(
        f"{_TRAINING_MODULE}.get_job_result_status",
        side_effect=RetryError("retries exhausted", cause=ServiceUnavailable("x")),
    ):
        workunits = _read_training(source, {"CustomJob": jobs})

    assert workunits == []
    client.list_executions.assert_not_called()


def test_training_job_lookup_transient_stop_is_not_retried_for_other_jobs() -> None:
    source = _training_source()
    jobs = _custom_jobs(3)

    with patch(
        f"{_TRAINING_MODULE}.get_job_result_status",
        side_effect=RetryError("retries exhausted", cause=ServiceUnavailable("x")),
    ) as lookup:
        workunits = _read_training(source, {"CustomJob": jobs})

    assert workunits == []
    assert lookup.call_count == 1


def test_training_output_model_transient_keeps_job_and_dataset() -> None:
    source = _training_source()
    job = gen_mock_training_automl_job()

    workunits = _read_training(
        source,
        {"AutoMLTabularTrainingJob": [job], "TabularDataset": [gen_mock_dataset()]},
        model=RetryError("retries exhausted", cause=ServiceUnavailable("x")),
    )

    assert workunits == []
    carry = source.training_extractor._carry
    _mock(carry.keep_urn).assert_any_call(
        source.urn_builder.make_dataset_urn(gen_mock_dataset().name)
    )


def test_training_dataset_list_transient_keeps_jobs_that_need_a_dataset() -> None:
    source = _training_source()
    job = gen_mock_training_automl_job()

    workunits = _read_training(
        source,
        {
            "AutoMLTabularTrainingJob": [job],
            "TabularDataset": RetryError("x", cause=ServiceUnavailable("y")),
        },
        model=NotFound("model gone"),
    )

    assert workunits == []
    assert source.training_extractor._datasets_keep_previous
    carry = source.training_extractor._carry
    _mock(carry.keep_kind).assert_any_call("proj-one", [ResourceKinds.DATASET])


def test_training_listed_project_with_only_transient_stop_is_a_warning() -> None:
    source = _make_source(
        project_ids=["proj-one"],
        regions=[REGION],
        include_models=True,
        include_pipelines=False,
        include_experiments=False,
        extract_execution_metrics=True,
    )

    model_step_regions, _ = _run_training_regions(source, {REGION: _throttled()})

    # The models were read, so the project was not unreadable: a temporary
    # loss is a warning.
    assert model_step_regions == [REGION]
    assert not source.report.failures
    assert any(
        "project=proj-one" in c and "resource=ml_metadata" in c
        for c in _warning_contexts(source)
    )


def test_training_discovered_project_with_transient_stop_and_refused_pipelines() -> (
    None
):
    source = _make_source(
        project_ids=["proj-one"],
        regions=[REGION],
        include_models=True,
        include_pipelines=True,
        include_experiments=False,
        extract_execution_metrics=True,
    )
    source.config.project_ids = []

    with patch.object(
        source.pipeline_extractor, "get_workunits", side_effect=_no_role()
    ):
        _run_training_regions(source, {REGION: _throttled()})

    # The models were read, so the discovered project gets warnings only.
    assert not source.report.failures
    assert _warning_contexts(source)


_MODEL_MODULE = "datahub.ingestion.source.vertexai.vertexai_model_extractor"


def _model_source(**config: object) -> VertexAISource:
    config.setdefault("include_models", True)
    source = _make_source(
        project_ids=["proj-one"],
        include_training_jobs=False,
        include_pipelines=False,
        include_experiments=False,
        **config,
    )
    source._current_project_id = "proj-one"
    source._current_region = REGION
    source.model_extractor._carry = MagicMock()
    return source


def _endpoint_serving(model: Any) -> Any:
    endpoint = gen_mock_endpoint()
    _mock(endpoint.list_models).return_value = [MagicMock(model=model.resource_name)]
    return endpoint


def _read_models(
    source: VertexAISource,
    models: Any,
    endpoints: Any = None,
    evaluations: Any = None,
) -> List[MetadataWorkUnit]:
    """Run the model step; `models`, `endpoints` and `evaluations` are a list, or an
    error their listing raises."""

    def gapic_list(cls: type, *_args: object, **_kw: object) -> Any:
        by_class: Dict[type, Any] = {
            aiplatform.Model: models,
            aiplatform.Endpoint: endpoints or [],
            ModelEvaluation: evaluations or [],
        }
        listed = by_class[cls]
        if isinstance(listed, Exception):
            raise listed
        return listed

    with patch(f"{_MODEL_MODULE}.rate_limited_gapic_list", side_effect=gapic_list):
        extractor = source.model_extractor
        return [
            *(extractor.get_model_workunits() if source.config.include_models else []),
            *(
                extractor.get_evaluation_workunits()
                if source.config.include_evaluations
                else []
            ),
        ]


def _written(workunits: List[MetadataWorkUnit]) -> List[str]:
    return sorted({wu.get_urn() for wu in workunits})


def _model_properties(workunits: List[MetadataWorkUnit]) -> Dict[str, Any]:
    return {
        wu.get_urn(): wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, MLModelPropertiesClass)
    }


def _kept_urns(source: VertexAISource) -> List[str]:
    return [
        c.args[0] for c in _mock(source.model_extractor._carry.keep_urn).call_args_list
    ]


def test_model_endpoint_list_refused_writes_models_without_deployments() -> None:
    source = _model_source()
    models = [gen_mock_model(1), gen_mock_model(2)]

    workunits = _read_models(source, models, endpoints=_iam_denied())

    properties = _model_properties(workunits)
    assert len(properties) == 2
    assert all(not p.deployments for p in properties.values())
    assert not any(
        urn.startswith("urn:li:mlModelDeployment:") for urn in _written(workunits)
    )
    _mock(source.model_extractor._carry.keep_kind).assert_called_with(
        "proj-one", [ResourceKinds.ENDPOINT]
    )
    contexts = [c for w in source.report.warnings for c in w.context]
    assert len([c for c in contexts if "resource=endpoint_list" in c]) == 1


def test_model_endpoint_list_transient_keeps_models_and_deployments() -> None:
    source = _model_source()
    models = [gen_mock_model(1), gen_mock_model(2)]

    workunits = _read_models(source, models, endpoints=_throttled())

    assert not _model_properties(workunits)
    assert not any(
        urn.startswith("urn:li:mlModelGroup:") for urn in _written(workunits)
    )
    carry = source.model_extractor._carry
    _mock(carry.keep_kind).assert_any_call("proj-one", [ResourceKinds.ENDPOINT])
    assert len(_mock(carry.keep_model_versions).call_args_list) == 2
    assert len(_kept_urns(source)) == 2


def test_model_endpoint_list_not_found_keeps_deployments_and_warns() -> None:
    source = _model_source()

    workunits = _read_models(source, [gen_mock_model(1)], endpoints=NotFound("gone"))

    assert len(_model_properties(workunits)) == 1
    _mock(source.model_extractor._carry.keep_kind).assert_called_with(
        "proj-one", [ResourceKinds.ENDPOINT]
    )
    (context,) = _warning_contexts(source)
    assert "resource=endpoint_list" in context


def test_model_versions_throttled_keeps_model_group_and_rest_of_region() -> None:
    source = _model_source()
    models = [gen_mock_model(1), gen_mock_model(2), gen_mock_model(3)]
    models[0].versioning_registry.client.list_model_versions.side_effect = _throttled()

    workunits = _read_models(source, models)

    assert _model_properties(workunits) == {}
    carry = source.model_extractor._carry
    assert len(_mock(carry.keep_model_versions).call_args_list) == 3
    assert len(_kept_urns(source)) == 3
    # The first throttle stops the region: Google is asked once.
    assert models[0].versioning_registry.client.list_model_versions.call_count == 1
    for model in models[1:]:
        model.versioning_registry.client.list_model_versions.assert_not_called()
    contexts = [c for w in source.report.warnings for c in w.context]
    assert len([c for c in contexts if "resource=model_lookup" in c]) == 1


def test_model_versions_refused_for_one_model_writes_the_others() -> None:
    source = _model_source()
    other_region = _model_source()
    models = [gen_mock_model(1), gen_mock_model(2)]
    models[0].versioning_registry.client.list_model_versions.side_effect = _iam_denied()

    workunits = _read_models(source, models)
    # Another region of the same run is still asked.
    other_region._stopped = source._stopped
    other_region.model_extractor._stopped = source._stopped
    other_region._current_region = "europe-west4"
    other_workunits = _read_models(other_region, [gen_mock_model(5)])

    assert len(_model_properties(workunits)) == 1
    assert len(_model_properties(other_workunits)) == 1
    _mock(source.model_extractor._carry.keep_model_versions).assert_called_once()


def test_model_version_lookup_not_found_keeps_the_model_and_warns() -> None:
    source = _model_source()
    model = gen_mock_model(1)
    model.versioning_registry.client.list_model_versions.side_effect = NotFound("x")

    workunits = _read_models(source, [model])

    assert not _model_properties(workunits)
    _mock(source.model_extractor._carry.keep_model_versions).assert_called_once()
    (context,) = _warning_contexts(source)
    assert "resource=model_lookup" in context


def test_model_kept_version_keeps_its_group_and_blocks_lineage_only_write() -> None:
    source = _model_source()
    model = gen_mock_model(1)
    model.versioning_registry.client.list_model_versions.side_effect = _throttled()
    group_urn = source.urn_builder.make_ml_model_group_urn(model)
    model_urn = _model_urn("proj-one.model.mock_prediction_model_1_1")
    source.model_usage_tracker.track_model_usage(
        model_urn, _job_urn(_flow_urn("f"), "t")
    )

    _read_models(source, [model])
    pending = list(source.model_extractor.emit_pending_lineage_updates())

    assert group_urn in _kept_urns(source)
    assert pending == []
    assert source.model_usage_tracker.get_model_usage(model_urn)


_SERVED = _deployment_urn("test-endpoint")
_UNSERVED = _deployment_urn("other-endpoint")


@pytest.mark.parametrize("error", [_iam_denied(), InvalidArgument("bad request")])
def test_model_kept_model_keeps_only_the_deployments_that_serve_it(
    error: Exception,
) -> None:
    source = _model_source()
    model = gen_mock_model(1)
    model.versioning_registry.client.list_model_versions.side_effect = error
    kept = _real_carry(source, [_SERVED, _UNSERVED])

    _read_models(source, [model], endpoints=[_endpoint_serving(model)])
    source.model_extractor._carry.flush()

    assert kept.urns == [_SERVED]


def test_model_kept_model_keeps_every_deployment_when_endpoints_unread() -> None:
    source = _model_source()
    model = gen_mock_model(1)
    model.versioning_registry.client.list_model_versions.side_effect = _iam_denied()
    kept = _real_carry(source, [_SERVED, _UNSERVED])

    _read_models(source, [model], endpoints=_iam_denied())
    source.model_extractor._carry.flush()

    assert sorted(kept.urns) == sorted([_SERVED, _UNSERVED])


def test_model_evaluation_of_kept_model_writes_no_model_version() -> None:
    source = _model_source(include_evaluations=True)
    model = gen_mock_model(1)
    model.versioning_registry.client.list_model_versions.side_effect = _throttled()
    evaluations = [gen_mock_model_evaluation(1)]

    workunits = _read_models(source, [model], evaluations=evaluations)

    urns = _written(workunits)
    assert any(urn.startswith("urn:li:mlModelDeployment:") for urn in urns)
    assert not any(urn.startswith("urn:li:mlModel:") for urn in urns)


def test_model_evaluation_list_failure_keeps_evaluation_kind() -> None:
    source = _model_source(include_evaluations=True)

    workunits = _read_models(source, [gen_mock_model(1)], evaluations=_throttled())

    assert len(_model_properties(workunits)) == 1
    assert not any(
        urn.startswith("urn:li:mlModelDeployment:") for urn in _written(workunits)
    )
    _mock(source.model_extractor._carry.keep_kind).assert_called_with(
        "proj-one", [ResourceKinds.MODEL_EVALUATION]
    )
    _mock(source.model_extractor._carry.keep_model_versions).assert_not_called()


def test_model_evaluation_list_not_found_keeps_evaluation_kind() -> None:
    source = _model_source(include_evaluations=True)

    _read_models(source, [gen_mock_model(1)], evaluations=NotFound("gone"))

    _mock(source.model_extractor._carry.keep_kind).assert_called_with(
        "proj-one", [ResourceKinds.MODEL_EVALUATION]
    )
    (context,) = _warning_contexts(source)
    assert "resource=evaluation_lookup" in context


def test_model_list_failure_keeps_that_regions_model_step() -> None:
    source = _region_source()

    run = _run_regions(
        source, {}, model_errors={REGIONS[0]: _throttled()}, previous=_EVERY_URN
    )

    assert run.model_step_regions == REGIONS
    assert sorted(run.kept.urns) == sorted(
        [*_MODEL_STEP_URNS, _CONTAINER_URN, _PROJECT_CONTAINER_URN]
    )
    skipped = [w for w in source.report.warnings if w.title == "Model step skipped"]
    contexts = [c for w in skipped for c in w.context]
    assert len(contexts) == 1
    assert contexts[0].startswith(f"project=proj-one, region={REGIONS[0]}")


def test_model_list_failure_does_not_escape_to_project_boundary() -> None:
    source = _region_source()

    run = _run_regions(
        source, {}, model_errors={region: _iam_denied() for region in REGIONS}
    )

    assert "Project could not be read" not in {f.title for f in source.report.failures}
    assert {_PIPELINE_URN, _EXPERIMENT_URN} <= {wu.get_urn() for wu in run.workunits}


def test_model_held_back_step_warning_names_the_phase1_error() -> None:
    source = _region_source()

    quota = RetryError("retries exhausted", cause=ResourceExhausted("quota"))
    _run_regions(source, {(REGIONS[0], "training_jobs"): quota})

    skipped = [w for w in source.report.warnings if w.title == "Model step skipped"]
    contexts = [c for w in skipped for c in w.context]
    assert len(contexts) == 1
    assert f"region={REGIONS[0]}" in contexts[0]
    assert "switch=rate_limit" in contexts[0]


def test_model_endpoint_not_found_on_one_endpoint_loses_only_that_endpoint() -> None:
    source = _model_source()
    models = [gen_mock_model(1), gen_mock_model(2)]
    serving = _endpoint_serving(models[0])
    gone = gen_mock_endpoint()
    _mock(gone).name = "gone-endpoint"
    _mock(gone.list_models).side_effect = NotFound("endpoint deleted")

    workunits = _read_models(source, models, endpoints=[serving, gone])

    properties = _model_properties(workunits)
    assert len(properties) == 2
    assert sorted(len(p.deployments) for p in properties.values()) == [0, 1]
    carry = source.model_extractor._carry
    _mock(carry.keep_kind).assert_not_called()
    _mock(carry.keep_urn).assert_any_call(_deployment_urn("gone-endpoint"))
    (context,) = _warning_contexts(source)
    assert "resource=endpoint_list, item=gone-endpoint" in context


def test_model_list_not_found_is_a_failed_list() -> None:
    source = _model_source()

    with pytest.raises(NotFound):
        _read_models(source, NotFound("gone"))


def test_model_kept_versions_match_only_that_models_numeric_versions() -> None:
    source = _model_source()
    model_name = source.name_formatter.format_model_name(entity_id="my_model")
    carry, current, _ = _carry_forward(
        [
            _model_urn("proj-one.model.my_model_1"),
            _model_urn("proj-one.model.my_model_12"),
            _model_urn("proj-one.model.my_model_v2_1"),
        ]
    )
    carry.keep_model_versions(model_name)
    carry.flush()
    assert sorted(current.urns) == sorted(
        [
            _model_urn("proj-one.model.my_model_1"),
            _model_urn("proj-one.model.my_model_12"),
        ]
    )


def test_many_kept_models_are_one_keep_and_keep_the_same_urns() -> None:
    source = _model_source()
    names = [f"model_{i}" for i in range(200)]
    previous = [_model_urn(f"proj-one.model.{n}_{v}") for n in names for v in (1, 12)]
    previous.append(_model_urn("proj-one.model.model_5_v2_1"))
    previous.append(_model_urn("proj-one.model.other_1"))
    carry, current, _ = _carry_forward(previous)
    for name in names[::2]:
        carry.keep_model_versions(
            source.name_formatter.format_model_name(entity_id=name)
        )
    carry.flush()
    assert sorted(current.urns) == sorted(
        _model_urn(f"proj-one.model.{n}_{v}") for n in names[::2] for v in (1, 12)
    )


_M1_CONTAINER = "urn:li:container:0123456789abcdef0123456789abcdef"
_M1_FLOW = _flow_urn("pipe-a")
_M1_JOB = _job_urn(_M1_FLOW, "task-1")
_M1_PLAIN_KEPT = [_M1_CONTAINER, _M1_FLOW, _M1_JOB]
_M1_OWN_CONTAINER = get_project_container("proj-one", PLATFORM, None, ENV).as_urn()
_M1_PREVIOUS = [*_M1_PLAIN_KEPT, _M1_OWN_CONTAINER]


def _m1_source(
    previous: List[str],
    platform_instance: Optional[str] = None,
    **config: object,
) -> Tuple[VertexAISource, GenericCheckpointState]:
    source = _make_source(
        project_ids=["proj-one"], platform_instance=platform_instance, **config
    )
    carry, current, _ = _carry_forward(previous, platform_instance=platform_instance)
    source._carry = carry
    return source, current


def _m1_instance_dataset(project: str) -> str:
    return _dataset_urn(f"prod-us.{project}.dataset.1")


def test_m1_discovered_no_role_keeps_containers_flows_and_jobs() -> None:
    source, current = _m1_source([*_M1_PREVIOUS, _model_urn("proj-one.model.m1_1")])
    source.config.project_ids = []

    with (
        patch("datahub.ingestion.source.vertexai.vertexai.aiplatform.init"),
        patch.object(source, "_gen_workunits_for_project", side_effect=_no_role()),
    ):
        list(source.get_workunits_internal())

    assert set(current.urns) == {*_M1_PREVIOUS, _model_urn("proj-one.model.m1_1")}


def test_m1_region_discovery_refused_keeps_containers_flows_and_jobs() -> None:
    source, current = _m1_source(_M1_PREVIOUS, discover_regions=True)
    source.config.project_ids = []

    with _locations_client(_no_role()):
        source._discover_regions_for_project("proj-one")
    source._carry.flush()

    assert set(current.urns) == set(_M1_PREVIOUS)


@pytest.mark.filterwarnings("ignore:.*region.*deprecated")
def test_m1_region_discovery_fallback_keeps_containers_flows_and_jobs() -> None:
    source, current = _m1_source(_M1_PREVIOUS, discover_regions=True, region=REGION)

    with _locations_client(ServiceUnavailable("busy")):
        source._resolve_project_regions()
    source._carry.flush()

    assert set(current.urns) == set(_M1_PREVIOUS)


def test_m1_carry_forward_keeps_datasets_with_platform_instance() -> None:
    mine = _m1_instance_dataset("proj-one")
    other = _m1_instance_dataset("proj-two")
    carry, current, _ = _carry_forward([mine, other], platform_instance="prod-us")

    carry.keep_kind("proj-one", [ResourceKinds.DATASET])
    carry.flush()

    assert set(current.urns) == {mine}


def test_m1_lost_training_kind_keeps_datasets_with_platform_instance() -> None:
    mine = _m1_instance_dataset("proj-one")
    source, current = _m1_source([mine], platform_instance="prod-us")

    source._keep_phase1_kind("proj-one", "training_jobs")
    source._carry.flush()

    assert set(current.urns) == {mine}


def test_m1_skipped_project_keeps_datasets_with_platform_instance() -> None:
    mine = _m1_instance_dataset("proj-one")
    source, current = _m1_source([mine], platform_instance="prod-us")
    source.config.project_ids = []

    _run_projects(source, {"proj-one": _no_role()})

    assert set(current.urns) == {mine}


@pytest.mark.parametrize(
    "exc,kind",
    [
        (Unauthenticated("x"), ErrorKind.REFUSED),
        (Unauthorized("x"), ErrorKind.REFUSED),
        (MethodNotImplemented("x"), ErrorKind.ITEM_LASTING),
        (Unknown("x"), ErrorKind.TRANSIENT),
        (Aborted("x"), ErrorKind.TRANSIENT),
        (Cancelled("x"), ErrorKind.TRANSIENT),
        (FailedPrecondition("x"), ErrorKind.ITEM_LASTING),
        (InvalidArgument("x"), ErrorKind.ITEM_LASTING),
        (GoogleAPIError("x"), ErrorKind.ITEM_LASTING),
    ],
)
def test_m1_classify_error_unlisted_google_errors(
    exc: BaseException, kind: ErrorKind
) -> None:
    assert classify_error(exc) is kind


def _m1_helper(client: MagicMock, **config: object) -> MLMetadataHelper:
    return MLMetadataHelper(
        metadata_client=client,
        config=MLMetadataConfig(project_id="proj-one", region=REGION, **config),
        uri_parser=MagicMock(),
        report=VertexAISourceReport(),
        stopped_scopes=StoppedScopes(),
        scope=lambda: Scope(project="proj-one", region=REGION),
    )


def _m1_execution() -> MagicMock:
    execution = MagicMock()
    execution.name = "execution-1"
    execution.metadata = {"learning_rate": MagicMock(string_value="0.001")}
    return execution


def test_m1_ml_metadata_filter_escapes_the_display_name() -> None:
    client = MagicMock()
    client.list_executions.return_value = [_m1_execution()]
    helper = _m1_helper(client, enable_lineage_extraction=False)
    job = MagicMock(display_name='my "job" back\\slash')

    helper.get_job_lineage_metadata(job)

    request = client.list_executions.call_args.kwargs["request"]
    assert request.filter == 'display_name="my \\"job\\" back\\\\slash"'


@pytest.mark.parametrize(
    "error",
    [_iam_denied(), NotFound("deleted")],
    ids=["refused", "deleted_execution"],
)
def test_m1_ml_metadata_lost_artifacts_cost_only_the_links(error: Exception) -> None:
    client = MagicMock()
    execution = MagicMock()
    execution.name = "execution-1"
    execution.metadata = {
        "learning_rate": Value(string_value="0.001"),
        "accuracy": Value(number_value=0.9),
    }
    client.list_executions.return_value = [execution]
    client.query_execution_inputs_and_outputs.side_effect = error
    helper = _m1_helper(client)

    result = helper.get_job_lineage_metadata(MagicMock(display_name="job"))

    # The execution list already holds them, so the links query cannot cost them.
    assert not result.keep_previous
    assert result.value is not None
    assert [h.name for h in result.value.hyperparams] == ["learning_rate"]
    assert [m.name for m in result.value.metrics] == ["accuracy"]
    assert not result.value.input_urns and not result.value.output_urns
    (context,) = [c for w in helper.report.warnings for c in w.context]
    assert "resource=ml_metadata_artifacts" in context


def test_m1_ml_metadata_throttled_artifacts_keep_the_job() -> None:
    client = MagicMock()
    client.list_executions.return_value = [_m1_execution()]
    client.query_execution_inputs_and_outputs.side_effect = _throttled()
    helper = _m1_helper(client)

    result = helper.get_job_lineage_metadata(MagicMock(display_name="job"))

    assert result.keep_previous
    assert result.value is None


def test_m1_ml_metadata_execution_cache_cap_is_a_warning() -> None:
    client = MagicMock()
    client.list_executions.return_value = [_m1_execution()]
    helper = _m1_helper(client, max_execution_search_limit=1)

    helper._load_execution_cache()

    assert [w.title for w in helper.report.warnings] == [
        "Execution cache limit reached"
    ]
    assert "project=proj-one" in helper.report.warnings[0].context[0]


def test_m1_ml_metadata_client_init_failure_is_a_warning() -> None:
    source = _make_source(
        project_ids=["proj-one"],
        include_training_jobs=False,
        include_pipelines=False,
        include_experiments=False,
        use_ml_metadata_for_lineage=True,
    )
    source._project_to_regions = {"proj-one": [REGION]}
    source._current_project_id = "proj-one"

    with (
        patch("datahub.ingestion.source.vertexai.vertexai.aiplatform.init"),
        patch(
            "datahub.ingestion.source.vertexai.vertexai.MetadataServiceClient",
            side_effect=InvalidArgument("bad endpoint"),
        ),
    ):
        list(source._gen_project_resource_workunits("proj-one"))

    assert source._ml_metadata_helper is None
    assert [w.title for w in source.report.warnings] == [
        "ML Metadata client could not be created"
    ]
    assert f"region={REGION}" in source.report.warnings[0].context[0]


def test_m1_automl_config_parse_failure_is_a_warning_for_the_item() -> None:
    source = _training_source()
    job = MagicMock()
    job.name = "job-1"
    job.to_dict.return_value = {"inputDataConfig": "not-an-object"}

    assert source.training_extractor._parse_automl_config(job) is None

    assert [w.title for w in source.report.warnings] == [
        "AutoML job config could not be parsed"
    ]
    assert "item=job-1" in source.report.warnings[0].context[0]


def test_m1_region_discovery_with_no_usable_region_is_reported() -> None:
    source = _make_source(project_ids=["proj-one"], discover_regions=True)
    source._carry = MagicMock()

    with _locations_client(locations_pb2.ListLocationsResponse()):
        regions = source._discover_regions_for_project("proj-one")

    assert regions == []
    assert any("project=proj-one" in c for c in _failure_contexts(source))
    source._carry.keep_project.assert_called_with("proj-one")


_EXPERIMENT_MODULE = "datahub.ingestion.source.vertexai.vertexai_experiment_extractor"
_RUN_URN_PREFIX = "urn:li:dataProcessInstance:"
_EXPERIMENT_NAME = gen_mock_experiment().name


def _experiment_source(**config: object) -> VertexAISource:
    config.setdefault("include_models", False)
    source = _make_source(
        project_ids=["proj-one"],
        include_training_jobs=False,
        include_pipelines=False,
        include_experiments=True,
        **config,
    )
    source._current_project_id = "proj-one"
    source._current_region = REGION
    source._carry = MagicMock()
    return source


def _read_experiment_runs(source: VertexAISource, runs: List[Any]) -> List[Any]:
    with (
        patch.object(
            source.experiment_extractor,
            "_list_experiments_rate_limited",
            return_value=[gen_mock_experiment()],
        ),
        patch.object(
            source.experiment_extractor,
            "_list_experiment_runs_rate_limited",
            return_value=runs,
        ),
    ):
        return list(source.experiment_extractor.get_experiment_run_workunits())


def _warning_contexts(source: VertexAISource) -> List[str]:
    return [c for w in source.report.warnings for c in w.context]


def test_experiment_missing_store_is_info_and_not_kept() -> None:
    source = _experiment_source()

    with (
        patch(
            f"{_EXPERIMENT_MODULE}.rate_limited_gapic_list",
            side_effect=NotFound("Requested Metadata Store default not found"),
        ),
        patch.object(
            source.experiment_extractor, "_metadata_store_parent", return_value="p"
        ),
    ):
        workunits = list(source.experiment_extractor.get_experiment_workunits())

    assert workunits == []
    assert not source.report.warnings
    infos = [i for i in source.report.infos if "Metadata Store" in (i.title or "")]
    assert f"region={REGION}" in infos[0].context[0]


def test_experiment_run_list_not_found_loses_only_that_experiment() -> None:
    source = _experiment_source(include_models=True)
    experiment = gen_mock_experiment()

    with (
        patch.object(
            source.experiment_extractor,
            "_list_experiments_rate_limited",
            return_value=[experiment],
        ),
        patch(
            f"{_EXPERIMENT_MODULE}.rate_limited_gapic_list",
            side_effect=NotFound("experiment deleted"),
        ),
        patch.object(
            source.experiment_extractor, "_metadata_store_parent", return_value="p"
        ),
    ):
        run = _run_source(source)

    assert run.model_step_regions == [REGION]
    assert not any(wu.get_urn().startswith(_RUN_URN_PREFIX) for wu in run.workunits)
    assert not source.report.failures
    (context,) = _warning_contexts(source)
    assert f"resource=experiments, item={experiment.name}" in context


def test_experiment_run_without_logged_jobs_key_has_none_and_no_warning() -> None:
    source = _experiment_source()
    run = gen_mock_experiment_run()
    run._metadata_node = MagicMock(metadata={"_params": {}, "_metrics": {}})
    _mock(run.get_logged_custom_jobs).side_effect = TypeError("not iterable")

    workunits = _read_experiment_runs(source, [run])

    assert any(wu.get_urn().startswith(_RUN_URN_PREFIX) for wu in workunits)
    assert not [
        a
        for a in _aspects(workunits, DataProcessInstanceRelationshipsClass)
        if a.upstreamInstances
    ]
    _mock(run.get_logged_custom_jobs).assert_not_called()
    assert not source.report.warnings


def test_experiment_sdk_type_error_writes_run_without_logged_jobs() -> None:
    source = _experiment_source()
    run = gen_mock_experiment_run()
    _mock(run.get_logged_custom_jobs).side_effect = TypeError("not a custom job run")

    workunits = _read_experiment_runs(source, [run])

    assert any(wu.get_urn().startswith(_RUN_URN_PREFIX) for wu in workunits)
    assert not [
        a
        for a in _aspects(workunits, DataProcessInstanceRelationshipsClass)
        if a.upstreamInstances
    ]
    assert not source._stopped._stopped
    contexts = _warning_contexts(source)
    assert len(contexts) == 1
    assert f"item={_EXPERIMENT_NAME}/{run.name}" in contexts[0]


def test_experiment_legacy_run_not_implemented_writes_run_without_executions() -> None:
    source = _experiment_source()
    run = gen_mock_experiment_run()
    _mock(run.get_executions).side_effect = NotImplementedError("legacy run")

    workunits = _read_experiment_runs(source, [run])

    urns = {wu.get_urn() for wu in workunits}
    assert len(urns) == 1
    assert next(iter(urns)).startswith(_RUN_URN_PREFIX)
    assert not source._stopped._stopped
    contexts = _warning_contexts(source)
    assert len(contexts) == 1
    assert f"item={_EXPERIMENT_NAME}/{run.name}" in contexts[0]


def test_experiment_run_detail_transient_keeps_run() -> None:
    source = _experiment_source()
    first, second = gen_mock_experiment_run(), gen_mock_experiment_run()
    _mock(first.get_executions).side_effect = _throttled()
    _mock(second).name = "mock_experiment_run_2"

    workunits = _read_experiment_runs(source, [first, second])

    assert workunits == []
    _mock(first.get_logged_custom_jobs).assert_not_called()
    _mock(second.get_executions).assert_not_called()
    assert len(_warning_contexts(source)) == 1


def test_experiment_list_refused_keeps_all_containers() -> None:
    source = _region_source(include_models=False)

    run = _run_regions(source, {(REGIONS[0], "experiments"): _iam_denied()})

    _mock(run.carry.keep_all_containers).assert_called()
    assert any("resource=experiments" in c for c in _failure_contexts(source))


@pytest.mark.parametrize("error", [_iam_denied(), _throttled()])
@pytest.mark.parametrize("lost_call", ["experiment_list", "run_list"])
def test_experiment_list_failure_through_the_extractor_keeps_every_container(
    error: Exception, lost_call: str
) -> None:
    source = _make_source(
        project_ids=["proj-one"],
        include_training_jobs=False,
        include_experiments=True,
        include_pipelines=True,
    )
    kept = _real_carry(source, _EVERY_URN)
    extractor = source.experiment_extractor
    patches = [
        patch(f"{_EXPERIMENT_MODULE}.rate_limited_gapic_list", side_effect=error),
        patch.object(extractor, "_metadata_store_parent", return_value="p"),
        patch(f"{_PIPELINE_MODULE}.rate_limited_gapic_list", return_value=[]),
    ]
    if lost_call == "run_list":
        patches.append(
            patch.object(
                extractor,
                "_list_experiments_rate_limited",
                return_value=[gen_mock_experiment()],
            )
        )

    _run_source(source, *patches)

    # Only the containers are kept: pipelines were read, so their flows go.
    assert kept.urns == [_CONTAINER_URN, _PROJECT_CONTAINER_URN]
    lasting = classify_error(error) is not ErrorKind.TRANSIENT
    contexts = _failure_contexts(source) if lasting else _warning_contexts(source)
    assert len([c for c in contexts if "resource=experiments" in c]) == 1


def _run_experiments_with_models(
    source: VertexAISource, run: Any, run_list_error: Optional[Exception] = None
) -> List[MetadataWorkUnit]:
    run_list: Dict[str, Any] = (
        {"side_effect": run_list_error} if run_list_error else {"return_value": [run]}
    )
    return _run_source(
        source,
        patch.object(
            source.experiment_extractor,
            "_list_experiments_rate_limited",
            return_value=[gen_mock_experiment()],
        ),
        patch.object(
            source.experiment_extractor,
            "_list_experiment_runs_rate_limited",
            **run_list,
        ),
    ).workunits


def test_experiment_throttled_run_detail_with_output_is_warning_not_failure() -> None:
    source = _experiment_source(include_models=True)
    kept = _real_carry(source, _EVERY_URN)
    run = gen_mock_experiment_run()
    _mock(run.get_executions).side_effect = _throttled()

    workunits = _run_experiments_with_models(source, run)

    assert workunits
    assert not source.report.failures
    titles = [w.title for w in source.report.warnings]
    assert "Detail read stopped after a temporary failure" in titles
    # Experiment links never reach the written models, so the models are written.
    assert "Model step skipped" not in titles
    assert kept.urns == []


@pytest.mark.parametrize("error", [_iam_denied(), _throttled()])
def test_experiment_run_list_failure_after_output_is_not_a_project_failure(
    error: Exception,
) -> None:
    source = _experiment_source(include_models=True)
    kept = _real_carry(source, _EVERY_URN)

    workunits = _run_experiments_with_models(
        source, gen_mock_experiment_run(), run_list_error=error
    )

    assert workunits
    assert "Project could not be read" not in {f.title for f in source.report.failures}
    # Experiment links never reach written models, so the models are written
    # and only the containers are kept, whatever the error.
    assert "Model step skipped" not in {w.title for w in source.report.warnings}
    assert sorted(kept.urns) == sorted([_CONTAINER_URN, _PROJECT_CONTAINER_URN])
    if classify_error(error) is ErrorKind.TRANSIENT:
        assert any("resource=experiments" in c for c in _warning_contexts(source))
    else:
        assert any("resource=experiments" in c for c in _failure_contexts(source))


def test_experiment_run_with_logged_custom_job_links_it_upstream() -> None:
    source = _experiment_source()
    run = gen_mock_experiment_run()
    run._metadata_node = MagicMock(
        metadata={"_custom_jobs": [{"custom_job_resource_name": "jobs/1"}]}
    )

    workunits = _read_experiment_runs(source, [run])

    _mock(run.get_logged_custom_jobs).assert_called_once()
    linked = [
        a
        for a in _aspects(workunits, DataProcessInstanceRelationshipsClass)
        if a.upstreamInstances
    ]
    assert len(linked) == 1
    assert "mock_training_job" in linked[0].upstreamInstances[0]
    assert not source.report.warnings


def test_experiment_artifact_failure_context_names_the_execution() -> None:
    source = _experiment_source()
    run = gen_mock_experiment_run()
    execution = MagicMock()
    execution.name = "exec-1"
    execution.get_input_artifacts.side_effect = TypeError("bad artifact")
    _mock(run.get_executions).return_value = [execution]

    _read_experiment_runs(source, [run])

    contexts = _warning_contexts(source)
    assert len(contexts) == 1
    assert f"item={_EXPERIMENT_NAME}/{run.name}/exec-1" in contexts[0]


def test_experiment_run_detail_transient_does_not_hold_back_models() -> None:
    source = _region_source(
        include_experiments=True,
        include_training_jobs=False,
        include_pipelines=False,
    )
    kept = _real_carry(source, _EVERY_URN)
    run = gen_mock_experiment_run()
    _mock(run.get_executions).side_effect = _throttled()

    result = _run_source(
        source,
        patch.object(
            source.experiment_extractor,
            "_list_experiments_rate_limited",
            return_value=[gen_mock_experiment()],
        ),
        patch.object(
            source.experiment_extractor,
            "_list_experiment_runs_rate_limited",
            return_value=[run],
        ),
    )

    assert result.model_step_regions == REGIONS
    # The run is a process instance and the experiment is written, so
    # nothing needs keeping.
    assert kept.urns == []
    assert not source.report.failures


_PIPELINE_MODULE = "datahub.ingestion.source.vertexai.vertexai_pipeline_extractor"
_MOCK_FLOW_URN = f"urn:li:dataFlow:({PLATFORM},Mock Pipeline,{ENV})"
_MOCK_JOB_URN = f"urn:li:dataJob:({_MOCK_FLOW_URN},reverse)"


def _pipeline_source(**config: object) -> VertexAISource:
    source = _make_source(
        project_ids=["proj-one"],
        include_training_jobs=False,
        include_experiments=False,
        include_pipelines=True,
        **config,
    )
    source._current_project_id = "proj-one"
    source._current_region = REGION
    source._carry = MagicMock()
    source.pipeline_extractor._carry = source._carry
    return source


def _read_pipelines(source: VertexAISource, jobs: Any) -> List[MetadataWorkUnit]:
    kwargs = (
        {"side_effect": jobs} if isinstance(jobs, Exception) else {"return_value": jobs}
    )
    with patch(f"{_PIPELINE_MODULE}.rate_limited_gapic_list", **kwargs):
        return list(source.pipeline_extractor.get_workunits())


def _fail_task_details(job: Any, error: Exception) -> None:
    type(job).task_details = PropertyMock(side_effect=error)


def test_pipeline_task_details_transient_keeps_flow_and_tasks() -> None:
    source = _pipeline_source()
    first, second = get_mock_pipeline_job(), get_mock_pipeline_job()
    _mock(second).name = "mock_pipeline_job_2"
    _fail_task_details(first, _throttled())
    _fail_task_details(second, _throttled())

    workunits = _read_pipelines(source, [first, second])

    assert workunits == []
    _mock(source._carry.keep_flow).assert_called_with(_MOCK_FLOW_URN)
    assert _mock(source._carry.keep_flow).call_count == 2
    assert len(_warning_contexts(source)) == 1
    assert not source.report.failures


def test_pipeline_list_refused_keeps_all_flows_and_containers() -> None:
    source = _region_source(
        include_models=False, include_training_jobs=False, include_experiments=False
    )

    run = _run_regions(source, {(REGIONS[0], "pipelines"): _iam_denied()})

    _mock(run.carry.keep_all_flows).assert_called()
    _mock(run.carry.keep_all_containers).assert_called()
    assert any("resource=pipelines" in c for c in _failure_contexts(source))


def test_pipeline_flow_of_failed_project_stays_in_checkpoint() -> None:
    source = _make_source(
        project_ids=["proj-one", "proj-two"],
        include_training_jobs=False,
        include_experiments=False,
        include_pipelines=True,
    )
    carry, current, _ = _carry_forward(
        [_MOCK_FLOW_URN, _MOCK_JOB_URN, _PROJECT_CONTAINER_URN]
    )
    source._carry = carry
    source.pipeline_extractor._carry = carry

    def gapic_list(*_args: object, **_kw: object) -> List[Any]:
        if source._current_project_id == "proj-one":
            raise _iam_denied()
        return [get_mock_pipeline_job()]

    with (
        patch(
            "datahub.ingestion.source.vertexai.vertexai.get_vertexai_disable_parallelism",
            return_value=True,
        ),
        patch("datahub.ingestion.source.vertexai.vertexai.aiplatform.init"),
        patch.object(source, "_gen_project_workunits", return_value=iter([])),
        patch(f"{_PIPELINE_MODULE}.rate_limited_gapic_list", side_effect=gapic_list),
    ):
        workunits = list(source.get_workunits_internal())

    assert _MOCK_FLOW_URN in {wu.get_urn() for wu in workunits}
    assert set(current.urns) >= {_MOCK_FLOW_URN, _MOCK_JOB_URN}


def test_pipeline_lookup_stop_keeps_that_pipelines_container() -> None:
    source = _region_source(
        include_training_jobs=False,
        include_experiments=False,
        include_models=False,
        include_evaluations=False,
    )
    container = PipelineContainerKey(
        project_id="proj-one",
        platform=PLATFORM,
        instance=None,
        env=ENV,
        pipeline_name="Mock Pipeline",
    ).as_urn()
    kept = _real_carry(source, [*_EVERY_URN, container])

    def gapic_list(*_args: object, **_kw: object) -> List[Any]:
        job = get_mock_pipeline_job()
        _fail_task_details(job, _throttled())
        return [job]

    _run_source(
        source,
        patch(f"{_PIPELINE_MODULE}.rate_limited_gapic_list", side_effect=gapic_list),
    )

    # No model step runs to keep containers, so the kept pipeline keeps its own.
    assert container in kept.urns
    assert _CONTAINER_URN not in kept.urns


def test_pipeline_task_details_transient_holds_back_that_regions_models() -> None:
    source = _region_source(include_training_jobs=False, include_experiments=False)
    kept = _real_carry(source, _EVERY_URN)

    def gapic_list(*_args: object, **_kw: object) -> List[Any]:
        job = get_mock_pipeline_job()
        if source._current_region == REGIONS[0]:
            _fail_task_details(job, _throttled())
        return [job]

    run = _run_source(
        source,
        patch(f"{_PIPELINE_MODULE}.rate_limited_gapic_list", side_effect=gapic_list),
    )

    assert run.model_step_regions == [REGIONS[1]]
    assert sorted(kept.urns) == sorted(
        [*_MODEL_STEP_URNS, _CONTAINER_URN, _PROJECT_CONTAINER_URN]
    )
    assert not source.report.failures


def test_pipeline_list_not_found_is_a_failed_list() -> None:
    source = _pipeline_source()

    with pytest.raises(NotFound):
        _read_pipelines(source, NotFound("no pipelines"))


def test_pipeline_task_artifact_error_warns_once_and_writes_task() -> None:
    source = _pipeline_source()
    job = get_mock_pipeline_job()
    task_detail = job.task_details[0]
    task_detail.inputs = MagicMock()
    task_detail.inputs.values.side_effect = TypeError("bad artifact")

    workunits = _read_pipelines(source, [job])

    assert any(wu.get_urn() == _MOCK_JOB_URN for wu in workunits)
    assert _aspects(workunits, DataProcessInstanceRelationshipsClass)
    contexts = _warning_contexts(source)
    assert len(contexts) == 1
    assert f"item=Mock Pipeline/{job.name}/reverse" in contexts[0]
    assert not _mock(source._carry.keep_flow).called


def _flow_properties(workunits: List[MetadataWorkUnit]) -> Dict[str, str]:
    (info,) = _aspects(workunits, DataFlowInfoClass)
    return info.customProperties


def _read_failed_lookup(
    error: Exception, jobs: int = 1
) -> Tuple[VertexAISource, List[MetadataWorkUnit]]:
    source = _pipeline_source()
    listed = [get_mock_pipeline_job() for _ in range(jobs)]
    for number, job in enumerate(listed):
        _mock(job).name = f"mock_pipeline_job_{number}"
        _fail_task_details(job, error)
    return source, _read_pipelines(source, listed)


def _assert_flow_written_from_list(workunits: List[MetadataWorkUnit]) -> None:
    properties = _flow_properties(workunits)
    assert "update_time" in properties
    assert "duration" in properties
    assert any(wu.get_urn() == _MOCK_JOB_URN for wu in workunits)


def test_pipeline_lookup_refused_writes_flow_from_list_and_warns_per_job() -> None:
    source, workunits = _read_failed_lookup(_iam_denied(), jobs=2)

    _assert_flow_written_from_list(workunits)
    assert len(_warning_contexts(source)) == 2
    assert (
        source._stopped.stop_kind(
            Scope(project="proj-one", region=REGION, kind="pipeline_lookup")
        )
        is None
    )
    _mock(source._carry.keep_flow).assert_not_called()


def test_pipeline_lookup_lasting_error_warns_per_job_without_stopping() -> None:
    source, workunits = _read_failed_lookup(InvalidArgument("bad job"), jobs=2)

    _assert_flow_written_from_list(workunits)
    assert len(_warning_contexts(source)) == 2
    assert not source._stopped._stopped
    _mock(source._carry.keep_flow).assert_not_called()


def test_pipeline_lookup_not_found_writes_flow_from_list_and_warns() -> None:
    source, workunits = _read_failed_lookup(NotFound("job gone"))

    _assert_flow_written_from_list(workunits)
    (context,) = _warning_contexts(source)
    assert "resource=pipeline_lookup" in context
    _mock(source._carry.keep_flow).assert_not_called()


def test_pipeline_checkpoint_early_stop_reads_listed_update_time() -> None:
    source = _pipeline_source()
    newer, older = get_mock_pipeline_job(), get_mock_pipeline_job()
    _mock(newer).name = "mock_pipeline_job_newer"
    _mock(older).name = "mock_pipeline_job_older"
    checkpoint = newer.gca_resource.update_time
    newer.gca_resource.update_time = checkpoint + timedelta(days=1)
    for job in (newer, older):
        type(_mock(job)).update_time = PropertyMock(side_effect=_throttled())

    with patch.object(
        source.pipeline_extractor.state_handler,
        "get_last_update_time",
        return_value=int(checkpoint.timestamp() * 1000),
    ):
        workunits = _read_pipelines(source, [newer, older])

    urns = {wu.get_urn() for wu in workunits}
    assert any(urn.endswith("mock_pipeline_job_newer") for urn in urns)
    assert not any(urn.endswith("mock_pipeline_job_older") for urn in urns)
    assert not source.report.warnings


def test_retry_budget_other_region_still_asked() -> None:
    stopped = StoppedScopes()
    asked: List[str] = []

    def read_in(region: str, error: Optional[Exception]) -> DetailResult[str]:
        def read() -> str:
            asked.append(region)
            if error:
                raise error
            return "ok"

        return DetailReader(
            VertexAISourceReport(), stopped, lambda: Scope("proj-one", region)
        )("model_lookup", read, item="item")

    assert read_in(REGIONS[0], _throttled()).keep_previous
    assert read_in(REGIONS[0], None).keep_previous
    assert read_in(REGIONS[1], None).value == "ok"
    assert asked == [REGIONS[0], REGIONS[1]]


def test_retry_budget_one_warning_per_stopped_scope() -> None:
    report = VertexAISourceReport()
    stopped = StoppedScopes()
    asked: List[str] = []

    for region in REGIONS:
        for index in range(25):

            def read(region: str = region) -> str:
                asked.append(region)
                raise _throttled()

            result = DetailReader(report, stopped, partial(Scope, "proj-one", region))(
                "model_lookup", read, item=f"item-{index}"
            )
            assert result.keep_previous

    assert asked == REGIONS
    assert [len(w.context) for w in report.warnings] == [2]
    assert not report.failures


_SCOPE_MODULE = "datahub.ingestion.source.vertexai.vertexai_scope"


def test_flush_is_one_pass_over_earlier_urns_for_many_keeps() -> None:
    previous = [
        *[_model_urn(f"p1.model.m{i}_1") for i in range(40)],
        *[_flow_urn(f"f{i}") for i in range(5)],
        *[_job_urn(_flow_urn(f"f{i}"), f"t{i}") for i in range(5)],
        get_project_container("p1", PLATFORM, None, ENV).as_urn(),
    ]
    carry, current, _ = _carry_forward(previous)
    carry.keep_all_containers("p1")
    carry.keep_all_flows("p1")
    carry.keep_kind("p1", [ResourceKinds.MODEL])
    carry.keep_project("p1")
    for i in range(60):
        carry.keep_flow(_flow_urn(f"f{i % 5}"))

    with patch(f"{_SCOPE_MODULE}.guess_entity_type", wraps=guess_entity_type) as typed:
        carry.flush()

    assert typed.call_count <= len(previous)
    assert sorted(current.urns) == sorted(previous)


def test_keep_flow_matches_the_tasks_of_a_flow_with_a_comma_in_its_id() -> None:
    flow = str(
        DataFlowUrn.create_from_ids(orchestrator=PLATFORM, flow_id="pipe,1", env=ENV)
    )
    job = str(DataJobUrn.create_from_ids(data_flow_urn=flow, job_id="task,1"))
    other_flow = str(
        DataFlowUrn.create_from_ids(orchestrator=PLATFORM, flow_id="pipe", env=ENV)
    )
    other_job = str(DataJobUrn.create_from_ids(data_flow_urn=other_flow, job_id="t"))
    carry, current, _ = _carry_forward([flow, job, other_flow, other_job])
    carry.keep_flow(flow)
    carry.flush()
    assert sorted(current.urns) == sorted([flow, job])


def test_project_wide_error_in_phase1_stops_the_projects_remaining_regions() -> None:
    source = _region_source()

    run = _run_regions(
        source,
        {(REGIONS[0], "training_jobs"): _service_disabled()},
        previous=_EVERY_URN,
    )

    assert run.model_step_regions == []
    assert sorted(run.kept.urns) == sorted(
        [
            *_TRAINING_URNS,
            *_FLOW_URNS,
            *_MODEL_STEP_URNS,
            _CONTAINER_URN,
            _PROJECT_CONTAINER_URN,
        ]
    )


def test_project_wide_error_in_phase1_makes_no_read_in_later_regions() -> None:
    source = _region_source(include_models=False)
    reads: List[Optional[str]] = []

    def training(*_args: object) -> Iterator[MetadataWorkUnit]:
        reads.append(source._current_region)
        raise _service_disabled()
        yield

    run = _run_source(
        source,
        patch.object(source.training_extractor, "get_workunits", training),
        patch.object(source.pipeline_extractor, "get_workunits", return_value=iter([])),
        patch.object(
            source.experiment_extractor,
            "get_experiment_workunits",
            return_value=iter([]),
        ),
        patch.object(
            source.experiment_extractor,
            "get_experiment_run_workunits",
            return_value=iter([]),
        ),
    )

    assert reads == [REGIONS[0]]
    assert run.workunits == []


def _ml_metadata_stop_run(jobs: List[Any]) -> Tuple[VertexAISource, MagicMock]:
    source = _training_source(extract_execution_metrics=True)
    client = MagicMock()
    client.list_executions.side_effect = _throttled()
    _attach_ml_metadata(source, client)
    with patch(
        f"{_TRAINING_MODULE}.get_job_result_status", wraps=get_job_result_status
    ) as lookup:
        _read_training(source, {"CustomJob": jobs})
    return source, lookup


def test_job_kept_after_a_stop_makes_no_further_reads() -> None:
    jobs = _custom_jobs(4)

    source, lookup = _ml_metadata_stop_run(jobs)

    assert lookup.call_count == 1


def test_automl_job_kept_after_output_model_stop_makes_no_further_reads() -> None:
    source = _training_source()
    jobs = [gen_mock_training_automl_job(), gen_mock_training_automl_job()]
    _mock(jobs[1]).name = "mock_training_job_2"

    with patch(
        f"{_TRAINING_MODULE}.get_job_result_status", wraps=get_job_result_status
    ) as lookup:
        _read_training(
            source,
            {"AutoMLTabularTrainingJob": jobs, "TabularDataset": [gen_mock_dataset()]},
            model=_throttled(),
        )

    assert lookup.call_count == 1
    _mock(source.training_extractor._carry.keep_urn).assert_any_call(
        source.urn_builder.make_dataset_urn(gen_mock_dataset().name)
    )


def test_experiment_run_kept_after_a_logged_jobs_stop_makes_no_further_reads() -> None:
    source = _experiment_source()
    first, second = gen_mock_experiment_run(), gen_mock_experiment_run()
    _mock(first.get_logged_custom_jobs).side_effect = _throttled()
    _mock(second).name = "mock_experiment_run_2"

    workunits = _read_experiment_runs(source, [first, second])

    assert workunits == []
    _mock(second.get_executions).assert_not_called()


def test_kept_model_does_not_rescan_tracked_links() -> None:
    source = _model_source()
    source.model_extractor.endpoints = {}
    tracker = source.model_usage_tracker
    kept = gen_mock_model(1)
    job = _job_urn(_flow_urn("f"), "t")
    kept_urns = [
        _model_urn("proj-one.model.mock_prediction_model_1_1"),
        _model_urn("proj-one.model.mock_prediction_model_1_12"),
        source.urn_builder.make_ml_model_group_urn(kept),
    ]
    other = _model_urn("proj-one.model.mock_prediction_model_2_1")
    other_versioned = _model_urn("proj-one.model.mock_prediction_model_1_v2_1")
    for urn in [*kept_urns, other, other_versioned]:
        tracker.track_model_usage(urn, job)

    with patch.object(
        ModelUsageTracker, "get_pending_lineage_urns", autospec=True
    ) as scan:
        source.model_extractor._keep_model(kept)

    scan.assert_not_called()
    assert sorted(tracker.get_pending_lineage_urns()) == sorted(
        [other, other_versioned]
    )


def test_stored_errors_do_not_pin_traceback_frames() -> None:
    def read() -> None:
        payload = bytearray(10)
        assert payload
        raise _throttled()

    def failed_frames(exc: BaseException) -> List[Dict[str, Any]]:
        tb = exc.__traceback__
        assert tb is not None
        frames = []
        while tb.tb_next is not None:
            tb = tb.tb_next
            frames.append(dict(tb.tb_frame.f_locals))
        return frames

    ledger = ReadLedger(report=VertexAISourceReport(), stopped=StoppedScopes())
    stopped = StoppedScopes()
    try:
        read()
    except RetryError as e:
        ledger.record_read("proj-one", REGION, "training_jobs", error=e)
        stopped.stop(Scope("proj-one", REGION, "ml_metadata"), ErrorKind.TRANSIENT, e)
        error = e

    assert failed_frames(error) == [{}]


def _skipped_project_run(previous: List[str]) -> GenericCheckpointState:
    source, current = _m1_source(previous)
    source.config.project_ids = []
    with (
        patch("datahub.ingestion.source.vertexai.vertexai.aiplatform.init"),
        patch.object(source, "_gen_workunits_for_project", side_effect=_no_role()),
    ):
        list(source.get_workunits_internal())
    return current


def test_skipped_discovered_project_never_read_keeps_no_broad_entities() -> None:
    other_project_container = get_project_container(
        "proj-two", PLATFORM, None, ENV
    ).as_urn()
    previous = [other_project_container, _M1_FLOW, _M1_JOB, _M1_CONTAINER]

    assert _skipped_project_run(previous).urns == []


def test_skipped_discovered_project_read_before_keeps_broad_entities() -> None:
    assert set(_skipped_project_run(_M1_PREVIOUS).urns) == set(_M1_PREVIOUS)


def _quota_warning_contexts(source: VertexAISource) -> List[str]:
    client = MagicMock()
    client.list_executions.side_effect = ResourceExhausted("quota")
    _attach_ml_metadata(source, client)
    _read_training(source, {"CustomJob": _custom_jobs(2)})
    return _warning_contexts(source)


def test_quota_stop_names_rate_limit_switch_when_it_is_off() -> None:
    source = _training_source(extract_execution_metrics=True, rate_limit=False)

    assert any("switch=rate_limit" in c for c in _quota_warning_contexts(source))


def test_quota_stop_does_not_name_rate_limit_switch_when_it_is_on() -> None:
    source = _training_source(extract_execution_metrics=True, rate_limit=True)

    assert not any("switch=rate_limit" in c for c in _quota_warning_contexts(source))


def test_server_error_stop_does_not_name_rate_limit_switch() -> None:
    source = _training_source(extract_execution_metrics=True, rate_limit=False)
    client = MagicMock()
    client.list_executions.side_effect = _throttled()
    _attach_ml_metadata(source, client)
    _read_training(source, {"CustomJob": _custom_jobs(1)})

    assert not any("switch=rate_limit" in c for c in _warning_contexts(source))


def test_lost_read_names_rate_limit_switch_for_a_quota_error() -> None:
    source = _region_source(include_models=False)

    _run_regions(source, {(REGIONS[0], "training_jobs"): ResourceExhausted("quota")})

    assert any(
        "resource=training_jobs" in c and "switch=rate_limit" in c
        for c in _warning_contexts(source)
    )


def test_report_counts_urns_carried_forward() -> None:
    report = VertexAISourceReport()
    carry, _, _ = _carry_forward(
        [_model_urn("p1.model.a_1"), _model_urn("p2.model.b_1")], report=report
    )
    carry.keep_kind("p1", [ResourceKinds.MODEL])
    carry.flush()

    assert report.urns_carried_forward == 1


def test_report_counts_items_kept_after_a_stop() -> None:
    jobs = _custom_jobs(4)

    source, _ = _ml_metadata_stop_run(jobs)

    assert isinstance(source.report, VertexAISourceReport)
    assert source.report.items_kept_after_stop == len(jobs) - 1


def test_read_detail_counts_the_item_kept_by_a_stopped_call() -> None:
    stopped = StoppedScopes()
    stopped.stop(
        Scope(project="proj-one", region=REGION, kind="ml_metadata"),
        ErrorKind.TRANSIENT,
        ServiceUnavailable("x"),
    )

    _, report, _ = _read_detail(MagicMock(), stopped)

    assert report.items_kept_after_stop == 1


def test_ledger_a_read_that_wrote_output_is_not_failed() -> None:
    ledger = ReadLedger(report=VertexAISourceReport(), stopped=StoppedScopes())
    ledger.record_read("proj-one", REGION, "pipelines", error=_throttled())
    assert ledger.every_read_failed("proj-one")

    # A list that broke after writing some entities was read in part.
    ledger.record_wrote_output("proj-one", REGION, "pipelines")

    assert not ledger.every_read_failed("proj-one")


_PHASE1_KINDS = ("training_jobs", "experiments", "pipelines")


@pytest.mark.parametrize("error", [_service_disabled(), _no_role()])
def test_region_loop_never_ingested_discovered_project_carries_no_broad_entities(
    error: Exception,
) -> None:
    source = _region_source()
    source.config.project_ids = []
    errors = {(region, kind): error for region in REGIONS for kind in _PHASE1_KINDS}
    other_project = get_project_container("proj-two", PLATFORM, None, ENV).as_urn()

    run = _run_regions(
        source, errors, previous=[other_project, _CONTAINER_URN, *_FLOW_URNS]
    )

    assert run.kept.urns == []


@pytest.mark.parametrize("error", [_service_disabled(), _no_role()])
def test_region_loop_ingested_before_discovered_project_carries_broad_entities(
    error: Exception,
) -> None:
    source = _region_source()
    source.config.project_ids = []
    errors = {(region, kind): error for region in REGIONS for kind in _PHASE1_KINDS}
    own_project = get_project_container("proj-one", PLATFORM, None, ENV).as_urn()
    previous = [own_project, _CONTAINER_URN, *_FLOW_URNS]

    run = _run_regions(source, errors, previous=previous)

    assert sorted(run.kept.urns) == sorted(previous)


def test_a_run_over_several_projects_makes_one_pass_over_earlier_urns() -> None:
    source = _make_source(
        project_ids=["proj-a", "proj-b", "proj-c"],
        regions=REGIONS,
        include_models=True,
    )
    errors: Dict[Tuple[str, str], Exception] = {
        (r, k): _iam_denied() for r in REGIONS for k in _PHASE1_KINDS
    }
    previous = [
        get_project_container(project, PLATFORM, None, ENV).as_urn()
        for project in ("proj-a", "proj-b", "proj-c")
    ]
    previous += [_model_urn(f"proj-b.model.m{i}_1") for i in range(30)]

    with patch(f"{_SCOPE_MODULE}.guess_entity_type", wraps=guess_entity_type) as typed:
        run = _run_regions(
            source,
            errors,
            previous=previous,
            model_errors={region: _iam_denied() for region in REGIONS},
        )

    assert typed.call_count <= len(previous)
    assert len(run.kept.urns) == len(previous)


def test_kept_models_after_an_endpoint_stop_list_no_versions() -> None:
    source = _model_source()
    source._stopped.stop(
        Scope("proj-one", REGION, DetailCalls.ENDPOINT_LIST),
        ErrorKind.TRANSIENT,
        ServiceUnavailable("busy"),
    )
    models = [gen_mock_model(i) for i in range(200)]
    list_versions = MagicMock(return_value=[])
    for model in models:
        registry: Any = model.versioning_registry
        registry.client = MagicMock(list_model_versions=list_versions)

    _read_models(source, models)

    list_versions.assert_not_called()
    assert source.report.items_kept_after_stop == 200


def test_carried_forward_counter_counts_distinct_urns() -> None:
    report = VertexAISourceReport()
    carry, _, _ = _carry_forward([_model_urn("p1.model.a_1")], report=report)
    carry.keep_kind("p1", [ResourceKinds.MODEL])
    carry.flush()
    carry.keep_urn(_model_urn("p1.model.a_1"))
    carry.flush()

    assert report.urns_carried_forward == 1


def test_carried_forward_counter_skips_urns_the_run_wrote() -> None:
    report = VertexAISourceReport()
    written, kept = _model_urn("p1.model.a_1"), _model_urn("p1.model.b_1")
    carry, current, _ = _carry_forward([written, kept], report=report)
    current.add_checkpoint_urn(type="mlModel", urn=written)
    carry.keep_kind("p1", [ResourceKinds.MODEL])
    carry.flush()

    assert set(current.urns) == {written, kept}
    assert report.urns_carried_forward == 1


def test_quota_stop_warning_names_one_switch() -> None:
    source = _training_source(extract_execution_metrics=True, rate_limit=False)

    stop_context = _quota_warning_contexts(source)[0]

    assert stop_context.count("switch=") == 1
    assert "switch=rate_limit" in stop_context


def test_model_version_base_reverses_the_builders_version_name() -> None:
    source = _model_source()
    version = MagicMock(version_id="12")

    urn = source.urn_builder.make_ml_model_urn(version, model_name="proj-one.model.m_1")

    assert model_version_base(urn) == "proj-one.model.m_1"


# A lasting loss writes the models and, in a listed project, fails the run;
# a refusal stops a read only before that read has worked in the region.


def _no_ml_metadata() -> PermissionDenied:
    return PermissionDenied(
        "no role",
        error_info=ErrorInfo(
            reason="IAM_PERMISSION_DENIED",
            metadata={"permission": "aiplatform.metadataStores.get"},
        ),
    )


def test_lasting_experiments_loss_writes_models_and_fails_listed_project() -> None:
    source = _region_source()

    run = _run_regions(source, {(REGIONS[0], "experiments"): _no_ml_metadata()})

    assert run.model_step_regions == REGIONS
    (context,) = _failure_contexts(source)
    assert "project=proj-one" in context
    assert "resource=experiments" in context
    assert "permission=aiplatform.metadataStores.get" in context
    assert "switch=include_experiments" in context


def test_lasting_loss_in_discovered_project_is_a_warning() -> None:
    source = _region_source()
    source.config.project_ids = []

    run = _run_regions(source, {(REGIONS[0], "training_jobs"): _iam_denied()})

    assert run.model_step_regions == REGIONS
    assert not source.report.failures
    assert any(
        "resource=training_jobs" in c and "switch=include_training_jobs" in c
        for c in _warning_contexts(source)
    )


def _evaluation_reads(
    outcomes: List[Any], reads: int
) -> Tuple[MagicMock, VertexAISourceReport, StoppedScopes]:
    report = VertexAISourceReport()
    stopped = StoppedScopes()
    read = MagicMock(side_effect=outcomes)
    for number in range(reads):
        DetailReader(report, stopped, lambda: Scope("proj-one", REGION))(
            DetailCalls.EVALUATION_LOOKUP, read, item=f"model-{number}"
        )
    return read, report, stopped


_EVALUATION_SCOPE = Scope(
    project="proj-one", region=REGION, kind=DetailCalls.EVALUATION_LOOKUP
)


def test_refusal_after_a_success_never_stops_the_read() -> None:
    outcomes = ["ok", *[_iam_denied()] * 30, "ok"]

    read, report, stopped = _evaluation_reads(outcomes, reads=32)

    assert read.call_count == 32
    assert stopped.stop_kind(_EVALUATION_SCOPE) is None
    assert len(report.warnings) == 1


def test_refusals_before_any_success_stop_after_25_with_one_warning() -> None:
    read, report, stopped = _evaluation_reads([_iam_denied()] * 30, reads=30)

    assert read.call_count == 25
    assert stopped.stop_kind(_EVALUATION_SCOPE) is ErrorKind.REFUSED
    assert len(report.warnings) == 1
