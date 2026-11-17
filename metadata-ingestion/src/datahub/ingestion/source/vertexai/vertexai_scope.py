import threading
import traceback
import urllib.parse
from dataclasses import dataclass, replace
from enum import Enum
from typing import (
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
)

from google.api_core.exceptions import (
    Aborted,
    BadGateway,
    Cancelled,
    DeadlineExceeded,
    Forbidden,
    GatewayTimeout,
    GoogleAPICallError,
    GoogleAPIError,
    InternalServerError,
    MethodNotImplemented,
    ResourceExhausted,
    RetryError,
    ServerError,
    ServiceUnavailable,
    TooManyRequests,
    Unauthorized,
)
from google.auth.exceptions import (
    GoogleAuthError,
    RefreshError,
    TimeoutError as AuthTimeoutError,
    TransportError,
)
from typing_extensions import LiteralString

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.source.common.gcp_errors import (
    error_info_permission,
    error_info_reason,
    error_info_service,
    is_service_disabled,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import StateProviderWrapper
from datahub.ingestion.source.vertexai.vertexai_builder import VertexAINameFormatter
from datahub.ingestion.source.vertexai.vertexai_constants import (
    DETAIL_SWITCHES,
    RATE_LIMIT_SWITCH,
    REFUSALS_BEFORE_STOP,
    UNEXPECTED_DETAIL_ERROR,
    UNEXPECTED_ERROR,
    UNEXPECTED_ERROR_TITLE,
    UNSCOPED_KEPT,
    UNSCOPED_KEPT_DISCOVERED,
    UNSCOPED_KEPT_TITLE,
    UNSCOPED_REMOVED,
    UNSCOPED_REMOVED_TITLE,
    DetailCalls,
    ResourceCategory,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ModelGroupKey,
    PipelineContainerKey,
    model_version_base,
)
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_utils import (
    get_project_container,
    get_resource_category_container,
)
from datahub.metadata.urns import (
    ContainerUrn,
    DataFlowUrn,
    DataJobUrn,
    DatasetUrn,
    MlModelDeploymentUrn,
    MlModelGroupUrn,
    MlModelUrn,
)
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import Urn, guess_entity_type

T = TypeVar("T")
V = TypeVar("V")

PROJECT_WIDE_REASONS = frozenset({"CONSUMER_INVALID"})

_NAMED_ENTITY_TYPES = frozenset(
    {
        MlModelUrn.ENTITY_TYPE,
        MlModelGroupUrn.ENTITY_TYPE,
        MlModelDeploymentUrn.ENTITY_TYPE,
        DatasetUrn.ENTITY_TYPE,
    }
)
_DATA_JOB = DataJobUrn.ENTITY_TYPE
_CONTAINER = ContainerUrn.ENTITY_TYPE
_DATA_FLOW = DataFlowUrn.ENTITY_TYPE
_ML_MODEL = MlModelUrn.ENTITY_TYPE
_ML_MODEL_GROUP = MlModelGroupUrn.ENTITY_TYPE
_TRANSIENT_ERRORS = (
    ResourceExhausted,
    TooManyRequests,
    ServiceUnavailable,
    DeadlineExceeded,
    GatewayTimeout,
    BadGateway,
    InternalServerError,
    RetryError,
)


class ErrorKind(Enum):
    PROJECT_WIDE = "project_wide"
    REFUSED = "refused"
    TRANSIENT = "transient"
    ITEM_LASTING = "item_lasting"


def classify_error(exc: BaseException) -> ErrorKind:
    if isinstance(exc, (*_TRANSIENT_ERRORS, TransportError, AuthTimeoutError)):
        return ErrorKind.TRANSIENT
    # Only a refresh the library marks retryable may succeed on the next run;
    # bad credentials fail the same way every time.
    if isinstance(exc, GoogleAuthError):
        if isinstance(exc, RefreshError) and exc.retryable:
            return ErrorKind.TRANSIENT
        return ErrorKind.REFUSED
    if isinstance(exc, Forbidden):
        if is_service_disabled(exc) or error_info_reason(exc) in PROJECT_WIDE_REASONS:
            return ErrorKind.PROJECT_WIDE
        return ErrorKind.REFUSED
    # Unauthenticated (gRPC) subclasses Unauthorized (REST).
    if isinstance(exc, Unauthorized):
        return ErrorKind.REFUSED
    # A call the service does not implement fails the same way next time.
    if isinstance(exc, MethodNotImplemented):
        return ErrorKind.ITEM_LASTING
    # Server errors and interrupted calls are temporary. Any other Google
    # error (a bad request, a missing resource) repeats for its item.
    if isinstance(exc, (ServerError, Aborted, Cancelled)):
        return ErrorKind.TRANSIENT
    return ErrorKind.ITEM_LASTING


def is_google_error(exc: BaseException) -> bool:
    return isinstance(exc, (GoogleAPIError, GoogleAuthError))


def error_title(exc: BaseException, title: LiteralString) -> LiteralString:
    return title if is_google_error(exc) else UNEXPECTED_ERROR_TITLE


def error_message(exc: BaseException, message: LiteralString) -> LiteralString:
    return message if is_google_error(exc) else UNEXPECTED_ERROR


def keeps_like_temporary(cause: Optional[BaseException]) -> bool:
    """Whether a lost read keeps every project's pipelines and containers, as a
    temporary loss does. With no error, the loss lasts."""
    # Remove only when Google says something the user must act on. An error of
    # ours is a bug, which says nothing about the data.
    return cause is not None and (
        not is_google_error(cause) or classify_error(cause) is ErrorKind.TRANSIENT
    )


@dataclass(frozen=True)
class Scope:
    project: str
    region: Optional[str] = None
    kind: Optional[str] = None
    item: Optional[str] = None

    def context(self) -> str:
        parts = [
            ("project", self.project),
            ("region", self.region),
            ("resource", self.kind),
            ("item", self.item),
        ]
        return ", ".join(f"{name}={value}" for name, value in parts if value)


def scope_context(scope: Scope, exc: Optional[BaseException] = None) -> str:
    context = scope.context()
    if not isinstance(exc, GoogleAPICallError):
        return context
    service = error_info_service(exc)
    if service:
        context += f", service={service}"
    permission = error_info_permission(exc)
    if permission:
        context += f", permission={permission}"
    return context


class StoppedScopes:
    """Detail reads stopped for the rest of the run in a region. A temporary
    failure stops a read at once. Refusals stop it only before it has worked in
    that region: after that, a refusal can only be one locked item."""

    def __init__(self, rate_limit_enabled: bool = True) -> None:
        self._stopped: Dict[Scope, Tuple[ErrorKind, Exception]] = {}
        self._worked: Set[Scope] = set()
        self._refusals: Dict[Scope, int] = {}
        self._first_losses: Dict[Scope, Tuple[ErrorKind, Optional[Exception]]] = {}
        self._lock = threading.Lock()
        self._rate_limit_enabled = rate_limit_enabled

    def stop(self, scope: Scope, kind: ErrorKind, exc: Exception) -> None:
        # The stored error keeps its traceback; only the stack's lines are reported.
        traceback.clear_frames(exc.__traceback__)
        with self._lock:
            self._stopped.setdefault(scope, (kind, exc))

    def worked(self, scope: Scope) -> None:
        if scope not in self._worked:
            with self._lock:
                self._worked.add(scope)

    def lost(self, scope: Scope, kind: ErrorKind, exc: Optional[Exception]) -> None:
        """Records the first loss of a read in a region for the report's summary."""
        if exc is not None:
            traceback.clear_frames(exc.__traceback__)
        with self._lock:
            self._first_losses.setdefault(scope, (kind, exc))

    def refused(self, scope: Scope, exc: Exception) -> None:
        self.lost(scope, ErrorKind.REFUSED, exc)
        with self._lock:
            if scope in self._worked:
                return
            refusals = self._refusals.get(scope, 0) + 1
            self._refusals[scope] = refusals
        if refusals >= REFUSALS_BEFORE_STOP:
            self.stop(scope, ErrorKind.REFUSED, exc)

    def stop_kind(self, scope: Scope) -> Optional[ErrorKind]:
        stop = self._stopped.get(scope)
        return stop[0] if stop else None

    def stop_error(self, scope: Scope) -> Optional[Exception]:
        stop = self._stopped.get(scope)
        return stop[1] if stop else None

    def lost_in(
        self, project: str
    ) -> List[Tuple[Scope, ErrorKind, Optional[Exception]]]:
        """Each read stopped or lost in the project, once per region and kind; a
        stop wins over an earlier loss."""
        with self._lock:
            lost: Dict[Scope, Tuple[ErrorKind, Optional[Exception]]] = {
                s: stop for s, stop in self._stopped.items() if s.project == project
            }
            for scope, loss in self._first_losses.items():
                if scope.project == project:
                    lost.setdefault(scope, loss)
        return [(scope, kind, exc) for scope, (kind, exc) in lost.items()]

    def any_transient(self, scope: Scope, call_kinds: Iterable[str]) -> bool:
        """True when one of these calls is stopped for a while in the region of
        `scope`: an item that needs it is kept, so its other reads are waste."""
        return any(
            self.stop_kind(replace(scope, kind=call_kind)) is ErrorKind.TRANSIENT
            for call_kind in call_kinds
        )

    def detail_switch(
        self, call_kind: Optional[str], exc: Optional[BaseException]
    ) -> Optional[str]:
        """The one switch to name in a warning. A quota error is fixed by pacing
        requests, so that wins over the switch that turns the call off."""
        cause = exc.cause if isinstance(exc, RetryError) else exc
        if not self._rate_limit_enabled and isinstance(
            cause, (ResourceExhausted, TooManyRequests)
        ):
            return RATE_LIMIT_SWITCH
        return DETAIL_SWITCHES.get(call_kind) if call_kind else None

    def detail_context(self, scope: Scope, exc: Optional[BaseException]) -> str:
        return with_switch(
            scope_context(scope, exc), self.detail_switch(scope.kind, exc)
        )


def with_switch(context: str, switch: Optional[str]) -> str:
    return f"{context}, switch={switch}" if switch else context


@dataclass
class DetailResult(Generic[T]):
    value: Optional[T]
    keep_previous: bool


def _warn_item_lasting(
    report: VertexAISourceReport, context: str, exc: Exception
) -> None:
    report.warning(
        title=error_title(exc, "Detail could not be read"),
        # An item read keeps nothing, so the connector-error body for a lost
        # list read would wrongly say earlier entities are kept.
        message=(
            "A read failed in a way that repeats, so it was skipped. What depends on it is written without it, or kept as it was when it cannot be written without it."
            if is_google_error(exc)
            else UNEXPECTED_DETAIL_ERROR
        ),
        context=context,
        exc=exc,
    )


def _entity_name(urn: str, entity_type: str) -> Optional[str]:
    if entity_type in _NAMED_ENTITY_TYPES:
        # The name is the second slot of (platform, name, env).
        return Urn.from_string(urn).entity_ids[1]
    return None


def urn_entity_name(urn: str) -> Optional[str]:
    return _entity_name(urn, guess_entity_type(urn))


def _flow_of_job(urn: str) -> Optional[str]:
    try:
        return str(DataJobUrn.from_string(urn).get_data_flow_urn())
    except InvalidUrnError:
        return None


def _pipeline_name(flow_urn: str, instance_prefix: str) -> Optional[str]:
    """The pipeline name the extractor keyed the flow's folder with."""
    try:
        flow_id = DataFlowUrn.from_string(flow_urn).flow_id
    except InvalidUrnError:
        return None
    # The URN encodes reserved characters such as "," and "("; the folder key does not.
    return urllib.parse.unquote(flow_id.removeprefix(instance_prefix))


def _model_group_name(urn: str) -> Optional[str]:
    try:
        return _entity_name(urn, _ML_MODEL_GROUP)
    except InvalidUrnError:
        return None


def _model_group_prefix(project: str) -> str:
    return VertexAINameFormatter(lambda: project).format_model_group_name("")


class CarryForward:
    """Keeps earlier URNs in the current checkpoint, so stale entity removal does
    not soft-delete what this run could not read. Worker threads queue keeps
    under a lock; the main thread applies them once, at the end of the run."""

    def __init__(
        self,
        state_provider: Optional[StateProviderWrapper],
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        discovered: bool = False,
        *,
        report: VertexAISourceReport,
    ) -> None:
        self._state_provider = state_provider
        self._platform = platform
        self._platform_instance = platform_instance
        self._env = env
        self._discovered = discovered
        self._report = report
        self._lock = threading.Lock()
        self._urns: Set[str] = set()
        self._flows: Set[str] = set()
        self._model_names: Set[str] = set()
        self._prefixes: Set[str] = set()
        # (lost read, entity type): every earlier entity of the type is kept, if
        # the read's project was ingested before. Their URNs do not show the project.
        self._broad: Set[Tuple[Scope, str]] = set()
        # The same, for a lasting loss in a discovered project: left to removal.
        self._unkept: Set[Tuple[Scope, str]] = set()
        # Lost reads whose error is not Google's.
        self._ours: Set[Scope] = set()
        # Projects with a lost read that covers their experiments.
        self._experiments_lost: Set[str] = set()

    def flush(self) -> None:
        if self._state_provider is None or not (
            self._urns
            or self._model_names
            or self._prefixes
            or self._broad
            or self._unkept
        ):
            return
        job_id = StaleEntityRemovalHandler.compute_job_id(self._platform)
        # The current checkpoint is None when stale removal is off.
        current = self._state_provider.get_current_checkpoint(job_id)
        if current is None or not isinstance(current.state, GenericCheckpointState):
            return
        last = self._state_provider.get_last_checkpoint(job_id, GenericCheckpointState)
        if last is None:
            return
        last_urns = set(last.state.urns)
        broad = self._ingested_before(self._broad, last_urns)
        unkept = self._ingested_before(self._unkept, last_urns)
        # A failed run soft-deletes nothing: stale removal copies the last state.
        if self._report.failures:
            broad |= unkept
            unkept = set()
        broad_types = {entity_type for _, entity_type in broad}
        for scope in sorted({scope for scope, _ in broad}, key=Scope.context):
            self._report.warning(
                title=UNSCOPED_KEPT_TITLE,
                message=UNEXPECTED_ERROR
                if scope in self._ours
                else UNSCOPED_KEPT_DISCOVERED
                if self._discovered
                else UNSCOPED_KEPT,
                context=scope.context(),
            )
        # Only experiment folders cannot be tied to their project. A broad keep
        # already keeps every container or every flow.
        if _CONTAINER not in broad_types:
            folder_projects = {s.project for s, t in unkept if t == _CONTAINER}
            for project in sorted(folder_projects & self._experiments_lost):
                self._report.warning(
                    title=UNSCOPED_REMOVED_TITLE,
                    message=UNSCOPED_REMOVED,
                    context=Scope(project=project).context(),
                )
            self._keep_model_group_containers(folder_projects, last_urns)
        if _DATA_FLOW not in broad_types:
            self._keep_own_pipelines(
                {s.project for s, t in unkept if t == _DATA_FLOW}, last_urns
            )
        prefixes = tuple(self._prefixes)
        state = current.state
        # Counted as a difference so URNs this run wrote itself are not counted.
        urns_before = len(state.urns)
        for urn in last.state.urns:
            entity_type = guess_entity_type(urn)
            if entity_type in broad_types or self._keeps(urn, entity_type, prefixes):
                state.add_checkpoint_urn(type=entity_type, urn=urn)
        self._report.report_urns_carried_forward(len(state.urns) - urns_before)

    def _keeps(self, urn: str, entity_type: str, prefixes: Tuple[str, ...]) -> bool:
        if urn in self._urns:
            return True
        if entity_type == _DATA_JOB and self._flows:
            return _flow_of_job(urn) in self._flows
        if entity_type == _ML_MODEL and model_version_base(urn) in self._model_names:
            return True
        name = _entity_name(urn, entity_type) if prefixes else None
        return name is not None and name.startswith(prefixes)

    def _ingested_before(
        self, keeps: Set[Tuple[Scope, str]], last_urns: Set[str]
    ) -> Set[Tuple[Scope, str]]:
        return {
            (scope, entity_type)
            for scope, entity_type in keeps
            if self._project_container(scope.project) in last_urns
        }

    def _project_container(self, project: str) -> str:
        return get_project_container(
            project_id=project,
            platform=self._platform,
            platform_instance=self._platform_instance,
            env=self._env,
        ).as_urn()

    def _queue(self, keeps: Set[T], *values: T) -> None:
        with self._lock:
            keeps.update(values)

    def keep_kind(self, project: str, kinds: Sequence[str]) -> None:
        # Dataset names carry the platform instance in front of the project.
        owners = [project]
        if self._platform_instance:
            owners.append(f"{self._platform_instance}.{project}")
        self._queue(
            self._prefixes, *(f"{owner}.{kind}." for owner in owners for kind in kinds)
        )

    def keep_project(self, project: str) -> None:
        self._queue(
            self._urns,
            self._project_container(project),
            *(
                get_resource_category_container(
                    project_id=project,
                    platform=self._platform,
                    platform_instance=self._platform_instance,
                    env=self._env,
                    category=category,
                ).as_urn()
                for category in ResourceCategory.all()
            ),
        )

    def _keep_model_group_containers(
        self, projects: Set[str], last_urns: Set[str]
    ) -> None:
        # A model group's folder key holds the group's name, which the group's
        # URN carries, so it is rebuilt like the project's category folders.
        if not projects:
            return
        names = [
            name
            for urn in last_urns
            if guess_entity_type(urn) == _ML_MODEL_GROUP
            and (name := _model_group_name(urn)) is not None
        ]
        for project in projects:
            prefix = _model_group_prefix(project)
            self._urns.update(
                ModelGroupKey(
                    project_id=project,
                    platform=self._platform,
                    instance=self._platform_instance,
                    env=self._env,
                    model_group_name=name,
                ).as_urn()
                for name in names
                if name.startswith(prefix)
            )

    def _keep_own_pipelines(self, projects: Set[str], last_urns: Set[str]) -> None:
        # A flow is this project's when the project's folder for it was written
        # before. The folder key holds the flow id without its instance prefix.
        if not projects:
            return
        instance_prefix = (
            f"{self._platform_instance}." if self._platform_instance else ""
        )
        flows = [
            (urn, name)
            for urn in last_urns
            if guess_entity_type(urn) == _DATA_FLOW
            and (name := _pipeline_name(urn, instance_prefix)) is not None
        ]
        for project in projects:
            for urn, name in flows:
                folder = PipelineContainerKey(
                    project_id=project,
                    platform=self._platform,
                    instance=self._platform_instance,
                    env=self._env,
                    pipeline_name=name,
                ).as_urn()
                if folder in last_urns:
                    self._urns.update((folder, urn))
                    self._flows.add(urn)

    def keep_urn(self, urn: str) -> None:
        self._queue(self._urns, urn)

    def keep_model_versions(self, model_name: str) -> None:
        """Keeps every URN made of this model name and a numeric version ID."""
        self._queue(self._model_names, model_name)

    def keep_all_containers(
        self, lost: Scope, cause: Optional[BaseException], *, experiments: bool
    ) -> None:
        """`experiments`: the lost read covers the project's experiments."""
        self._queue_broad(lost, cause, _CONTAINER)
        if experiments:
            self._queue(self._experiments_lost, lost.project)

    def keep_all_flows(self, lost: Scope, cause: Optional[BaseException]) -> None:
        self._queue_broad(lost, cause, _DATA_FLOW, _DATA_JOB)

    def _queue_broad(
        self, lost: Scope, cause: Optional[BaseException], *entity_types: str
    ) -> None:
        # Nobody chose a discovered project, so its lasting loss must not pause
        # removal in every project on every run. Its entities are restored by
        # the first run that writes them again.
        keeps = (
            self._broad
            if keeps_like_temporary(cause) or not self._discovered
            else self._unkept
        )
        self._queue(keeps, *((lost, entity_type) for entity_type in entity_types))
        if cause is not None and not is_google_error(cause):
            self._queue(self._ours, lost)

    def keep_flow(self, flow_urn: str) -> None:
        """Keeps the flow and every task of it."""
        self._queue(self._urns, flow_urn)
        self._queue(self._flows, flow_urn)


@dataclass(frozen=True)
class DetailReader:
    """Reads details in the source's current project and region."""

    report: VertexAISourceReport
    stopped: StoppedScopes
    current_scope: Callable[[], Scope]

    def __call__(
        self, call_kind: str, read: Callable[[], T], item: Optional[str] = None
    ) -> DetailResult[T]:
        """Reads a detail of one item and decides, from the error kind, what the
        item is written with. A stop covers the item's region."""
        stop_scope = replace(self.current_scope(), kind=call_kind)
        previous_stop = self.stopped.stop_kind(stop_scope)
        if previous_stop is not None:
            keep_previous = previous_stop is ErrorKind.TRANSIENT
            # A call on a whole list, with no item, is not an item kept.
            if keep_previous and item is not None:
                self.report.report_item_kept_after_stop()
            return DetailResult(value=None, keep_previous=keep_previous)
        try:
            value = read()
        except Exception as e:
            kind = classify_error(e)
            if kind is ErrorKind.PROJECT_WIDE:
                raise
            context = self.stopped.detail_context(replace(stop_scope, item=item), e)
            if kind is ErrorKind.TRANSIENT:
                self.stopped.stop(stop_scope, kind, e)
                self.report.warning(
                    title="Detail read stopped after a temporary failure",
                    message="A read could not be completed after retries, so it is skipped for the rest of the run in this region. The items that need it keep their previous version, or are absent if never ingested, until a run completes without this stop.",
                    context=context,
                    exc=e,
                )
                if call_kind == DetailCalls.PIPELINE_LOOKUP:
                    self.report.report_pipeline_links_elsewhere(context)
                return DetailResult(value=None, keep_previous=True)
            if kind is ErrorKind.REFUSED:
                self.stopped.refused(stop_scope, e)
                self.report.warning(
                    title="Detail read refused",
                    message="A read was refused for this item, so the item is written without it, or kept as it was when it cannot be written without it. Until the read has worked once in this region, repeated refusals stop it there for the rest of the run.",
                    context=context,
                    exc=e,
                )
            else:
                _warn_item_lasting(self.report, context, e)
                self.stopped.lost(stop_scope, kind, e)
            return DetailResult(value=None, keep_previous=False)
        self.stopped.worked(stop_scope)
        return DetailResult(value=value, keep_previous=False)

    def region_list(
        self,
        call_kind: str,
        read: Callable[[], Dict[str, V]],
        kind: str,
        carry: CarryForward,
    ) -> DetailResult[Dict[str, V]]:
        """Reads a region-wide list. When it is not read, its own entities are
        kept, since nothing else writes them."""
        result = self(call_kind, read)
        if result.value is None:
            carry.keep_kind(self.current_scope().project, [kind])
        return result
