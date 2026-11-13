import threading
import traceback
from dataclasses import dataclass
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
    InternalServerError,
    MethodNotImplemented,
    ResourceExhausted,
    RetryError,
    ServerError,
    ServiceUnavailable,
    TooManyRequests,
    Unauthorized,
)
from google.auth.exceptions import GoogleAuthError

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
from datahub.ingestion.source.vertexai.vertexai_constants import (
    DETAIL_SWITCHES,
    RATE_LIMIT_SWITCH,
    REFUSALS_BEFORE_STOP,
    ResourceCategory,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
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
    if isinstance(exc, (*_TRANSIENT_ERRORS, GoogleAuthError)):
        return ErrorKind.TRANSIENT
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
            self.stop_kind(
                Scope(project=scope.project, region=scope.region, kind=call_kind)
            )
            is ErrorKind.TRANSIENT
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


def with_switch(context: str, switch: Optional[str]) -> str:
    return f"{context}, switch={switch}" if switch else context


@dataclass
class DetailResult(Generic[T]):
    value: Optional[T]
    keep_previous: bool


def _detail_context(
    scope: Scope, call_kind: str, exc: Exception, stopped: StoppedScopes
) -> str:
    return with_switch(
        scope_context(
            Scope(
                project=scope.project,
                region=scope.region,
                kind=call_kind,
                item=scope.item,
            ),
            exc,
        ),
        stopped.detail_switch(call_kind, exc),
    )


def _warn_item_lasting(
    report: VertexAISourceReport, context: str, exc: Exception
) -> None:
    report.warning(
        title="Detail could not be read",
        message="A read failed in a way that repeats, so it was skipped. What depends on it is written without it, or kept as it was when it cannot be written without it.",
        context=context,
        exc=exc,
    )


def read_list_item(
    scope: Scope,
    read: Callable[[], T],
    *,
    report: VertexAISourceReport,
    stopped: StoppedScopes,
    call_kind: str,
) -> Optional[T]:
    """Reads one item inside a list read. An error that repeats for this item
    (one deleted since the list) costs only the item; any other error fails
    the whole list, as before."""
    try:
        return read()
    except Exception as e:
        if classify_error(e) is not ErrorKind.ITEM_LASTING:
            raise
        _warn_item_lasting(report, _detail_context(scope, call_kind, e, stopped), e)
        stopped.lost(
            Scope(project=scope.project, region=scope.region, kind=call_kind),
            ErrorKind.ITEM_LASTING,
            e,
        )
        return None


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
        *,
        report: VertexAISourceReport,
    ) -> None:
        self._state_provider = state_provider
        self._platform = platform
        self._platform_instance = platform_instance
        self._env = env
        self._report = report
        self._lock = threading.Lock()
        self._urns: Set[str] = set()
        self._flows: Set[str] = set()
        self._model_names: Set[str] = set()
        self._prefixes: Set[str] = set()
        # (project, entity type): every earlier entity of the type is kept, if
        # that project was ingested before. Such entities carry no project.
        self._broad: Set[Tuple[str, str]] = set()

    def flush(self) -> None:
        if self._state_provider is None or not (
            self._urns or self._model_names or self._prefixes or self._broad
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
        broad_types = {
            entity_type
            for project, entity_type in self._broad
            if self._project_container(project) in last_urns
        }
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

    def keep_urn(self, urn: str) -> None:
        self._queue(self._urns, urn)

    def keep_model_versions(self, model_name: str) -> None:
        """Keeps every URN made of this model name and a numeric version ID."""
        self._queue(self._model_names, model_name)

    def keep_all_containers(self, project: str) -> None:
        self._queue(self._broad, (project, _CONTAINER))

    def keep_all_flows(self, project: str) -> None:
        self._queue(self._broad, (project, _DATA_FLOW), (project, _DATA_JOB))

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
        current = self.current_scope()
        scope = Scope(project=current.project, region=current.region, item=item)
        stop_scope = Scope(project=scope.project, region=scope.region, kind=call_kind)
        previous_stop = self.stopped.stop_kind(stop_scope)
        if previous_stop is not None:
            keep_previous = previous_stop is ErrorKind.TRANSIENT
            # A call on a whole list, with no item, is not an item kept.
            if keep_previous and scope.item is not None:
                self.report.report_item_kept_after_stop()
            return DetailResult(value=None, keep_previous=keep_previous)
        try:
            value = read()
        except Exception as e:
            kind = classify_error(e)
            if kind is ErrorKind.PROJECT_WIDE:
                raise
            context = _detail_context(scope, call_kind, e, self.stopped)
            if kind is ErrorKind.TRANSIENT:
                self.stopped.stop(stop_scope, kind, e)
                self.report.warning(
                    title="Detail read stopped after a temporary failure",
                    message="A read could not be completed after retries, so it is skipped for the rest of the run in this region. The items that need it keep their previous version, or are absent if never ingested, until a run completes without this stop.",
                    context=context,
                    exc=e,
                )
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
