from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Sequence, Set, Tuple

from google.cloud.aiplatform_v1.types import PipelineTaskDetail

from datahub.ingestion.source.vertexai.vertexai_constants import PipelineSpecKeys
from datahub.utilities.time import datetime_to_ts_millis


@dataclass(frozen=True)
class PipelineStep:
    path: str
    shown_name: str
    parent_path: Optional[str]
    is_loop: bool
    upstream_paths: Tuple[str, ...]


@dataclass(frozen=True)
class TaskRun:
    detail: PipelineTaskDetail
    step: PipelineStep
    run_path: str
    parent_run_path: Optional[str]
    upstream_run_paths: Tuple[str, ...]


@dataclass
class RunTree:
    runs: List[TaskRun]
    # Run paths of details that map to no step. For a missing parent or a parent
    # cycle, the path starts at the detail where the chain breaks.
    unmatched: List[str]
    # Task names of details at the root (parent task id 0) other than the run's own.
    extra_roots: List[str]
    # Passes (ParallelFor iterations) of loop steps, and details that match a step
    # but have not started, so get no run.
    loop_passes: int
    not_started: int


def _as_mapping(value: object) -> Mapping[str, object]:
    # A malformed spec must still fail the job's guarded build, not read as empty.
    if not isinstance(value, Mapping):
        raise TypeError(f"Expected a mapping in the pipeline spec, got {type(value)}")
    return value


def _dependent_keys(task_spec: Mapping[str, object]) -> List[str]:
    dependent = task_spec.get(PipelineSpecKeys.DEPENDENT_TASKS, [])
    if isinstance(dependent, (str, bytes)) or not isinstance(dependent, Sequence):
        raise TypeError(f"Expected a list of dependentTasks, got {type(dependent)}")
    for key in dependent:
        if not isinstance(key, str):
            raise TypeError(f"Expected a task name in dependentTasks, got {type(key)}")
    return list(dependent)


def _join(prefix: Optional[str], key: str) -> str:
    return f"{prefix}.{key}" if prefix else key


def build_step_tree(pipeline_spec: Mapping[str, object]) -> Dict[str, PipelineStep]:
    components = _as_mapping(
        _as_mapping(pipeline_spec).get(PipelineSpecKeys.COMPONENTS, {})
    )
    steps: Dict[str, PipelineStep] = {}

    def walk(
        dag: Mapping[str, object],
        parent_path: Optional[str],
        open_components: Tuple[str, ...],
    ) -> None:
        tasks = _as_mapping(dag.get(PipelineSpecKeys.TASKS, {}))
        for dag_key in tasks:
            task_spec = _as_mapping(tasks[dag_key])
            path = _join(parent_path, dag_key)
            component_name = _as_mapping(
                task_spec.get(PipelineSpecKeys.COMPONENT_REF, {})
            ).get(PipelineSpecKeys.NAME)
            component = (
                _as_mapping(components.get(component_name, {}))
                if isinstance(component_name, str)
                else {}
            )
            child_dag = component.get(PipelineSpecKeys.DAG)
            task_info = _as_mapping(task_spec.get(PipelineSpecKeys.TASK_INFO, {}))
            steps[path] = PipelineStep(
                path=path,
                shown_name=str(task_info.get(PipelineSpecKeys.NAME) or dag_key),
                parent_path=parent_path,
                is_loop=PipelineSpecKeys.PARAMETER_ITERATOR in task_spec
                or PipelineSpecKeys.ARTIFACT_ITERATOR in task_spec,
                upstream_paths=tuple(
                    _join(parent_path, key)
                    for key in _dependent_keys(task_spec)
                    if key in tasks
                ),
            )
            # A component that contains itself would otherwise recurse forever.
            if (
                child_dag is not None
                and isinstance(component_name, str)
                and component_name not in open_components
            ):
                walk(
                    _as_mapping(child_dag),
                    path,
                    open_components + (component_name,),
                )

    root = _as_mapping(_as_mapping(pipeline_spec).get(PipelineSpecKeys.ROOT, {}))
    walk(_as_mapping(root.get(PipelineSpecKeys.DAG, {})), None, ())
    return steps


def is_started(detail: PipelineTaskDetail) -> bool:
    # A start time that is present but zero counts as unset.
    return (
        "start_time" in detail
        and datetime_to_ts_millis(detail.start_time) != 0
        and detail.state
        not in (
            PipelineTaskDetail.State.NOT_TRIGGERED,
            PipelineTaskDetail.State.PENDING,
        )
    )


@dataclass(frozen=True)
class _Position:
    run_path: str
    step_path: str
    step: Optional[PipelineStep]
    started: bool
    is_worker: bool
    # Cut off from the run's root (missing parent, parent cycle or an extra root);
    # never matches a step.
    detached: bool
    # An extra root detail or one of its descendants: skipped, not reported as unmatched.
    under_extra_root: bool
    # Run path of the nearest started ancestor step; None means the pipeline run.
    parent_run_path: Optional[str]

    def child(
        self, steps: Dict[str, PipelineStep], detail: PipelineTaskDetail
    ) -> "_Position":
        run_path = _join(self.run_path, detail.task_name)
        reported_parent = (
            self.run_path
            if self.step is not None and self.started
            else self.parent_run_path
        )
        if self.detached:
            return _Position(
                run_path=run_path,
                step_path="",
                step=None,
                started=False,
                is_worker=False,
                detached=True,
                under_extra_root=self.under_extra_root,
                parent_run_path=self.parent_run_path,
            )
        if self.step is not None and self.step.is_loop:
            # A loop worker has no step; its children belong to the loop's step.
            return _Position(
                run_path=run_path,
                step_path=self.step_path,
                step=None,
                started=False,
                is_worker=True,
                detached=False,
                under_extra_root=False,
                parent_run_path=reported_parent,
            )
        step_path = _join(self.step_path, detail.task_name)
        return _Position(
            run_path=run_path,
            step_path=step_path,
            step=steps.get(step_path),
            started=is_started(detail),
            is_worker=False,
            detached=False,
            under_extra_root=False,
            parent_run_path=reported_parent,
        )


def build_run_tree(
    steps: Dict[str, PipelineStep],
    details: Sequence[PipelineTaskDetail],
    run_name: str,
) -> RunTree:
    """`run_name` is the pipeline run's name, which its own root detail carries."""
    by_id = {detail.task_id: detail for detail in details}
    at_root = [d for d in details if d.parent_task_id == 0]
    root = next(
        (d for d in at_root if d.task_name == run_name),
        at_root[0] if len(at_root) == 1 else None,
    )
    positions: Dict[int, _Position] = {}

    def locate(detail: PipelineTaskDetail, visiting: Set[int]) -> _Position:
        if detail.task_id in positions:
            return positions[detail.task_id]
        chain = visiting | {detail.task_id}
        if root is not None and detail.task_id == root.task_id:
            position = _Position(
                run_path="",
                step_path="",
                step=None,
                started=False,
                is_worker=False,
                detached=False,
                under_extra_root=False,
                parent_run_path=None,
            )
        elif (
            detail.parent_task_id == 0
            or detail.parent_task_id in chain
            or detail.parent_task_id not in by_id
        ):
            position = _Position(
                run_path=detail.task_name,
                step_path="",
                step=None,
                started=False,
                is_worker=False,
                detached=True,
                under_extra_root=detail.parent_task_id == 0,
                parent_run_path=None,
            )
        else:
            position = locate(by_id[detail.parent_task_id], chain).child(steps, detail)
        positions[detail.task_id] = position
        return position

    unmatched: List[str] = []
    loop_passes = 0
    not_started = 0
    started: List[Tuple[PipelineTaskDetail, PipelineStep, _Position]] = []
    for detail in details:
        if detail.parent_task_id == 0:
            continue
        position = locate(detail, set())
        if position.under_extra_root:
            continue
        if position.step is None:
            if position.is_worker:
                loop_passes += 1
            else:
                unmatched.append(position.run_path)
        elif position.started:
            started.append((detail, position.step, position))
        else:
            not_started += 1

    run_paths_by_step = {
        (detail.parent_task_id, step.path): position.run_path
        for detail, step, position in reversed(started)
    }
    runs = [
        TaskRun(
            detail=detail,
            step=step,
            run_path=position.run_path,
            parent_run_path=position.parent_run_path,
            upstream_run_paths=tuple(
                run_paths_by_step[(detail.parent_task_id, upstream)]
                for upstream in step.upstream_paths
                if (detail.parent_task_id, upstream) in run_paths_by_step
            ),
        )
        for detail, step, position in started
    ]
    return RunTree(
        runs=runs,
        unmatched=unmatched,
        extra_roots=[d.task_name for d in at_root if d is not root],
        loop_passes=loop_passes,
        not_started=not_started,
    )
