import json
import re
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import (
    PermissionDenied,
    RetryError,
    ServiceUnavailable,
)
from google.cloud.aiplatform_v1.types import (
    Execution,
    PipelineState,
    PipelineTaskDetail,
)
from google.rpc.error_details_pb2 import ErrorInfo

from datahub.configuration.common import PipelineExecutionError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai.vertexai import VertexAISource
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    BACKING_JOBS_NOT_LINKED,
    BACKING_JOBS_NOT_LINKED_TITLE,
    BACKING_JOBS_SKIPPED_TITLE,
    EXTRA_ROOT_DETAILS_TITLE,
    PIPELINE_LIST_LOST_JOBS,
    UNMATCHED_DETAILS_TITLE,
    ResourceReads,
    ResourceTypes,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    AutoMLJobConfig,
    ModelToUpload,
)
from datahub.ingestion.source.vertexai.vertexai_pipeline_extractor import (
    VertexAIPipelineExtractor,
)
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_result_type_utils import (
    RunOutcome,
    task_run_outcome,
)
from datahub.ingestion.source.vertexai.vertexai_scope import DetailResult
from datahub.ingestion.source.vertexai.vertexai_state import VertexAIStateHandler
from datahub.metadata.schema_classes import (
    DataProcessInstanceRunEventClass,
    MetadataChangeProposalClass,
)
from datahub.utilities.time import datetime_to_ts_millis
from tests.integration.vertexai.mock_vertexai import (
    PIPELINE_TASKS_FIXTURE_DIR,
    gen_custom_job_from_fixture,
    gen_mock_training_automl_job,
    gen_pipeline_job_from_fixture,
    run_pipeline_fixtures,
)

NESTED = "nested-pipe-20261104161316"
LOOP = "loop-pipe-20261103171359"
CACHE1 = "demo-cache-20261123155235"
FAIL = "demo-failure-20261123155239"
CANCEL = "demo-cancel-20261123155241"
CACHE2 = "demo-cache-20261123160226"
EXTRACTOR = "datahub.ingestion.source.vertexai.vertexai_pipeline_extractor"

WorldAspects = Dict[str, Dict[str, List[Dict[str, Any]]]]
Fixture = Union[str, Dict[str, Any]]


def job(flow: str, task_id: str) -> str:
    return f"urn:li:dataJob:(urn:li:dataFlow:(vertexai,{flow},PROD),{task_id})"


def run(run_id: str) -> str:
    return f"urn:li:dataProcessInstance:my-project.pipeline_task_run.{run_id}"


def run_world_with_report(
    fixtures: Sequence[Fixture], **config: Any
) -> Tuple[WorldAspects, VertexAISourceReport]:
    """Run the source over the fixtures; return entity URN -> aspect name -> aspect JSON list, and the report.

    A fixture is a name in `pipeline_tasks/` or an in-memory pipeline run.
    `include_training_jobs` defaults to whether "custom_jobs" is in `fixtures`.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        sink_path = Path(tmp_dir) / "mcps.json"
        report = run_pipeline_fixtures(fixtures, str(sink_path), **config)
        mcps = json.loads(sink_path.read_text())

    world: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for mcp in mcps:
        world[mcp["entityUrn"]][mcp["aspectName"]].append(mcp["aspect"]["json"])
    return {urn: dict(aspects) for urn, aspects in world.items()}, report


def run_world(fixtures: Sequence[Fixture], **config: Any) -> WorldAspects:
    return run_world_with_report(fixtures, **config)[0]


def load_fixture(name: str) -> Dict[str, Any]:
    return json.loads((PIPELINE_TASKS_FIXTURE_DIR / f"{name}.json").read_text())


def listed_custom_jobs() -> List[Dict[str, Any]]:
    return json.loads((PIPELINE_TASKS_FIXTURE_DIR / "custom_jobs.json").read_text())


def _task_details(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return data["jobDetail"]["taskDetails"]


def loop_run_with_summarize() -> Dict[str, Any]:
    """loop_run plus a body step `summarize` after `evaluate`, run once per worker."""
    data = load_fixture("loop_run")
    body = data["pipelineSpec"]["components"]["comp-for-loop-2"]["dag"]["tasks"]
    body["summarize"] = {
        "componentRef": {"name": "comp-evaluate"},
        "dependentTasks": ["evaluate"],
        "taskInfo": {"name": "summarize"},
    }
    evaluations = [d for d in _task_details(data) if d["taskName"] == "evaluate"]
    for new_id, evaluation in zip(
        ("1000000000000000071", "1000000000000000072"), evaluations, strict=True
    ):
        _task_details(data).append(
            {**evaluation, "taskId": new_id, "taskName": "summarize"}
        )
    return data


def failure_run_with_boom_after_condition() -> Dict[str, Any]:
    """failure_run where `boom` also depends on `condition-1`, which never ran."""
    data = load_fixture("failure_run")
    boom = data["pipelineSpec"]["root"]["dag"]["tasks"]["boom"]
    boom["dependentTasks"] = ["emit", "condition-1"]
    return data


def nested_run_with_ghost() -> Dict[str, Any]:
    """nested_run plus a started detail that no step in the pipeline spec describes."""
    data = load_fixture("nested_run")
    announce = next(d for d in _task_details(data) if d["taskName"] == "announce")
    _task_details(data).append(
        {**announce, "taskId": "1000000000000000073", "taskName": "ghost"}
    )
    return data


def nested_run_with_pending_announce() -> Dict[str, Any]:
    """nested_run where `announce` is PENDING although it has a start time."""
    data = load_fixture("nested_run")
    announce = next(d for d in _task_details(data) if d["taskName"] == "announce")
    announce["state"] = int(PipelineTaskDetail.State.PENDING)
    return data


# A timestamp that is present but zero, which must count as unset.
EPOCH = "1970-01-01T00:00:00Z"


def nested_run_with_sub_millisecond_start_announce() -> Dict[str, Any]:
    """nested_run where `announce` starts within the epoch's first millisecond."""
    data = load_fixture("nested_run")
    announce = next(d for d in _task_details(data) if d["taskName"] == "announce")
    announce["startTime"] = "1970-01-01T00:00:00.000500Z"
    return data


def nested_run_with_zero_start_announce() -> Dict[str, Any]:
    """nested_run where `announce` has a start time of zero."""
    data = load_fixture("nested_run")
    announce = next(d for d in _task_details(data) if d["taskName"] == "announce")
    announce["startTime"] = EPOCH
    return data


DERIVED_FIXTURES = {
    "loop_run+summarize": loop_run_with_summarize,
    "failure_run+boom_after_condition": failure_run_with_boom_after_condition,
    "nested_run+pending_announce": nested_run_with_pending_announce,
    "nested_run+zero_start_announce": nested_run_with_zero_start_announce,
    "nested_run+sub_millisecond_start_announce": (
        nested_run_with_sub_millisecond_start_announce
    ),
}


def fixture_named(name: str) -> Fixture:
    return DERIVED_FIXTURES[name]() if name in DERIVED_FIXTURES else name


def pipeline_run(run_id: str) -> str:
    return f"urn:li:dataProcessInstance:my-project.pipeline_run.{run_id}"


def relationships(world: WorldAspects, run_id: str) -> Dict[str, Any]:
    (aspect,) = world[run(run_id)]["dataProcessInstanceRelationships"]
    return aspect


def datajob_links(world: WorldAspects, job_urn: str, edges: str) -> List[str]:
    """Destination URNs of the `inputDatajobEdges`, `inputDatasetEdges` or `outputDatasetEdges` patches."""
    patches: List[Any] = world[job_urn].get("dataJobInputOutput", [])
    return [
        op["value"]["destinationUrn"]
        for patch in patches
        for op in patch
        if op["path"].startswith(f"/{edges}/")
    ]


def datajob_upstreams(world: WorldAspects, job_urn: str) -> List[str]:
    return datajob_links(world, job_urn, "inputDatajobEdges")


def datajob_urns(world: WorldAspects, flow: str) -> List[str]:
    prefix = f"urn:li:dataJob:(urn:li:dataFlow:(vertexai,{flow},PROD),"
    return [urn for urn in world if urn.startswith(prefix)]


@pytest.mark.parametrize(
    "fixture,flow,step",
    [
        pytest.param("nested_run", "nested-pipe", "announce", id="top_level"),
        pytest.param(
            "nested_run", "nested-pipe", "exit-handler-1.importer", id="exit_handler"
        ),
        pytest.param(
            "nested_run",
            "nested-pipe",
            "exit-handler-1.branch-a.train",
            id="sub_pipeline",
        ),
        pytest.param(
            "failure_run", "demo-failure", "condition-1.emit-2", id="false_condition"
        ),
        pytest.param("loop_run", "loop-pipe", "for-loop-2.evaluate", id="parallel_for"),
        pytest.param(
            "nested_run",
            "nested-pipe",
            "exit-handler-1.branch-b.train",
            id="same_name",
        ),
    ],
)
def test_step_ids(fixture: str, flow: str, step: str) -> None:
    world = run_world([fixture])
    assert "dataJobInfo" in world[job(flow, step)]


def test_nested_run_writes_one_datajob_per_step() -> None:
    assert len(datajob_urns(run_world(["nested_run"]), "nested-pipe")) == 11


def test_each_datajob_writes_its_platform_instance_once() -> None:
    world = run_world(["nested_run"])
    for urn in datajob_urns(world, "nested-pipe"):
        assert len(world[urn]["dataPlatformInstance"]) == 1


@pytest.mark.parametrize(
    "incremental_lineage",
    [
        pytest.param(False, id="incremental_off"),
        pytest.param(True, id="incremental_on"),
    ],
)
def test_step_dependencies_resolve_within_container(
    incremental_lineage: bool,
) -> None:
    world = run_world(["nested_run"], incremental_lineage=incremental_lineage)
    upstreams = datajob_upstreams(
        world, job("nested-pipe", "exit-handler-1.model-upload")
    )
    assert upstreams == [job("nested-pipe", "exit-handler-1.importer")]


GCS_MODEL = "urn:li:dataset:(urn:li:dataPlatform:gcs,demo-loop-bucket/pipeline-root/123456789012/loop-pipe-20261103171359/train_1000000000000000055/model,PROD)"


TASK_LINKS = {
    "inputDatasets": (
        "loop_run",
        job("loop-pipe", "for-loop-2.evaluate"),
        "inputDatasetEdges",
        [GCS_MODEL],
    ),
    "outputDatasets": (
        "loop_run",
        job("loop-pipe", "train"),
        "outputDatasetEdges",
        [GCS_MODEL],
    ),
    "inputDatajobs": (
        "loop_run",
        job("loop-pipe", "check-registry-model"),
        "inputDatajobEdges",
        [job("loop-pipe", "importer")],
    ),
    # Refused at creation, so neither `flaky` nor `emit` has a run.
    "inputDatajobWithoutRun": (
        "refused_run",
        job("demo-cache", "flaky"),
        "inputDatajobEdges",
        [job("demo-cache", "emit")],
    ),
}


@pytest.mark.parametrize(
    "part",
    [
        pytest.param("inputDatasets", id="input_datasets"),
        pytest.param("outputDatasets", id="output_datasets"),
        pytest.param("inputDatajobs", id="upstream_steps"),
        pytest.param("inputDatajobWithoutRun", id="upstream_step_without_run"),
        pytest.param(False, id="incremental_off"),
        pytest.param(True, id="incremental_on"),
    ],
)
def test_task_lineage_patch(part: Union[str, bool]) -> None:
    """Task lineage is only ever added, so it is a patch whatever `incremental_lineage` says."""
    if isinstance(part, str):
        fixture, task, edges, expected = TASK_LINKS[part]
        for incremental_lineage in (False, True):
            world = run_world([fixture], incremental_lineage=incremental_lineage)
            assert datajob_links(world, task, edges) == expected
        return
    world = run_world(["loop_run"], incremental_lineage=part)
    linked = [
        urn
        for urn in datajob_urns(world, "loop-pipe")
        if "dataJobInputOutput" in world[urn]
    ]
    assert linked
    for urn in linked:
        for aspect in world[urn]["dataJobInputOutput"]:
            assert isinstance(aspect, list), urn


@pytest.mark.parametrize(
    "fixture,flow,step,shown",
    [
        pytest.param(
            "nested_run", "nested-pipe", "announce", "announce", id="top_level"
        ),
        pytest.param(
            "nested_run",
            "nested-pipe",
            "exit-handler-1.use-model",
            "check-champion",
            id="nested",
        ),
        pytest.param(
            "failure_run", "demo-failure", "condition-1", "never-true", id="container"
        ),
    ],
)
def test_step_shown_name(fixture: str, flow: str, step: str, shown: str) -> None:
    world = run_world([fixture])
    assert world[job(flow, step)]["dataJobInfo"][0]["name"] == shown


@pytest.mark.parametrize(
    "fixture,flow,step",
    [
        pytest.param("nested_run", "nested-pipe", "exit-handler-1", id="exit_handler"),
        pytest.param("failure_run", "demo-failure", "condition-1", id="condition"),
        pytest.param("loop_run", "loop-pipe", "for-loop-2", id="parallel_for"),
        pytest.param(
            "nested_run", "nested-pipe", "exit-handler-1.branch-a", id="sub_pipeline"
        ),
        pytest.param(
            "nested_run",
            "nested-pipe",
            "exit-handler-1.branch-b",
            id="nested_container",
        ),
    ],
)
def test_container_step_is_task(fixture: str, flow: str, step: str) -> None:
    world = run_world([fixture])
    assert world[job(flow, step)]["subTypes"][0]["typeNames"] == ["Task"]


@pytest.mark.parametrize(
    "fixture,flow,step,ancestors",
    [
        pytest.param(
            "nested_run",
            "nested-pipe",
            "exit-handler-1.importer",
            ["exit-handler-1"],
            id="exit_handler",
        ),
        pytest.param(
            "nested_run",
            "nested-pipe",
            "exit-handler-1.branch-a.train",
            ["exit-handler-1", "exit-handler-1.branch-a"],
            id="sub_pipeline",
        ),
        pytest.param(
            "failure_run",
            "demo-failure",
            "condition-1.emit-2",
            ["condition-1"],
            id="condition",
        ),
        pytest.param(
            "loop_run",
            "loop-pipe",
            "for-loop-2.evaluate",
            ["for-loop-2"],
            id="parallel_for",
        ),
    ],
)
def test_step_browse_path(
    fixture: str, flow: str, step: str, ancestors: List[str]
) -> None:
    path = run_world([fixture])[job(flow, step)]["browsePathsV2"][-1]["path"]
    assert [e["urn"] for e in path[3:]] == [job(flow, a) for a in ancestors]
    assert not any("worker" in e["id"] for e in path)


@pytest.mark.parametrize(
    "fixture,child,parent",
    [
        pytest.param(
            "nested_run", f"{NESTED}_announce", pipeline_run(NESTED), id="top_level"
        ),
        pytest.param(
            "nested_run",
            f"{NESTED}_exit-handler-1.branch-a.train",
            run(f"{NESTED}_exit-handler-1.branch-a"),
            id="nested",
        ),
        pytest.param(
            "loop_run",
            f"{LOOP}_for-loop-2.for-loop-2-worker-0.evaluate",
            run(f"{LOOP}_for-loop-2"),
            id="loop_pass",
        ),
    ],
)
def test_task_run_parent(fixture: str, child: str, parent: str) -> None:
    world = run_world([fixture])
    assert relationships(world, child)["parentInstance"] == parent
    pipeline_name, run_path = child.split("_", 1)
    assert relationships(world, child)["parentTemplate"] == job(
        re.sub(r"-\d{14}$", "", pipeline_name),
        re.sub(r"\.for-loop-2-worker-\d+", "", run_path),
    )


def test_loop_worker_is_not_a_step() -> None:
    world = run_world(["loop_run"])
    assert not [u for u in datajob_urns(world, "loop-pipe") if "worker" in u]
    assert sorted(
        u for u in world if "dataProcessInstance:" in u and "worker" in u
    ) == [
        run(f"{LOOP}_for-loop-2.for-loop-2-worker-0.evaluate"),
        run(f"{LOOP}_for-loop-2.for-loop-2-worker-1.evaluate"),
    ]


@pytest.mark.parametrize(
    "fixture,run_id,name",
    [
        pytest.param("nested_run", f"{NESTED}_announce", "announce", id="top_level"),
        pytest.param(
            "nested_run",
            f"{NESTED}_exit-handler-1.use-model",
            "check-champion",
            id="nested",
        ),
    ],
)
def test_task_run_name(fixture: str, run_id: str, name: str) -> None:
    world = run_world([fixture])
    assert world[run(run_id)]["dataProcessInstanceProperties"][0]["name"] == name


@pytest.mark.parametrize(
    "fixture,run_id,upstreams",
    [
        pytest.param(
            "loop_run",
            f"{LOOP}_check-registry-model",
            [f"{LOOP}_importer"],
            id="top_level",
        ),
        pytest.param(
            "nested_run",
            f"{NESTED}_exit-handler-1.model-upload",
            [f"{NESTED}_exit-handler-1.importer"],
            id="nested",
        ),
        pytest.param(
            "loop_run+summarize",
            f"{LOOP}_for-loop-2.for-loop-2-worker-0.summarize",
            [f"{LOOP}_for-loop-2.for-loop-2-worker-0.evaluate"],
            id="loop_pass",
        ),
        pytest.param(
            "loop_run+summarize",
            f"{LOOP}_for-loop-2.for-loop-2-worker-1.summarize",
            [f"{LOOP}_for-loop-2.for-loop-2-worker-1.evaluate"],
            id="loop_second_pass",
        ),
        pytest.param(
            "nested_run",
            f"{NESTED}_announce",
            [f"{NESTED}_exit-handler-1"],
            id="container_dependency",
        ),
        pytest.param(
            "failure_run+boom_after_condition",
            f"{FAIL}_boom",
            [f"{FAIL}_emit"],
            id="dependency_without_run",
        ),
    ],
)
def test_task_run_upstreams(fixture: str, run_id: str, upstreams: List[str]) -> None:
    world = run_world([fixture_named(fixture)])
    assert relationships(world, run_id)["upstreamInstances"] == [
        run(upstream) for upstream in upstreams
    ]


@pytest.mark.parametrize(
    "fixture,flow,step",
    [
        pytest.param(
            "failure_run", "demo-failure", "condition-1.emit-2", id="spec_only"
        ),
        pytest.param("failure_run", "demo-failure", "emit-3", id="upstream_failed"),
        pytest.param(
            "failure_run", "demo-failure", "condition-1", id="condition_false"
        ),
        pytest.param("cancel_run", "demo-cancel", "emit", id="cancelled_before_start"),
        pytest.param("running_run", "loop-pipe", "for-loop-2", id="pending"),
        pytest.param(
            "nested_run+pending_announce",
            "nested-pipe",
            "announce",
            id="pending_with_start_time",
        ),
        pytest.param(
            "nested_run+zero_start_announce",
            "nested-pipe",
            "announce",
            id="zero_start_time",
        ),
        pytest.param(
            "nested_run+sub_millisecond_start_announce",
            "nested-pipe",
            "announce",
            id="sub_millisecond_start_time",
        ),
    ],
)
def test_step_without_run(fixture: str, flow: str, step: str) -> None:
    world = run_world([fixture_named(fixture)])
    assert "dataJobInfo" in world[job(flow, step)]
    for urn, aspects in world.items():
        if "dataProcessInstance:" not in urn:
            continue
        for template in aspects.get("dataProcessInstanceRelationships", []):
            assert template.get("parentTemplate") != job(flow, step)
        for properties in aspects.get("dataProcessInstanceProperties", []):
            assert properties["created"]["time"] != 0


def test_detail_without_step_is_reported() -> None:
    world, report = run_world_with_report([nested_run_with_ghost()])
    assert not [u for u in world if u.endswith("_ghost")]
    assert report.pipeline_task_details_without_step == 1
    (warning,) = [
        w for w in report.warnings if w.title == "Pipeline task details match no step"
    ]
    assert f"{NESTED}" in str(warning.context)
    assert "ghost" in str(warning.context)


def failure_run_with_extra_roots() -> Dict[str, Any]:
    """failure_run plus two started details at the root, named after top-level
    steps that have no run of their own."""
    data = load_fixture("failure_run")
    boom = next(d for d in _task_details(data) if d["taskName"] == "boom")
    for task_id, name in (
        ("1000000000000000074", "emit-3"),
        ("1000000000000000075", "condition-1"),
    ):
        _task_details(data).append(
            {**boom, "taskId": task_id, "taskName": name, "parentTaskId": "0"}
        )
    return data


@pytest.mark.parametrize("part", ["not_written", "counted", "one_warning"])
def test_extra_root_details(part: str) -> None:
    world, report = run_world_with_report([failure_run_with_extra_roots()])
    if part == "not_written":
        assert sorted(u for u in world if "pipeline_task_run" in u) == [
            run(f"{FAIL}_boom"),
            run(f"{FAIL}_emit"),
        ]
    elif part == "counted":
        assert report.pipeline_task_details_extra_roots == 2
    else:
        (warning,) = [w for w in report.warnings if w.title == EXTRA_ROOT_DETAILS_TITLE]
        assert len(warning.context) == 1
        context = " ".join(warning.context)
        assert FAIL in context
        assert "extra=2" in context
        assert "emit-3" in context and "condition-1" in context


def failure_run_with_extra_root_family() -> Dict[str, Any]:
    """failure_run plus an extra root detail `condition-1` with a child `emit-2` and
    a grandchild, and an orphan whose parent id is in no detail."""
    data = load_fixture("failure_run")
    boom = next(d for d in _task_details(data) if d["taskName"] == "boom")
    for task_id, name, parent in (
        ("1000000000000000077", "condition-1", "0"),
        ("1000000000000000078", "emit-2", "1000000000000000077"),
        ("1000000000000000079", "grandchild", "1000000000000000078"),
        ("1000000000000000080", "orphan", "1000000000000000999"),
    ):
        _task_details(data).append(
            {**boom, "taskId": task_id, "taskName": name, "parentTaskId": parent}
        )
    return data


def test_extra_root_children_skipped_with_root() -> None:
    world, report = run_world_with_report([failure_run_with_extra_root_family()])
    assert sorted(u for u in world if "pipeline_task_run" in u) == [
        run(f"{FAIL}_boom"),
        run(f"{FAIL}_emit"),
    ]
    assert report.pipeline_task_details_without_step == 1
    (unmatched,) = [w for w in report.warnings if w.title == UNMATCHED_DETAILS_TITLE]
    assert [c.split(", ", 1)[1] for c in unmatched.context] == [
        "unmatched=1, sample=['orphan']"
    ]
    assert report.pipeline_task_details_extra_roots == 1


def failure_run_without_named_root() -> Dict[str, Any]:
    """failure_run whose root is not named after the run, plus a second detail at the root."""
    data = load_fixture("failure_run")
    root = next(d for d in _task_details(data) if d["parentTaskId"] == "0")
    root["taskName"] = "renamed-root"
    _task_details(data).append(
        {**root, "taskId": "1000000000000000081", "taskName": "second-root"}
    )
    return data


def test_no_root_reports_only_extra_roots() -> None:
    world, report = run_world_with_report([failure_run_without_named_root()])
    assert not [u for u in world if "pipeline_task_run" in u]
    assert report.pipeline_task_details_extra_roots == 2
    assert report.pipeline_task_details_without_step == 0
    assert [w.title for w in report.warnings] == [EXTRA_ROOT_DETAILS_TITLE]


def failure_run_with_extra_root_first() -> Dict[str, Any]:
    """failure_run with a started detail at the root listed before the run's own root."""
    data = load_fixture("failure_run")
    boom = next(d for d in _task_details(data) if d["taskName"] == "boom")
    _task_details(data).insert(
        0,
        {
            **boom,
            "taskId": "1000000000000000076",
            "taskName": "emit-3",
            "parentTaskId": "0",
        },
    )
    return data


def test_root_is_the_detail_named_after_the_run() -> None:
    world, report = run_world_with_report([failure_run_with_extra_root_first()])
    assert sorted(u for u in world if "pipeline_task_run" in u) == [
        run(f"{FAIL}_boom"),
        run(f"{FAIL}_emit"),
    ]
    assert report.pipeline_task_details_extra_roots == 1


def test_run_with_one_root_reports_no_extra_roots() -> None:
    _, report = run_world_with_report(["failure_run"])
    assert report.pipeline_task_details_extra_roots == 0
    assert not [w for w in report.warnings if w.title == EXTRA_ROOT_DETAILS_TITLE]


def test_job_without_spec_reports_its_details_once() -> None:
    data = load_fixture("nested_run")
    del data["pipelineSpec"]
    world, report = run_world_with_report([data])
    # every detail except the pipeline-level root one
    assert report.pipeline_task_details_without_step == len(_task_details(data)) - 1
    assert (
        len(
            [
                w
                for w in report.warnings
                if w.title == "Pipeline task details match no step"
            ]
        )
        == 1
    )
    assert not [u for u in world if f"pipeline_task_run.{NESTED}_" in u]
    assert not report.failures


def events(world: WorldAspects, urn: str) -> Dict[str, Dict[str, Any]]:
    """Run events of a DPI by status; fails if a status appears twice."""
    by_status: Dict[str, Dict[str, Any]] = {}
    for event in world[urn].get("dataProcessInstanceRunEvent", []):
        assert event["status"] not in by_status
        by_status[event["status"]] = event
    return by_status


def millis(iso: str) -> int:
    moment = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return (moment - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(
        milliseconds=1
    )


@pytest.mark.parametrize(
    "fixture,run_id,result,native",
    [
        pytest.param(
            "nested_run", f"{NESTED}_announce", "SUCCESS", "SUCCEEDED", id="succeeded"
        ),
        pytest.param("failure_run", f"{FAIL}_boom", "FAILURE", "FAILED", id="failed"),
        pytest.param(
            "cancel_run", f"{CANCEL}_sleeper", "SKIPPED", "CANCELLED", id="cancelled"
        ),
        pytest.param(
            "cache_hit_run",
            "demo-cache-20261123160226_emit",
            "SUCCESS",
            "SKIPPED",
            id="cache_hit",
        ),
        pytest.param(
            "running_run",
            "loop-pipe-20261103170000_check-registry-model",
            None,
            None,
            id="running",
        ),
        pytest.param(
            "loop_run", f"{LOOP}_importer", "SUCCESS", "SUCCEEDED", id="zero_duration"
        ),
    ],
)
def test_task_run_result(fixture: str, run_id: str, result: Any, native: Any) -> None:
    by_status = events(run_world([fixture]), run(run_id))
    assert "STARTED" in by_status
    if result is None:
        assert "COMPLETE" not in by_status
        return
    complete = by_status["COMPLETE"]
    assert complete["result"] == {"type": result, "nativeResultType": native}
    if run_id.endswith("_importer"):
        assert complete["durationMillis"] == 0


@pytest.mark.parametrize("which", ["created", "start_event", "end_event"])
def test_task_run_times(which: str) -> None:
    world = run_world(["failure_run"])
    urn = run(f"{FAIL}_boom")
    by_status = events(world, urn)
    if which == "created":
        created = world[urn]["dataProcessInstanceProperties"][0]["created"]["time"]
        assert created == millis("2026-11-23T10:22:42.218Z")
    elif which == "start_event":
        assert by_status["STARTED"]["timestampMillis"] == millis(
            "2026-11-23T10:25:06.015Z"
        )
    else:
        assert by_status["COMPLETE"]["timestampMillis"] == millis(
            "2026-11-23T10:27:19.184Z"
        )
        assert by_status["COMPLETE"]["durationMillis"] == 133169


def test_retried_task_run_attempt() -> None:
    by_status = events(run_world(["cache_first_run"]), run(f"{CACHE1}_flaky"))
    assert by_status["COMPLETE"]["attempt"] == 2


def test_cache_hit_task_run_attempt_is_first() -> None:
    # Vertex copies the earlier run's failed jobs into the cached detail.
    by_status = events(
        run_world(["cache_hit_run"]), run("demo-cache-20261123160226_flaky")
    )
    assert by_status["COMPLETE"]["attempt"] == 1


def test_cached_execution_is_success_whatever_the_task_state() -> None:
    detail = PipelineTaskDetail(
        state=PipelineTaskDetail.State.CANCELLED,
        execution=Execution(state=Execution.State.CACHED),
    )
    outcome = task_run_outcome(detail)
    assert outcome is not None
    assert (outcome.result, outcome.native) == ("SUCCESS", "CANCELLED")


def test_skipped_task_is_a_cache_hit_without_a_cached_execution() -> None:
    detail = PipelineTaskDetail(
        state=PipelineTaskDetail.State.SKIPPED,
        execution=Execution(state=Execution.State.COMPLETE),
    )
    outcome = task_run_outcome(detail)
    assert outcome is not None
    assert (outcome.result, outcome.native) == ("SUCCESS", "SKIPPED")


@pytest.mark.parametrize(
    "execution,outcome",
    [
        pytest.param(
            Execution.State.CACHED,
            RunOutcome(result="SUCCESS", native="42"),
            id="cache_hit",
        ),
        pytest.param(Execution.State.COMPLETE, None, id="not_cached"),
    ],
)
def test_unknown_task_state(
    execution: Execution.State, outcome: Optional[RunOutcome]
) -> None:
    detail = PipelineTaskDetail(execution=Execution(state=execution))
    # A state newer than the installed client library reads back as a bare number.
    PipelineTaskDetail.pb(detail).state = 42
    assert task_run_outcome(detail) == outcome


@pytest.mark.parametrize("cached", [False, True], ids=["not_cached", "cache_hit"])
def test_unknown_task_state_end_to_end(cached: bool) -> None:
    data = load_fixture("nested_run")
    announce = next(d for d in _task_details(data) if d["taskName"] == "announce")
    announce["state"] = 42
    if cached:
        announce["execution"]["state"] = "CACHED"
    by_status = events(run_world([data]), run(f"{NESTED}_announce"))
    if not cached:
        assert list(by_status) == ["STARTED"]
        return
    assert list(by_status) == ["STARTED", "COMPLETE"]
    assert by_status["COMPLETE"]["result"] == {
        "type": "SUCCESS",
        "nativeResultType": "42",
    }


def created_time(world: WorldAspects, urn: str) -> int:
    return world[urn]["dataProcessInstanceProperties"][0]["created"]["time"]


@pytest.mark.parametrize(
    "removed,expected_from",
    [
        pytest.param(("createTime",), "startTime", id="start_time"),
        pytest.param(("createTime", "startTime"), "endTime", id="end_time"),
    ],
)
def test_pipeline_run_created_time_fallback(
    removed: Tuple[str, ...], expected_from: str
) -> None:
    data = load_fixture("nested_run")
    expected = millis(data[expected_from])
    for key in removed:
        del data[key]
    assert created_time(run_world([data]), pipeline_run(NESTED)) == expected


def test_pipeline_run_zero_create_time_falls_back_to_start_time() -> None:
    data = load_fixture("nested_run")
    data["createTime"] = EPOCH
    world = run_world([data])
    assert created_time(world, pipeline_run(NESTED)) == millis(data["startTime"])


def nested_run_with_zero_end_times() -> Dict[str, Any]:
    """nested_run where the run and `announce` have an end time of zero."""
    data = load_fixture("nested_run")
    data["endTime"] = EPOCH
    announce = next(d for d in _task_details(data) if d["taskName"] == "announce")
    announce["endTime"] = EPOCH
    return data


@pytest.mark.parametrize(
    "urn",
    [
        pytest.param(pipeline_run(NESTED), id="pipeline_run"),
        pytest.param(run(f"{NESTED}_announce"), id="task_run"),
    ],
)
def test_zero_end_time_writes_no_end_event(urn: str) -> None:
    by_status = events(run_world([nested_run_with_zero_end_times()]), urn)
    assert list(by_status) == ["STARTED"]
    assert by_status["STARTED"]["timestampMillis"] != 0


def test_zero_start_time_writes_no_start_event() -> None:
    data = load_fixture("nested_run")
    data["startTime"] = EPOCH
    by_status = events(run_world([data]), pipeline_run(NESTED))
    assert list(by_status) == ["COMPLETE"]
    assert by_status["COMPLETE"]["timestampMillis"] == millis(data["endTime"])
    assert "durationMillis" not in by_status["COMPLETE"]


def test_pipeline_run_times() -> None:
    data = load_fixture("nested_run")
    # Updated after it ended, for example by a label edit.
    data["updateTime"] = "2026-11-04T12:00:00Z"
    world = run_world([data])
    assert created_time(world, pipeline_run(NESTED)) == millis(data["createTime"])
    by_status = events(world, pipeline_run(NESTED))
    assert by_status["STARTED"]["timestampMillis"] == millis(data["startTime"])
    assert by_status["COMPLETE"]["timestampMillis"] == millis(data["endTime"])


def test_task_run_created_time_falls_back_to_start_time() -> None:
    data = load_fixture("nested_run")
    announce = next(d for d in _task_details(data) if d["taskName"] == "announce")
    start = millis(announce["startTime"])
    del announce["createTime"]
    assert created_time(run_world([data]), run(f"{NESTED}_announce")) == start


def test_run_without_times_gets_no_record() -> None:
    data = load_fixture("nested_run")
    for key in ("createTime", "startTime", "endTime"):
        del data[key]
    world, report = run_world_with_report([data])
    assert pipeline_run(NESTED) not in world
    assert report.runs_skipped_without_time == 1
    assert "dataFlowInfo" in world["urn:li:dataFlow:(vertexai,nested-pipe,PROD)"]
    assert "dataJobInfo" in world[job("nested-pipe", "announce")]
    assert "parentInstance" not in relationships(world, f"{NESTED}_announce")


def test_pipeline_run_subtype() -> None:
    world = run_world(["nested_run"])
    assert world[pipeline_run(NESTED)]["subTypes"][0]["typeNames"] == ["Pipeline Run"]


@pytest.mark.parametrize(
    "fixture,run_id,result,native,started",
    [
        pytest.param(
            "nested_run",
            NESTED,
            "SUCCESS",
            "PIPELINE_STATE_SUCCEEDED",
            True,
            id="succeeded",
        ),
        pytest.param(
            "failure_run", FAIL, "FAILURE", "PIPELINE_STATE_FAILED", True, id="failed"
        ),
        pytest.param(
            "cancel_run",
            CANCEL,
            "SKIPPED",
            "PIPELINE_STATE_CANCELLED",
            True,
            id="cancelled",
        ),
        pytest.param(
            "running_run", "loop-pipe-20261103170000", None, None, True, id="running"
        ),
        pytest.param(
            "refused_run",
            "demo-cache-20261123154523",
            "FAILURE",
            "PIPELINE_STATE_FAILED",
            False,
            id="refused",
        ),
    ],
)
def test_pipeline_run_events(
    fixture: str, run_id: str, result: Any, native: Any, started: bool
) -> None:
    by_status = events(run_world([fixture]), pipeline_run(run_id))
    assert ("STARTED" in by_status) is started
    if result is None:
        assert "COMPLETE" not in by_status
        return
    complete = by_status["COMPLETE"]
    assert complete["result"] == {"type": result, "nativeResultType": native}
    assert ("durationMillis" in complete) is started


@pytest.mark.parametrize("kind", ["pipeline_run", "task_run", "job_record"])
def test_run_has_no_container(kind: str) -> None:
    if kind == "job_record":
        world = run_world(JOBS_WORLD)
        runs = job_records(world)
    else:
        world = run_world(["nested_run", "refused_run"])
        marker = {"pipeline_run": "pipeline_run", "task_run": "pipeline_task_run"}[kind]
        runs = [u for u in world if f"dataProcessInstance:my-project.{marker}." in u]
    assert runs
    assert not [u for u in runs if "container" in world[u]]


FLAKY_MAIN, FLAKY_FAILED, EMIT_MAIN = (
    "1000000000000000006",
    "1000000000000000007",
    "1000000000000000004",
)
BACKING_JOB_IDS = [
    FLAKY_MAIN,
    FLAKY_FAILED,
    EMIT_MAIN,
    *(
        "1000000000000000019",
        "1000000000000000017",
        "1000000000000000024",
    ),
]
JOBS_WORLD = ["cache_first_run", "cache_hit_run", "custom_jobs"]


def job_record(job_id: str) -> str:
    return f"urn:li:dataProcessInstance:my-project.job.{job_id}"


def job_records(world: WorldAspects) -> List[str]:
    return [u for u in world if u.startswith(job_record(""))]


def task_run_properties(world: WorldAspects, run_id: str) -> Dict[str, str]:
    (properties,) = world[run(run_id)]["dataProcessInstanceProperties"]
    return properties["customProperties"]


def extra_job(job_id: str, billing: str, task: str, **fields: Any) -> Dict[str, Any]:
    return {
        "create_time": "2026-11-23T10:30:00Z",
        "update_time": "2026-11-23T10:31:00Z",
        "start_time": "2026-11-23T10:30:10Z",
        "end_time": "2026-11-23T10:30:40Z",
        "state": 4,
        "display_name": f"caip_pipelines_{billing}_{task}",
        "labels": {"vertex-ai-pipelines-run-billing-id": billing},
        "name": f"projects/123456789012/locations/us-central1/customJobs/{job_id}",
        **fields,
    }


@pytest.mark.parametrize("check", ["not_a_training_job", "parent"])
def test_backing_job_record(check: str) -> None:
    world = run_world(JOBS_WORLD)
    aspects = world[job_record(FLAKY_MAIN)]
    if check == "not_a_training_job":
        assert "container" not in aspects
        return
    (relationships,) = aspects["dataProcessInstanceRelationships"]
    assert relationships["parentInstance"] == run(f"{CACHE1}_flaky")
    assert "parentTemplate" not in relationships


def test_cache_hit_task_run_names_no_job() -> None:
    properties = task_run_properties(run_world(JOBS_WORLD), f"{CACHE2}_emit")
    assert not [key for key in properties if "job" in key]


def test_cache_hit_job_links_to_earlier_run() -> None:
    world = run_world(JOBS_WORLD)
    (relationships,) = world[job_record(EMIT_MAIN)]["dataProcessInstanceRelationships"]
    assert relationships["parentInstance"] == run(f"{CACHE1}_emit")


@pytest.mark.parametrize(
    "case,count",
    [
        pytest.param("run_not_read", 3, id="run_not_read"),
        pytest.param("task_not_found", 1, id="task_not_found"),
    ],
)
def test_backing_job_skipped(case: str, count: int) -> None:
    extra = (
        [extra_job("1000000000000000998", "1000000000000000001", "1000000000000000999")]
        if case == "task_not_found"
        else []
    )
    world, report = run_world_with_report(JOBS_WORLD, extra_custom_jobs=extra)
    assert (
        report.backing_jobs_skipped_run_not_read
        if case == "run_not_read"
        else report.backing_jobs_skipped_task_not_found
    ) == count
    assert report.backing_jobs_linked == 3
    skipped = (
        ["1000000000000000019", "1000000000000000017", "1000000000000000024"]
        if case == "run_not_read"
        else ["1000000000000000998"]
    )
    for job_id in skipped:
        assert job_record(job_id) not in world
    (warning,) = skipped_backing_job_warnings(report)
    (context,) = warning.context
    assert f"{case}={count}" in context
    names = [
        job["display_name"]
        for job in [*listed_custom_jobs(), *extra]
        if job["name"].rsplit("/", 1)[-1] in skipped
    ]
    assert len(names) == len(skipped)
    assert all(name in context for name in names)


@pytest.mark.parametrize(
    "cap", [pytest.param(None, id="within_cap"), pytest.param(1, id="beyond_cap")]
)
def test_task_run_job_property(cap: Optional[int]) -> None:
    world = run_world(JOBS_WORLD, max_training_jobs_per_type=cap)
    properties = task_run_properties(world, f"{CACHE1}_flaky")
    assert properties["main_job"].endswith(f"/customJobs/{FLAKY_MAIN}")
    assert properties["main_job_record"] == job_record(FLAKY_MAIN)
    assert properties["failed_jobs"].endswith(f"/customJobs/{FLAKY_FAILED}")
    assert properties["failed_job_records"] == job_record(FLAKY_FAILED)


def listed_job(job_id: str) -> Dict[str, Any]:
    (data,) = [j for j in listed_custom_jobs() if j["name"].endswith(f"/{job_id}")]
    return data


def test_failed_attempt_job_record() -> None:
    world = run_world(JOBS_WORLD)
    aspects = world[job_record(FLAKY_FAILED)]
    (relationships,) = aspects["dataProcessInstanceRelationships"]
    assert relationships["parentInstance"] == run(f"{CACHE1}_flaky")
    complete = events(world, job_record(FLAKY_FAILED))["COMPLETE"]
    assert complete["result"]["type"] == "FAILURE"
    listed = listed_job(FLAKY_FAILED)
    assert complete["timestampMillis"] == millis(listed["create_time"])
    assert complete["durationMillis"] == millis(listed["end_time"]) - millis(
        listed["create_time"]
    )


@pytest.mark.parametrize(
    "removed,state",
    [
        pytest.param("start_time", 5, id="ended_without_start"),
        pytest.param("end_time", 5, id="no_end_time"),
        pytest.param("start_time", 7, id="cancelled_without_start"),
    ],
)
def test_job_record_run_event_times(removed: str, state: int) -> None:
    attempt = {
        key: value
        for key, value in extra_job(
            "1000000000000000995",
            "1000000000000000001",
            "1000000000000000005",
            state=state,
        ).items()
        if key != removed
    }
    world = run_world(JOBS_WORLD, extra_custom_jobs=[attempt])
    assert "dataProcessInstanceProperties" in world[job_record("1000000000000000995")]
    by_status = events(world, job_record("1000000000000000995"))
    # Like a Training Job, a job record has no run event without an end time, or
    # for a cancelled job.
    if removed == "end_time" or state == 7:
        assert not by_status
        return
    assert list(by_status) == ["COMPLETE"]
    complete = by_status["COMPLETE"]
    assert complete["timestampMillis"] == millis(attempt["end_time"])
    assert complete["result"]["type"] == "FAILURE"
    assert "durationMillis" not in complete


def test_training_job_run_event_is_stamped_at_create_time() -> None:
    automl: object = gen_mock_training_automl_job()
    assert isinstance(automl, MagicMock)
    automl.state = PipelineState.PIPELINE_STATE_SUCCEEDED
    automl.end_time = automl.create_time + timedelta(minutes=5)
    automl.to_dict.return_value = {}
    source = bare_source(
        use_ml_metadata_for_lineage=False, extract_execution_metrics=False
    )
    with patch(
        "datahub.ingestion.source.vertexai.vertexai_training_extractor.rate_limited_gapic_list",
        side_effect=lambda cls, *_a, **_k: (
            [automl] if cls.__name__ == "AutoMLTabularTrainingJob" else []
        ),
    ):
        (event,) = [
            workunit.metadata.aspect
            for workunit in source.training_extractor.get_workunits()
            if isinstance(workunit.metadata, MetadataChangeProposalWrapper)
            and isinstance(workunit.metadata.aspect, DataProcessInstanceRunEventClass)
        ]
    assert event.timestampMillis == datetime_to_ts_millis(automl.create_time)
    assert event.durationMillis == timedelta(minutes=5) // timedelta(milliseconds=1)


SECOND_FAILED = "1000000000000000990"


def cache_first_run_with_two_failed_attempts() -> Dict[str, Any]:
    """cache_first_run where `flaky` failed twice before its main job ran."""
    data = load_fixture("cache_first_run")
    flaky = next(d for d in _task_details(data) if d["taskName"] == "flaky")
    failed = flaky["executorDetail"]["containerDetail"]["failedMainJobs"]
    failed.insert(0, failed[0].replace(FLAKY_FAILED, SECOND_FAILED))
    return data


def test_task_run_with_two_failed_attempts() -> None:
    earlier = extra_job(
        SECOND_FAILED, "1000000000000000001", "1000000000000000005", state=5
    )
    world = run_world(
        [cache_first_run_with_two_failed_attempts(), "cache_hit_run", "custom_jobs"],
        extra_custom_jobs=[earlier],
    )
    flaky = run(f"{CACHE1}_flaky")
    assert events(world, flaky)["COMPLETE"]["attempt"] == 3
    properties = task_run_properties(world, f"{CACHE1}_flaky")
    failed = (SECOND_FAILED, FLAKY_FAILED)
    assert properties["failed_jobs"] == ",".join(
        f"projects/123456789012/locations/us-central1/customJobs/{i}" for i in failed
    )
    assert properties["failed_job_records"] == ",".join(map(job_record, failed))
    for job_id in failed:
        (relationships,) = world[job_record(job_id)]["dataProcessInstanceRelationships"]
        assert relationships["parentInstance"] == flaky


def test_job_record_reads_listed_state() -> None:
    world = run_world(JOBS_WORLD, custom_job_state_raises=True)
    assert sorted(job_records(world)) == sorted(
        job_record(i) for i in BACKING_JOB_IDS[:3]
    )
    assert (
        events(world, job_record(FLAKY_MAIN))["COMPLETE"]["result"]["type"] == "SUCCESS"
    )


def test_job_records_written_after_a_training_job_lookup_stops() -> None:
    training_job = extra_job(
        "1000000000000000994",
        "1000000000000000001",
        "1000000000000000005",
        display_name="hand-named-job",
    )
    with patch(
        "datahub.ingestion.source.vertexai.vertexai_training_extractor.get_job_result_status",
        side_effect=ServiceUnavailable("busy"),
    ):
        world, report = run_world_with_report(
            JOBS_WORLD, extra_custom_jobs=[training_job]
        )
    assert job_record("1000000000000000994") not in world
    assert report.backing_jobs_linked == 3
    assert sorted(job_records(world)) == sorted(
        job_record(i) for i in BACKING_JOB_IDS[:3]
    )


@pytest.mark.parametrize(
    "part",
    [
        "subtype",
        "training_properties",
        "run_event",
        "edges",
        "no_parent_template",
        "names_task_run",
    ],
)
def test_job_record_content(part: str) -> None:
    output_job = extra_job(
        "1000000000000000888",
        "1000000000000000001",
        "1000000000000000005",
        to_dict={
            "jobSpec": {
                "output": {"artifactOutputUri": "gs://bucket/training-output/model"}
            }
        },
    )
    world = run_world(JOBS_WORLD, extra_custom_jobs=[output_job])
    aspects = world[job_record(FLAKY_MAIN)]
    if part == "subtype":
        assert aspects["subTypes"][0]["typeNames"] == ["Training Job"]
    elif part == "training_properties":
        assert aspects["mlTrainingRunProperties"][0]["id"] == FLAKY_MAIN
    elif part == "run_event":
        complete = events(world, job_record(FLAKY_MAIN))["COMPLETE"]
        assert complete["result"]["type"] == "SUCCESS"
        listed = listed_job(FLAKY_MAIN)
        assert complete["timestampMillis"] == millis(listed["create_time"])
        assert complete["durationMillis"] == millis(listed["end_time"]) - millis(
            listed["create_time"]
        )
    elif part == "edges":
        today, _ = run_world_with_report(
            ["custom_jobs"], extra_custom_jobs=[output_job], include_pipelines=False
        )
        urn = job_record("1000000000000000888")
        outputs = world[urn]["dataProcessInstanceOutput"][0]["outputEdges"]
        assert outputs
        assert outputs == today[urn]["dataProcessInstanceOutput"][0]["outputEdges"]
    elif part == "no_parent_template":
        assert all(
            "parentTemplate" not in r
            for r in aspects["dataProcessInstanceRelationships"]
        )
    else:
        properties = aspects["dataProcessInstanceProperties"][0]["customProperties"]
        assert properties["pipeline_task_run"] == run(f"{CACHE1}_flaky")


def test_custom_job_training_jobs_get_no_run_event() -> None:
    world = run_world(["custom_jobs"], include_pipelines=False)
    assert len(job_records(world)) == 6
    for urn in job_records(world):
        assert "dataProcessInstanceRunEvent" not in world[urn]


def test_pipelines_off_keeps_training_jobs() -> None:
    world = run_world(["custom_jobs"], include_pipelines=False)
    assert len(job_records(world)) == 6
    for urn in job_records(world):
        assert "container" in world[urn]
        assert "parentInstance" not in str(
            world[urn].get("dataProcessInstanceRelationships")
        )


@pytest.mark.parametrize("case", ["counts_backing_jobs", "beyond_cap_no_record"])
def test_cap(case: str) -> None:
    world = run_world(JOBS_WORLD, max_training_jobs_per_type=2)
    if case == "counts_backing_jobs":
        assert sorted(job_records(world)) == sorted(
            [job_record(FLAKY_MAIN), job_record(FLAKY_FAILED)]
        )
    else:
        assert job_record(EMIT_MAIN) not in world


def bare_source(**config: Any) -> VertexAISource:
    source = VertexAISource(
        ctx=PipelineContext(run_id="backing-job-test"),
        config=VertexAIConfig.model_validate(
            {"project_id": "my-project", "region": "us-central1", **config}
        ),
    )
    source._current_project_id = "my-project"
    source._current_region = "us-central1"
    return source


@pytest.mark.parametrize(
    "case", ["urn", "earlier_ingestion", "experiment_link", "model_link"]
)
def test_job_record_urn(case: str) -> None:
    expected = f"urn:li:dataProcessInstance:my-project.job.{FLAKY_MAIN}"
    if case == "urn":
        assert expected in run_world(JOBS_WORLD)
    elif case == "earlier_ingestion":
        assert expected in run_world(["custom_jobs"], include_pipelines=False)
    elif case == "experiment_link":
        job = gen_custom_job_from_fixture(listed_custom_jobs()[0])
        source = bare_source()
        assert source.experiment_extractor._logged_training_job_urns([job]) == [
            expected
        ]
    else:
        job = gen_custom_job_from_fixture(listed_custom_jobs()[0])
        source = bare_source()
        source.training_extractor._track_listed_output_model(
            job,
            AutoMLJobConfig(
                modelToUpload=ModelToUpload(
                    name="projects/1/locations/us-central1/models/234", versionId="1"
                )
            ),
            None,
        )
        tracked = [
            urn
            for relationship in source.model_usage_tracker.relationships.values()
            for urn in relationship.training_jobs
        ]
        assert tracked == [expected]


@pytest.mark.parametrize("case", ["training_jobs_off", "pattern_excludes_custom_job"])
def test_no_job_records_without_listing(case: str) -> None:
    config: Dict[str, Any] = (
        {"include_training_jobs": False}
        if case == "training_jobs_off"
        else {"training_job_type_pattern": {"deny": ["CustomJob"]}}
    )
    world = run_world(JOBS_WORLD, **config)
    assert not job_records(world)
    assert "main_job" in task_run_properties(world, f"{CACHE1}_flaky")


def test_held_jobs_do_not_advance_checkpoint() -> None:
    source = bare_source()
    jobs = [gen_custom_job_from_fixture(data) for data in listed_custom_jobs()]
    # Only the flaky task (id ...05) of billing id ...01 is read: jobs 6 and 7 match.
    parents = {"1000000000000000001": {"1000000000000000005": run(f"{CACHE1}_flaky")}}
    handler = source.training_extractor.state_handler
    with (
        patch.object(handler, "get_last_update_time", return_value=1),
        patch.object(handler, "update_resource_timestamp") as update,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_training_extractor.rate_limited_gapic_list",
            side_effect=lambda cls, *_a, **_k: (
                jobs if cls.__name__ == "CustomJob" else []
            ),
        ),
    ):
        list(source.training_extractor.get_workunits())
        update.assert_not_called()
        list(
            source.training_extractor.gen_backing_job_records(
                parents, run_not_read=[], task_not_found=[]
            )
        )
    recorded = {call.args[1] for call in update.call_args_list}
    written = {
        int(job.update_time.timestamp() * 1000)
        for job in jobs
        if job.name in (FLAKY_MAIN, FLAKY_FAILED)
    }
    assert recorded == written


def test_backing_job_parents_do_not_outlive_the_region() -> None:
    left: List[Dict[str, Dict[str, str]]] = []

    def collect(source: Any) -> None:
        left.append(dict(source.pipeline_extractor.backing_job_parents))

    run_world(JOBS_WORLD, after_run=collect)
    assert left == [{}]


@pytest.mark.parametrize("path", ["transient_stop", "keep_previous_detail"])
def test_kept_job_is_neither_linked_nor_checkpointed(path: str) -> None:
    source = bare_source(use_ml_metadata_for_lineage=True)
    extractor = source.training_extractor
    jobs = [gen_custom_job_from_fixture(data) for data in listed_custom_jobs()]
    parents = {"1000000000000000001": {"1000000000000000005": run(f"{CACHE1}_flaky")}}
    keep = (
        patch.object(extractor._stopped, "any_transient", return_value=True)
        if path == "transient_stop"
        else patch.object(
            extractor,
            "_get_job_lineage_from_ml_metadata",
            return_value=DetailResult(value=None, keep_previous=True),
        )
    )
    with (
        patch.object(extractor.state_handler, "get_last_update_time", return_value=1),
        patch.object(extractor.state_handler, "update_resource_timestamp") as update,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_training_extractor.rate_limited_gapic_list",
            side_effect=lambda cls, *_a, **_k: (
                jobs if cls.__name__ == "CustomJob" else []
            ),
        ),
        keep,
    ):
        list(extractor.get_workunits())
        assert not list(
            extractor.gen_backing_job_records(
                parents, run_not_read=[], task_not_found=[]
            )
        )
    assert source.report.backing_jobs_linked == 0
    update.assert_not_called()


def test_held_jobs_are_joined_only_in_their_own_region() -> None:
    source = bare_source()
    extractor = source.training_extractor
    jobs = [gen_custom_job_from_fixture(data) for data in listed_custom_jobs()]
    parents = {"1000000000000000001": {"1000000000000000005": run(f"{CACHE1}_flaky")}}
    listings = iter([jobs, []])
    skipped: List[List[str]] = []
    with patch(
        "datahub.ingestion.source.vertexai.vertexai_training_extractor.rate_limited_gapic_list",
        side_effect=lambda cls, *_a, **_k: (
            next(listings) if cls.__name__ == "CustomJob" else []
        ),
    ):
        for _region in ("us-central1", "europe-west4"):
            run_not_read: List[str] = []
            task_not_found: List[str] = []
            list(extractor.get_workunits())
            list(
                extractor.gen_backing_job_records(
                    parents, run_not_read=run_not_read, task_not_found=task_not_found
                )
            )
            skipped.append(run_not_read + task_not_found)
    assert source.report.backing_jobs_linked == 2
    assert skipped[0]
    assert skipped[1] == []


def test_join_records_no_checkpoint_time_without_an_earlier_one() -> None:
    source = bare_source()
    jobs = [gen_custom_job_from_fixture(data) for data in listed_custom_jobs()]
    parents = {"1000000000000000001": {"1000000000000000005": run(f"{CACHE1}_flaky")}}
    handler = source.training_extractor.state_handler
    with (
        patch.object(handler, "get_last_update_time", return_value=None),
        patch.object(handler, "update_resource_timestamp") as update,
        patch(
            "datahub.ingestion.source.vertexai.vertexai_training_extractor.rate_limited_gapic_list",
            side_effect=lambda cls, *_a, **_k: (
                jobs if cls.__name__ == "CustomJob" else []
            ),
        ),
    ):
        list(source.training_extractor.get_workunits())
        list(
            source.training_extractor.gen_backing_job_records(
                parents, run_not_read=[], task_not_found=[]
            )
        )
    assert source.report.backing_jobs_linked == 2
    update.assert_not_called()


@pytest.mark.parametrize(
    "recorded", [True, False], ids=["keeps_listing_error", "records_when_none"]
)
def test_join_error_never_replaces_the_listing_error(recorded: bool) -> None:
    source = bare_source()
    listing_error, join_error = RuntimeError("listing"), RuntimeError("join")
    if recorded:
        source._ledger.record_read(
            "my-project", "us-central1", "training_jobs", error=listing_error
        )
    with patch.object(
        source.training_extractor, "gen_backing_job_records", side_effect=join_error
    ):
        assert not list(source._join_backing_jobs("my-project", "us-central1"))
    lost = source._ledger.lost_phase1_kinds("my-project", "us-central1")
    assert lost == {"training_jobs": listing_error if recorded else join_error}


@pytest.mark.parametrize("listing_failed", [True, False], ids=["warned", "not_warned"])
def test_join_error_after_a_failed_listing_is_warned(listing_failed: bool) -> None:
    source = bare_source()
    listing_error, join_error = RuntimeError("listing"), RuntimeError("join")
    if listing_failed:
        source._ledger.record_read(
            "my-project", "us-central1", "training_jobs", error=listing_error
        )
    with patch.object(
        source.training_extractor, "gen_backing_job_records", side_effect=join_error
    ):
        assert not list(source._join_backing_jobs("my-project", "us-central1"))
    warnings = [
        w for w in source.report.warnings if w.title == BACKING_JOBS_NOT_LINKED_TITLE
    ]
    if not listing_failed:
        assert not warnings
        return
    (warning,) = warnings
    assert warning.message == BACKING_JOBS_NOT_LINKED
    context = " ".join(warning.context)
    assert "project=my-project, region=us-central1" in context
    assert "RuntimeError" in context and "join" in context


def cache_first_run_with_failed_jobs_only() -> Dict[str, Any]:
    """cache_first_run where `flaky` names its failed attempt but no main job."""
    data = load_fixture("cache_first_run")
    flaky = next(d for d in _task_details(data) if d["taskName"] == "flaky")
    flaky["executorDetail"]["containerDetail"].pop("mainJob")
    return data


def nested_run_without_jobs() -> Dict[str, Any]:
    """nested_run with no step naming a backing job."""
    data = load_fixture("nested_run")
    for detail in _task_details(data):
        detail.pop("executorDetail", None)
    return data


# A read run is held by its billing id even when none of its tasks can name a job.
JOINED_PARENTS = {
    "1000000000000000001": {
        "-1000000000000000003": run(f"{CACHE1}_emit"),
        "1000000000000000005": run(f"{CACHE1}_flaky"),
    },
    "1000000000000000008": {},
}


@pytest.mark.parametrize(
    "fixtures,config,expected",
    [
        pytest.param(JOBS_WORLD, {}, JOINED_PARENTS, id="joined"),
        pytest.param(
            JOBS_WORLD, {"include_training_jobs": False}, {}, id="training_jobs_off"
        ),
        pytest.param(
            JOBS_WORLD,
            {"training_job_type_pattern": {"deny": ["CustomJob"]}},
            {},
            id="pattern_denies_custom_job",
        ),
        pytest.param(
            ["cache_hit_run"],
            {"include_training_jobs": True},
            {"1000000000000000008": {}},
            id="cache_hit",
        ),
        pytest.param(
            [nested_run_without_jobs()],
            {"include_training_jobs": True},
            {"1000000000000000026": {}},
            id="no_main_or_failed_job",
        ),
        pytest.param(
            [cache_first_run_with_failed_jobs_only(), "cache_hit_run", "custom_jobs"],
            {},
            JOINED_PARENTS,
            id="failed_jobs_only",
        ),
    ],
)
def test_backing_job_parents_hold_only_joinable_runs(
    fixtures: List[Fixture],
    config: Dict[str, Any],
    expected: Dict[str, Dict[str, str]],
) -> None:
    seen: List[Dict[str, Dict[str, str]]] = []

    def copy_before_clear(extractor: VertexAIPipelineExtractor) -> None:
        seen.append(
            {
                billing: dict(tasks)
                for billing, tasks in extractor.backing_job_parents.items()
            }
        )

    with patch.object(
        VertexAIPipelineExtractor,
        "clear_backing_job_parents",
        autospec=True,
        side_effect=copy_before_clear,
    ):
        run_world(fixtures, **config)
    assert seen == [expected]


def renamed(fixture: str, name: str) -> Dict[str, Any]:
    data = load_fixture(fixture)
    data["name"] = data["name"].rsplit("/", 1)[0] + "/" + name
    return data


@pytest.mark.parametrize(
    "fixture,counter,expected",
    [
        # 11 steps, of which exit-handler-1 and announce are top level.
        pytest.param("nested_run", "pipeline_nested_steps_written", 9, id="nested"),
        # 5 steps; for-loop-2.evaluate is the only one below the top level.
        pytest.param("loop_run", "pipeline_nested_steps_written", 1, id="loop_body"),
        # Details for-loop-2-worker-0 and -worker-1 are the two passes.
        pytest.param("loop_run", "pipeline_loop_passes", 2, id="loop_passes"),
        pytest.param("nested_run", "pipeline_loop_passes", 0, id="no_loop"),
        # emit-3 has no start_time; condition-1 has one but is NOT_TRIGGERED.
        pytest.param(
            "failure_run", "pipeline_task_runs_skipped_not_started", 2, id="not_started"
        ),
        pytest.param(
            "nested_run", "pipeline_task_runs_skipped_not_started", 0, id="all_started"
        ),
    ],
)
def test_pipeline_counters(fixture: str, counter: str, expected: int) -> None:
    _, report = run_world_with_report([fixture])
    assert getattr(report, counter) == expected


def test_counters_across_runs_of_one_pipeline() -> None:
    _, report = run_world_with_report(
        ["loop_run", renamed("loop_run", "loop-pipe-20261103171400")]
    )
    # The step's DataJob is written once, but each run reads its own two passes.
    assert report.pipeline_nested_steps_written == 1
    assert report.pipeline_loop_passes == 4


def nested_run_with_ghosts(count: int) -> Dict[str, Any]:
    data = load_fixture("nested_run")
    announce = next(d for d in _task_details(data) if d["taskName"] == "announce")
    for index in range(count):
        _task_details(data).append(
            {
                **announce,
                "taskId": str(1000000000000000100 + index),
                "taskName": f"ghost-{index:02d}",
            }
        )
    return data


def test_unmatched_details_context_is_a_sample() -> None:
    _, report = run_world_with_report([nested_run_with_ghosts(12)])
    assert report.pipeline_task_details_without_step == 12
    (warning,) = [w for w in report.warnings if w.title == UNMATCHED_DETAILS_TITLE]
    context = " ".join(warning.context)
    assert "unmatched=12" in context
    assert len(re.findall(r"ghost-\d\d", context)) == 10
    assert "ghost-09" in context
    assert "ghost-10" not in context


@pytest.mark.parametrize(
    "display_name",
    [
        pytest.param("hand-named-job", id="other_name"),
        pytest.param(
            "caip_pipelines_1000000000000000999_1000000000000000005",
            id="other_billing_id",
        ),
    ],
)
def test_labelled_job_without_backing_job_name_is_training_job(
    display_name: str,
) -> None:
    labelled = extra_job(
        "1000000000000000996",
        "1000000000000000001",
        "1000000000000000005",
        display_name=display_name,
    )
    world, report = run_world_with_report(JOBS_WORLD, extra_custom_jobs=[labelled])
    aspects = world[job_record("1000000000000000996")]
    assert aspects["subTypes"][0]["typeNames"] == ["Training Job"]
    assert "container" in aspects
    assert "parentInstance" not in str(aspects.get("dataProcessInstanceRelationships"))
    assert report.backing_jobs_skipped_task_not_found == 0
    assert report.backing_jobs_linked == 3


def test_job_of_a_run_with_lost_details_is_run_not_read() -> None:
    lost: DetailResult[List[PipelineTaskDetail]] = DetailResult(
        value=None, keep_previous=False
    )
    with patch.object(
        VertexAIPipelineExtractor, "_read_pipeline_task_details", return_value=lost
    ):
        world, report = run_world_with_report(JOBS_WORLD)
    assert report.backing_jobs_skipped_run_not_read == len(listed_custom_jobs())
    assert report.backing_jobs_linked == 0
    assert not job_records(world)


def test_jobs_of_a_lost_pipeline_list_get_no_record() -> None:
    lost = RetryError("retries exhausted", cause=ServiceUnavailable("busy"))
    with patch.object(VertexAIPipelineExtractor, "get_workunits", side_effect=lost):
        world, report = run_world_with_report(JOBS_WORLD)
    assert not job_records(world)
    assert report.backing_jobs_skipped_run_not_read == len(listed_custom_jobs())
    assert [w for w in report.warnings if w.message.endswith(PIPELINE_LIST_LOST_JOBS)]


def skipped_backing_job_warnings(report: VertexAISourceReport) -> List[Any]:
    return [w for w in report.warnings if w.title == BACKING_JOBS_SKIPPED_TITLE]


def test_skipped_backing_jobs_warned_once_per_region() -> None:
    unread = [
        extra_job("1000000000000000951", "1000000000000000961", "1000000000000000971"),
        extra_job("1000000000000000952", "1000000000000000962", "1000000000000000972"),
    ]
    world, report = run_world_with_report(
        ["cache_first_run"], include_training_jobs=True, extra_custom_jobs=unread
    )
    (warning,) = skipped_backing_job_warnings(report)
    (context,) = warning.context
    assert (
        "project=my-project, region=us-central1, run_not_read=2, task_not_found=0"
        in context
    )
    assert all(job["display_name"] in context for job in unread)
    assert report.backing_jobs_skipped_run_not_read == 2
    assert job_record("1000000000000000951") not in world
    assert job_record("1000000000000000952") not in world


def test_skipped_backing_jobs_not_warned_when_pipeline_list_lost() -> None:
    lost = RetryError("retries exhausted", cause=ServiceUnavailable("busy"))
    with patch.object(VertexAIPipelineExtractor, "get_workunits", side_effect=lost):
        _, report = run_world_with_report(JOBS_WORLD)
    assert report.backing_jobs_skipped_run_not_read == len(listed_custom_jobs())
    assert not skipped_backing_job_warnings(report)
    assert [w for w in report.warnings if w.message.endswith(PIPELINE_LIST_LOST_JOBS)]


def test_skipped_backing_jobs_warned_after_checkpoint_stop() -> None:
    # cache_first_run was last updated before this checkpoint, so the listing stops
    # at it and its jobs, like those of runs never listed, are skipped.
    checkpoint = millis("2026-11-24T00:00:00Z")
    with patch.object(
        VertexAIStateHandler,
        "get_last_update_time",
        side_effect=lambda resource_type: (
            checkpoint if resource_type == ResourceTypes.PIPELINE_JOB else None
        ),
    ):
        world, report = run_world_with_report(["cache_first_run", "custom_jobs"])
    assert not job_records(world)
    assert report.backing_jobs_skipped_run_not_read == len(listed_custom_jobs())
    (warning,) = skipped_backing_job_warnings(report)
    (context,) = warning.context
    assert f"run_not_read={len(listed_custom_jobs())}, task_not_found=0" in context
    assert listed_job(FLAKY_MAIN)["display_name"] in context


def test_skipped_backing_jobs_warning_names_at_most_ten() -> None:
    unread = [
        extra_job(
            f"10000000000000009{i:02d}",
            f"10000000000000008{i:02d}",
            "1000000000000000005",
        )
        for i in range(11)
    ]
    _, report = run_world_with_report(
        ["cache_first_run"], include_training_jobs=True, extra_custom_jobs=unread
    )
    (warning,) = skipped_backing_job_warnings(report)
    (context,) = warning.context
    assert "run_not_read=11, task_not_found=0" in context
    named = [job["display_name"] in context for job in unread]
    assert named == [True] * 10 + [False]


@pytest.mark.parametrize(
    "config,carried",
    [
        pytest.param({}, True, id="job_records_on"),
        # The model step keeps a read that does not fail, so the loss is reported
        # per read rather than as a project that could not be read.
        pytest.param(
            {"include_training_jobs": False, "include_models": True},
            False,
            id="training_jobs_off",
        ),
        pytest.param(
            {"training_job_type_pattern": {"deny": ["CustomJob"]}},
            False,
            id="custom_job_denied",
        ),
    ],
)
def test_refused_pipeline_list_names_held_jobs_only_with_job_records(
    config: Dict[str, Any], carried: bool
) -> None:
    refused = PermissionDenied(
        "no role", error_info=ErrorInfo(reason="IAM_PERMISSION_DENIED")
    )
    with (
        patch.object(VertexAIPipelineExtractor, "get_workunits", side_effect=refused),
        pytest.raises(PipelineExecutionError) as raised,
    ):
        run_world_with_report(JOBS_WORLD, **config)
    (failure,) = [
        f for f in raised.value.args[1] if "resource=pipelines" in " ".join(f.context)
    ]
    assert failure.message.endswith(PIPELINE_LIST_LOST_JOBS) is carried


def test_pipeline_read_stopped_partway_links_jobs_of_runs_read() -> None:
    original = VertexAIPipelineExtractor._get_pipeline_workunits
    calls: List[None] = []

    def fail_after_first_run(
        extractor: VertexAIPipelineExtractor, *args: Any, **kwargs: Any
    ) -> Iterator[Any]:
        calls.append(None)
        if len(calls) > 1:
            raise ValueError("error while writing a later run")
        yield from original(extractor, *args, **kwargs)

    with tempfile.TemporaryDirectory() as tmp_dir:
        sink_path = Path(tmp_dir) / "mcps.json"
        with (
            patch.object(
                VertexAIPipelineExtractor,
                "_get_pipeline_workunits",
                fail_after_first_run,
            ),
            pytest.raises(PipelineExecutionError) as raised,
        ):
            run_pipeline_fixtures(JOBS_WORLD, str(sink_path))
        urns = {mcp["entityUrn"] for mcp in json.loads(sink_path.read_text())}
    # cache_first_run is read first; cache_hit_run, whose run names no job, fails.
    assert sorted(u for u in urns if u.startswith(job_record(""))) == sorted(
        job_record(i) for i in BACKING_JOB_IDS[:3]
    )
    (failure,) = [
        f for f in raised.value.args[1] if "resource=pipelines" in " ".join(f.context)
    ]
    assert failure.message.endswith(PIPELINE_LIST_LOST_JOBS)


def test_job_of_a_read_run_without_joinable_tasks_is_task_not_found() -> None:
    # Every task of cache_hit_run is a cache hit, so none can name a job of its own.
    extra = [
        extra_job("1000000000000000997", "1000000000000000008", "1000000000000000011")
    ]
    _, report = run_world_with_report(JOBS_WORLD, extra_custom_jobs=extra)
    assert report.backing_jobs_skipped_task_not_found == 1
    assert report.backing_jobs_skipped_run_not_read == 3
    (warning,) = skipped_backing_job_warnings(report)
    (context,) = warning.context
    assert "run_not_read=3, task_not_found=1" in context
    assert extra[0]["display_name"] in context


PIPELINE_RECORDINGS = sorted(
    path.stem for path in PIPELINE_TASKS_FIXTURE_DIR.glob("*_run.json")
)


@pytest.mark.parametrize("recording", PIPELINE_RECORDINGS)
def test_no_recording_has_a_job_whose_task_is_not_found(recording: str) -> None:
    _, report = run_world_with_report([recording, "custom_jobs"])
    assert report.backing_jobs_skipped_task_not_found == 0
    for warning in skipped_backing_job_warnings(report):
        assert "task_not_found=0" in " ".join(warning.context)


def test_full_world_skips_no_backing_job() -> None:
    assert PIPELINE_RECORDINGS
    _, report = run_world_with_report([*PIPELINE_RECORDINGS, "custom_jobs"])
    assert report.backing_jobs_skipped_task_not_found == 0
    assert not skipped_backing_job_warnings(report)


def test_job_whose_task_is_not_found_alone_is_warned() -> None:
    orphan = extra_job(
        "1000000000000000993", "1000000000000000001", "1000000000000000999"
    )
    _, report = run_world_with_report(
        [*PIPELINE_RECORDINGS, "custom_jobs"], extra_custom_jobs=[orphan]
    )
    (warning,) = skipped_backing_job_warnings(report)
    (context,) = warning.context
    assert "run_not_read=0, task_not_found=1" in context
    assert orphan["display_name"] in context


def test_job_whose_task_is_not_found_is_named_before_unread_runs() -> None:
    unread = [
        extra_job(
            f"10000000000000009{i:02d}",
            f"10000000000000008{i:02d}",
            "1000000000000000005",
        )
        for i in range(11)
    ]
    orphan = extra_job(
        "1000000000000000993", "1000000000000000001", "1000000000000000999"
    )
    _, report = run_world_with_report(
        ["cache_first_run"],
        include_training_jobs=True,
        extra_custom_jobs=[*unread, orphan],
    )
    (warning,) = skipped_backing_job_warnings(report)
    (context,) = warning.context
    assert "run_not_read=11, task_not_found=1" in context
    assert orphan["display_name"] in context


def test_job_whose_task_is_not_found_is_warned_when_pipeline_list_lost() -> None:
    original = VertexAIPipelineExtractor._get_pipeline_workunits
    calls: List[None] = []

    def fail_after_first_run(
        extractor: VertexAIPipelineExtractor, *args: Any, **kwargs: Any
    ) -> Iterator[Any]:
        calls.append(None)
        if len(calls) > 1:
            raise RetryError("retries exhausted", cause=ServiceUnavailable("busy"))
        yield from original(extractor, *args, **kwargs)

    orphan = extra_job(
        "1000000000000000993", "1000000000000000001", "1000000000000000999"
    )
    with patch.object(
        VertexAIPipelineExtractor, "_get_pipeline_workunits", fail_after_first_run
    ):
        _, report = run_world_with_report(JOBS_WORLD, extra_custom_jobs=[orphan])
    assert report.backing_jobs_skipped_run_not_read == 3
    (warning,) = skipped_backing_job_warnings(report)
    (context,) = warning.context
    assert "task_not_found=1" in context
    assert "run_not_read" not in context
    assert orphan["display_name"] in context


def loop_run_copies(count: int = 3) -> List[Dict[str, Any]]:
    """Copies of loop_run under other run names, which its artifact URIs also hold."""
    text = (PIPELINE_TASKS_FIXTURE_DIR / "loop_run.json").read_text()
    return [
        json.loads(text.replace(LOOP, f"loop-pipe-2026110317{index:04d}"))
        for index in range(count)
    ]


def lineage_patches(world: WorldAspects) -> Dict[str, List[Any]]:
    """Task URN -> its `dataJobInputOutput` patches, for loop-pipe tasks that have any."""
    return {
        urn: world[urn]["dataJobInputOutput"]
        for urn in datajob_urns(world, "loop-pipe")
        if "dataJobInputOutput" in world[urn]
    }


def linked_paths(patches: Dict[str, List[Any]]) -> Dict[str, Set[str]]:
    return {
        urn: {op["path"] for task_patch in task_patches for op in task_patch}
        for urn, task_patches in patches.items()
    }


def union_of_single_runs(copies: Sequence[Fixture]) -> Dict[str, Set[str]]:
    union: Dict[str, Set[str]] = defaultdict(set)
    for copy in copies:
        for urn, paths in linked_paths(lineage_patches(run_world([copy]))).items():
            union[urn] |= paths
    return dict(union)


def loop_run_with_distinct_passes() -> Dict[str, Any]:
    """loop_run where each pass of `evaluate` reads and writes its own artifacts."""
    data = load_fixture("loop_run")
    passes = [d for d in _task_details(data) if d["taskName"] == "evaluate"]
    for index, evaluation in enumerate(passes):
        evaluation["inputs"] = {
            "model": {
                "artifacts": [{"uri": f"gs://demo-loop-bucket/pass-{index}/model"}]
            }
        }
        evaluation["outputs"] = {
            "metrics": {
                "artifacts": [{"uri": f"gs://demo-loop-bucket/pass-{index}/metrics"}]
            }
        }
    return data


def test_loop_step_lineage_holds_every_pass() -> None:
    world = run_world([loop_run_with_distinct_passes()])
    evaluate = job("loop-pipe", "for-loop-2.evaluate")
    for edges, artifact in (
        ("inputDatasetEdges", "model"),
        ("outputDatasetEdges", "metrics"),
    ):
        assert sorted(datajob_links(world, evaluate, edges)) == [
            f"urn:li:dataset:(urn:li:dataPlatform:gcs,demo-loop-bucket/pass-{index}/{artifact},PROD)"
            for index in (0, 1)
        ]


def test_task_lineage_grouped_per_task() -> None:
    patches = lineage_patches(run_world(loop_run_copies()))
    assert linked_paths(patches) == union_of_single_runs(loop_run_copies())
    # Each run's model output is a different dataset, so the union holds three.
    assert len(linked_paths(patches)[job("loop-pipe", "train")]) == 3
    for urn, task_patches in patches.items():
        (ops,) = task_patches
        assert len(ops) == len({op["path"] for op in ops}), urn


def assert_lineage_sent(
    world: WorldAspects, report: VertexAISourceReport, patches: int, links: int
) -> None:
    """The lineage patch and link counters hold the given values, which are the patches and links in `world`."""
    written = lineage_patches(world)
    assert (
        report.pipeline_lineage_patches_sent
        == patches
        == sum(map(len, written.values()))
    )
    assert (
        report.pipeline_lineage_links_sent
        == links
        == sum(len(ops) for task_patches in written.values() for ops in task_patches)
    )


def test_task_lineage_flushes_at_threshold() -> None:
    unpatched_world, unpatched_report = run_world_with_report(loop_run_copies())
    # Four tasks get links: 11 links, one patch per task.
    assert_lineage_sent(unpatched_world, unpatched_report, patches=4, links=11)
    unpatched = lineage_patches(unpatched_world)
    with patch(f"{EXTRACTOR}.LINEAGE_FLUSH_PENDING_LINKS", 2):
        world, report = run_world_with_report(loop_run_copies())
    # A flush after each run: a link repeated across runs is sent again.
    assert_lineage_sent(world, report, patches=12, links=15)
    patched = lineage_patches(world)
    assert linked_paths(patched) == linked_paths(unpatched)
    # A flush empties the buffer, so a later flush does not send the link again.
    train_outputs = datajob_links(
        world, job("loop-pipe", "train"), "outputDatasetEdges"
    )
    assert len(train_outputs) == len(set(train_outputs)) == 3
    assert any(len(task_patches) > 1 for task_patches in patched.values())
    assert sum(map(len, patched.values())) > sum(map(len, unpatched.values()))


def test_repeated_links_do_not_count_toward_the_flush() -> None:
    one_run = lineage_patches(run_world(["loop_run"]))
    links_of_one_run = sum(len(ops) for patches in one_run.values() for ops in patches)
    # Counted once, the links of three identical runs never reach the threshold.
    with patch(f"{EXTRACTOR}.LINEAGE_FLUSH_PENDING_LINKS", links_of_one_run + 1):
        patched = lineage_patches(run_world(["loop_run"] * 3))
    assert patched
    assert all(len(task_patches) == 1 for task_patches in patched.values())


def test_task_lineage_workunit_ids_are_unique() -> None:
    jobs = [gen_pipeline_job_from_fixture(data) for data in loop_run_copies()]
    source = bare_source()
    with (
        patch(f"{EXTRACTOR}.LINEAGE_MAX_LINKS_PER_PATCH", 1),
        patch(f"{EXTRACTOR}.LINEAGE_FLUSH_PENDING_LINKS", 2),
        patch(f"{EXTRACTOR}.rate_limited_gapic_list", return_value=jobs),
    ):
        ids = [
            workunit.id
            for workunit in source.pipeline_extractor.get_workunits()
            if isinstance(workunit.metadata, MetadataChangeProposalClass)
            and workunit.metadata.aspectName == "dataJobInputOutput"
        ]
    assert len(ids) > 1
    assert len(ids) == len(set(ids))


def test_task_lineage_cap_splits_patch() -> None:
    unpatched_world, unpatched_report = run_world_with_report(loop_run_copies())
    assert_lineage_sent(unpatched_world, unpatched_report, patches=4, links=11)
    unpatched = lineage_patches(unpatched_world)
    with patch(f"{EXTRACTOR}.LINEAGE_MAX_LINKS_PER_PATCH", 1):
        world, report = run_world_with_report(loop_run_copies())
    assert_lineage_sent(world, report, patches=11, links=11)
    patched = lineage_patches(world)
    assert linked_paths(patched) == linked_paths(unpatched)
    assert all(
        len(ops) <= 1 for task_patches in patched.values() for ops in task_patches
    )
    assert sum(map(len, patched.values())) > sum(map(len, unpatched.values()))


def read_region_pipelines(
    source: VertexAISource, listing: Callable[..., Iterator[Any]]
) -> Dict[str, Set[str]]:
    """Read the region's pipelines through the phase-1 guard; return task URN -> link paths."""
    with patch(f"{EXTRACTOR}.rate_limited_gapic_list", side_effect=listing):
        workunits = list(
            source._read_phase1_resource(
                ResourceReads.PIPELINES, "my-project", "us-central1"
            )
        )
    links: Dict[str, Set[str]] = defaultdict(set)
    for workunit in workunits:
        mcp = workunit.metadata
        if (
            isinstance(mcp, MetadataChangeProposalClass)
            and mcp.aspectName == "dataJobInputOutput"
        ):
            assert mcp.aspect is not None
            links[workunit.get_urn()] |= {
                op["path"] for op in json.loads(mcp.aspect.value)
            }
    return dict(links)


def test_task_lineage_flushed_when_region_read_fails() -> None:
    (first,) = loop_run_copies(1)
    error = RuntimeError("listing")

    def listing(*_args: Any, **_kwargs: Any) -> Iterator[Any]:
        yield gen_pipeline_job_from_fixture(first)
        raise error

    source = bare_source()
    links = read_region_pipelines(source, listing)
    assert links == union_of_single_runs([first])
    lost = source._ledger.lost_phase1_kinds("my-project", "us-central1")
    assert lost == {ResourceReads.PIPELINES: error}


def test_task_lineage_flushed_when_checkpoint_stops_listing() -> None:
    first, second = loop_run_copies(2)
    second["updateTime"] = "2026-11-01T00:00:00Z"
    source = bare_source()
    handler = source.pipeline_extractor.state_handler
    checkpoint = millis("2026-11-02T00:00:00Z")
    jobs = [gen_pipeline_job_from_fixture(data) for data in (first, second)]
    with patch.object(handler, "get_last_update_time", return_value=checkpoint):
        links = read_region_pipelines(source, lambda *_a, **_k: iter(jobs))
    assert links
    assert links == union_of_single_runs([first])


def test_task_lineage_skips_runs_with_lost_details() -> None:
    lost: DetailResult[List[PipelineTaskDetail]] = DetailResult(
        value=None, keep_previous=False
    )
    with patch.object(
        VertexAIPipelineExtractor, "_read_pipeline_task_details", return_value=lost
    ):
        world = run_world(["loop_run"])
    assert datajob_urns(world, "loop-pipe")
    assert not lineage_patches(world)
