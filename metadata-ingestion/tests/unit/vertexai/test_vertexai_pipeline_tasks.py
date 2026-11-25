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
from unittest.mock import patch

import pytest
from google.cloud.aiplatform_v1.types import (
    Execution,
    PipelineTaskDetail,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai.vertexai import VertexAISource
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    EXTRA_ROOT_DETAILS_TITLE,
    UNMATCHED_DETAILS_TITLE,
    ResourceReads,
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
from datahub.metadata.schema_classes import MetadataChangeProposalClass
from tests.integration.vertexai.mock_vertexai import (
    PIPELINE_TASKS_FIXTURE_DIR,
    gen_pipeline_job_from_fixture,
    run_pipeline_fixtures,
)

NESTED = "nested-pipe-20261104161316"
LOOP = "loop-pipe-20261103171359"
CACHE1 = "demo-cache-20261123155235"
FAIL = "demo-failure-20261123155239"
CANCEL = "demo-cancel-20261123155241"
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


@pytest.mark.parametrize("kind", ["pipeline_run", "task_run"])
def test_run_has_no_container(kind: str) -> None:
    world = run_world(["nested_run", "refused_run"])
    marker = {"pipeline_run": "pipeline_run", "task_run": "pipeline_task_run"}[kind]
    runs = [u for u in world if f"dataProcessInstance:my-project.{marker}." in u]
    assert runs
    assert not [u for u in runs if "container" in world[u]]


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
