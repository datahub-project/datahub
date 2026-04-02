import uuid
from datetime import datetime, timezone
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest
from langsmith.schemas import (
    Dataset as LangSmithDataset,
    Run as LangSmithRun,
    TracerSession as LangSmithProject,
)
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.langsmith import LangSmithConfig, LangSmithSource
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    ContainerClass,
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    MLTrainingRunPropertiesClass,
    SubTypesClass,
)

PROJECT_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
RUN_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
DATASET_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")
TENANT_ID = uuid.UUID("44444444-4444-4444-4444-444444444444")

START_TIME = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
END_TIME = datetime(2024, 1, 1, 10, 0, 5, tzinfo=timezone.utc)  # 5 seconds later


def _make_config(**kwargs) -> LangSmithConfig:
    defaults = {"api_key": "test-api-key"}
    defaults.update(kwargs)
    return LangSmithConfig(**defaults)


def _make_source(config: LangSmithConfig = None) -> LangSmithSource:
    if config is None:
        config = _make_config()
    with patch("langsmith.Client"):
        source = LangSmithSource(
            ctx=PipelineContext(run_id="langsmith-test"),
            config=config,
        )
    return source


def _make_project(
    name: str = "my-project", description: str = None
) -> LangSmithProject:
    return LangSmithProject(
        id=PROJECT_ID,
        name=name,
        description=description,
        start_time=START_TIME,
        tenant_id=TENANT_ID,
        reference_dataset_id=None,
    )


def _make_run(
    name: str = "test-chain",
    run_type: str = "chain",
    status: str = "success",
    tags: List[str] = None,
    prompt_tokens: int = None,
    completion_tokens: int = None,
    total_tokens: int = None,
    feedback_stats: dict = None,
    app_path: str = None,
) -> LangSmithRun:
    return LangSmithRun(
        id=RUN_ID,
        name=name,
        run_type=run_type,
        status=status,
        start_time=START_TIME,
        end_time=END_TIME,
        tags=tags or [],
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        feedback_stats=feedback_stats,
        app_path=app_path,
        inputs={},
        outputs={},
    )


def _make_child_run(
    parent_run_id: uuid.UUID,
    run_id: uuid.UUID = None,
    name: str = "llm-call",
    run_type: str = "llm",
    status: str = "success",
    prompt_tokens: Optional[int] = None,
    completion_tokens: Optional[int] = None,
    total_tokens: Optional[int] = None,
) -> LangSmithRun:
    if run_id is None:
        run_id = uuid.uuid4()
    return LangSmithRun(
        id=run_id,
        name=name,
        run_type=run_type,
        status=status,
        start_time=START_TIME,
        end_time=END_TIME,
        tags=[],
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        feedback_stats=None,
        app_path=None,
        inputs={},
        outputs={},
        parent_run_id=parent_run_id,
    )


def _make_dataset(
    name: str = "eval-dataset", description: str = "Test eval"
) -> LangSmithDataset:
    return LangSmithDataset(
        id=DATASET_ID,
        name=name,
        description=description,
        created_at=START_TIME,
        data_type="kv",
    )


# -------------------------------------------------------------------------
# Config tests
# -------------------------------------------------------------------------


def test_config_requires_api_key():
    with pytest.raises(ValidationError):
        LangSmithConfig()


def test_config_defaults():
    config = _make_config()
    assert config.api_url == "https://api.smith.langchain.com"
    assert config.include_datasets is True
    assert config.include_feedback_on_traces is True
    assert config.max_traces_per_project == 1000


# -------------------------------------------------------------------------
# _convert_run_status tests
# -------------------------------------------------------------------------


def test_convert_run_status_success():
    source = _make_source()
    assert source._convert_run_status("success") == "SUCCESS"


def test_convert_run_status_error():
    source = _make_source()
    assert source._convert_run_status("error") == "FAILURE"


def test_convert_run_status_unknown():
    source = _make_source()
    assert source._convert_run_status("pending") == "SKIPPED"
    assert source._convert_run_status(None) == "SKIPPED"


# -------------------------------------------------------------------------
# _get_token_metrics tests
# -------------------------------------------------------------------------


def test_get_token_metrics_all_present():
    source = _make_source()
    run = _make_run(prompt_tokens=100, completion_tokens=50, total_tokens=150)
    metrics = source._get_token_metrics(run)
    names = {m.name: m.value for m in metrics}
    assert names == {
        "prompt_tokens": "100",
        "completion_tokens": "50",
        "total_tokens": "150",
    }


def test_get_token_metrics_partial():
    source = _make_source()
    run = _make_run(total_tokens=75)
    metrics = source._get_token_metrics(run)
    names = [m.name for m in metrics]
    assert names == ["total_tokens"]


def test_get_token_metrics_none():
    source = _make_source()
    run = _make_run()
    metrics = source._get_token_metrics(run)
    assert metrics == []


# -------------------------------------------------------------------------
# _make_trace_url tests
# -------------------------------------------------------------------------


def test_make_trace_url_with_app_path():
    source = _make_source()
    project = _make_project()
    run = _make_run(app_path="/o/abc123/projects/p/proj1/r/run1")
    url = source._make_trace_url(project, run)
    assert url == "https://smith.langchain.com/o/abc123/projects/p/proj1/r/run1"


def test_make_trace_url_without_app_path():
    source = _make_source()
    project = _make_project()
    run = _make_run()
    url = source._make_trace_url(project, run)
    assert url is None


# -------------------------------------------------------------------------
# _emit_project_container tests
# -------------------------------------------------------------------------


def test_emit_project_container_produces_workunits():
    source = _make_source()
    project = _make_project(name="test-project", description="A test project")
    wus = list(source._emit_project_container(project))
    assert len(wus) > 0
    # Container URNs use a hash of the key - verify by checking the entity URN
    urns = {wu.metadata.entityUrn for wu in wus}
    # All workunits should reference the same container URN
    assert len(urns) == 1
    assert list(urns)[0].startswith("urn:li:container:")


def test_emit_project_container_subtype():
    source = _make_source()
    project = _make_project()
    wus = list(source._emit_project_container(project))
    subtypes = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, SubTypesClass)
    ]
    assert any("LangSmith Project" in st.typeNames for st in subtypes)


# -------------------------------------------------------------------------
# _emit_trace tests
# -------------------------------------------------------------------------


def test_emit_trace_basic_aspects():
    source = _make_source()
    project = _make_project()
    run = _make_run()
    wus = list(source._emit_run(project, run))

    aspect_types = [type(wu.metadata.aspect) for wu in wus]
    assert DataProcessInstancePropertiesClass in aspect_types
    assert ContainerClass in aspect_types
    assert SubTypesClass in aspect_types


def test_emit_trace_subtype():
    source = _make_source()
    project = _make_project()
    run = _make_run()
    wus = list(source._emit_run(project, run))
    subtypes = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, SubTypesClass)
    ]
    assert any("LLM Trace" in st.typeNames for st in subtypes)


def test_emit_trace_token_metrics():
    source = _make_source()
    project = _make_project()
    run = _make_run(prompt_tokens=100, completion_tokens=50, total_tokens=150)
    wus = list(source._emit_run(project, run))
    ml_runs = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, MLTrainingRunPropertiesClass)
    ]
    assert len(ml_runs) == 1
    metric_names = {m.name for m in ml_runs[0].trainingMetrics}
    assert metric_names == {"prompt_tokens", "completion_tokens", "total_tokens"}


def test_emit_trace_no_token_metrics_when_absent():
    source = _make_source()
    project = _make_project()
    run = _make_run()  # no token counts
    wus = list(source._emit_run(project, run))
    ml_runs = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, MLTrainingRunPropertiesClass)
    ]
    assert len(ml_runs) == 0


def test_emit_trace_run_event():
    source = _make_source()
    project = _make_project()
    run = _make_run(status="success")
    wus = list(source._emit_run(project, run))
    run_events = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceRunEventClass)
    ]
    assert len(run_events) == 1
    assert run_events[0].result.type == "SUCCESS"
    assert run_events[0].durationMillis == 5000  # 5 seconds


def test_emit_trace_tags():
    source = _make_source()
    project = _make_project()
    run = _make_run(tags=["production", "gpt-4o"])
    wus = list(source._emit_run(project, run))
    props = next(
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstancePropertiesClass)
    )
    tags_value = props.customProperties.get("tags", "")
    assert "production" in tags_value
    assert "gpt-4o" in tags_value


def test_emit_trace_feedback_as_custom_props():
    source = _make_source()
    project = _make_project()
    run = _make_run(feedback_stats={"correctness": {"avg": 0.85, "n": 10}})
    wus = list(source._emit_run(project, run))
    props = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstancePropertiesClass)
    ]
    assert len(props) == 1
    assert "feedback.correctness.avg" in props[0].customProperties
    assert props[0].customProperties["feedback.correctness.avg"] == "0.85"


# -------------------------------------------------------------------------
# _emit_dataset tests
# -------------------------------------------------------------------------


def test_emit_dataset_produces_workunits():
    source = _make_source()
    ls_dataset = _make_dataset()
    wus = list(source._emit_dataset(ls_dataset))
    assert len(wus) > 0


def test_emit_dataset_urn_contains_platform():
    source = _make_source()
    ls_dataset = _make_dataset(name="my-eval")
    wus = list(source._emit_dataset(ls_dataset))
    assert any("langsmith" in wu.id for wu in wus)
    assert any("my-eval" in wu.id for wu in wus)


# -------------------------------------------------------------------------
# Project pattern filtering
# -------------------------------------------------------------------------


def test_project_pattern_filters_projects():
    from datahub.configuration.common import AllowDenyPattern

    config = _make_config(project_pattern=AllowDenyPattern(deny=["dev-.*"]))
    source = _make_source(config)

    projects = [
        _make_project(name="prod-chatbot"),
        _make_project(name="dev-experiment"),
    ]
    source.client.list_projects = MagicMock(return_value=iter(projects))
    source.client.list_runs = MagicMock(return_value=iter([]))

    wus = list(source._get_project_workunits())

    assert source.report.projects_scanned == 2
    assert source.report.projects_filtered == 1
    # prod-chatbot should produce workunits; dev-experiment should be filtered
    aspect_names = [
        wu.metadata.aspect.name for wu in wus if hasattr(wu.metadata.aspect, "name")
    ]
    assert any("prod-chatbot" in n for n in aspect_names)


# -------------------------------------------------------------------------
# max_traces_per_project limit
# -------------------------------------------------------------------------


def test_max_traces_per_project_limits_ingestion():
    config = _make_config(max_traces_per_project=2)
    source = _make_source(config)

    project = _make_project()
    runs = [_make_run(name=f"run-{i}") for i in range(5)]
    # Each run needs a unique ID
    for i, r in enumerate(runs):
        object.__setattr__(r, "id", uuid.UUID(f"aaaaaaaa-aaaa-aaaa-aaaa-{i:012d}"))

    source.client.list_runs = MagicMock(return_value=iter(runs))

    list(source._get_trace_workunits(project))

    assert source.report.traces_ingested == 2
    assert source.report.traces_skipped_limit == 3


# -------------------------------------------------------------------------
# include_child_spans config
# -------------------------------------------------------------------------


def test_config_include_child_spans_default_false():
    config = _make_config()
    assert config.include_child_spans is False


# -------------------------------------------------------------------------
# _emit_run as child span
# -------------------------------------------------------------------------

CHILD_RUN_ID = uuid.UUID("55555555-5555-5555-5555-555555555555")
PARENT_RUN_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")  # == RUN_ID


def test_emit_run_root_trace_no_relationships():
    """Root traces (no parent_dpi_urn) must not emit a relationships aspect."""
    source = _make_source()
    project = _make_project()
    run = _make_run()
    wus = list(source._emit_run(project, run))
    rel_aspects = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceRelationshipsClass)
    ]
    assert rel_aspects == []


def test_emit_run_as_child_span_has_relationships():
    """Child spans must emit DataProcessInstanceRelationshipsClass with correct parentInstance."""
    source = _make_source()
    project = _make_project()
    child_run = _make_child_run(parent_run_id=PARENT_RUN_ID, run_id=CHILD_RUN_ID)
    parent_dpi_urn = source._make_dpi_urn(PARENT_RUN_ID)
    wus = list(source._emit_run(project, child_run, parent_dpi_urn=parent_dpi_urn))
    rel_aspects = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceRelationshipsClass)
    ]
    assert len(rel_aspects) == 1
    assert rel_aspects[0].parentInstance == parent_dpi_urn
    assert rel_aspects[0].upstreamInstances == [parent_dpi_urn]


def test_emit_run_as_child_span_subtype():
    """Child spans must carry the 'LLM Span' subtype."""
    from datahub.ingestion.source.common.subtypes import MLAssetSubTypes

    source = _make_source()
    project = _make_project()
    child_run = _make_child_run(parent_run_id=PARENT_RUN_ID, run_id=CHILD_RUN_ID)
    parent_dpi_urn = source._make_dpi_urn(PARENT_RUN_ID)
    wus = list(
        source._emit_run(
            project,
            child_run,
            subtype=MLAssetSubTypes.LANGSMITH_SPAN,
            parent_dpi_urn=parent_dpi_urn,
        )
    )
    subtypes = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, SubTypesClass)
    ]
    assert len(subtypes) == 1
    assert MLAssetSubTypes.LANGSMITH_SPAN in subtypes[0].typeNames


# -------------------------------------------------------------------------
# _get_child_span_workunits
# -------------------------------------------------------------------------


def test_get_child_span_workunits_links_to_parent():
    """Child spans must have parentInstance pointing to the root trace DPI."""
    source = _make_source()
    project = _make_project()
    root_run = _make_run()  # id = RUN_ID

    # Two child runs: one direct child, one nested under a sibling
    llm_run_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
    retriever_run_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002")
    llm_child = _make_child_run(parent_run_id=RUN_ID, run_id=llm_run_id, run_type="llm")
    retriever_child = _make_child_run(
        parent_run_id=RUN_ID, run_id=retriever_run_id, run_type="retriever"
    )

    source.client.list_runs = MagicMock(return_value=iter([llm_child, retriever_child]))

    wus = list(source._get_child_span_workunits(project, root_run, {}))

    # Two spans => two relationships aspects
    rel_aspects = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceRelationshipsClass)
    ]
    assert len(rel_aspects) == 2
    root_dpi_urn = source._make_dpi_urn(RUN_ID)
    for rel in rel_aspects:
        assert rel.parentInstance == root_dpi_urn

    assert source.report.spans_ingested == 2

    # Verify list_runs was called with trace_id and is_root=False
    source.client.list_runs.assert_called_once_with(trace_id=root_run.id, is_root=False)


def test_get_child_span_workunits_nested_parent():
    """A grandchild run's parentInstance must point to the intermediate child, not the root."""
    source = _make_source()
    project = _make_project()
    root_run = _make_run()  # id = RUN_ID

    tool_run_id = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000001")
    llm_under_tool_id = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002")
    # tool_run is a direct child of root; llm_under_tool is a child of tool_run
    tool_run = _make_child_run(
        parent_run_id=RUN_ID, run_id=tool_run_id, run_type="tool"
    )
    nested_llm = _make_child_run(
        parent_run_id=tool_run_id, run_id=llm_under_tool_id, run_type="llm"
    )

    source.client.list_runs = MagicMock(return_value=iter([tool_run, nested_llm]))

    wus = list(source._get_child_span_workunits(project, root_run, {}))

    rel_aspects = {
        wu.metadata.entityUrn: wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceRelationshipsClass)
    }
    tool_urn = source._make_dpi_urn(tool_run_id)
    nested_urn = source._make_dpi_urn(llm_under_tool_id)
    root_dpi_urn = source._make_dpi_urn(RUN_ID)

    assert rel_aspects[tool_urn].parentInstance == root_dpi_urn
    assert rel_aspects[nested_urn].parentInstance == source._make_dpi_urn(tool_run_id)


def test_child_spans_not_fetched_when_disabled():
    """When include_child_spans=False, list_runs must only be called once (for root runs)."""
    config = _make_config(include_child_spans=False)
    source = _make_source(config)
    project = _make_project()
    root_run = _make_run()
    source.client.list_runs = MagicMock(return_value=iter([root_run]))

    list(source._get_trace_workunits(project))

    # list_runs called exactly once: for root runs
    assert source.client.list_runs.call_count == 1
    assert source.report.spans_ingested == 0


# -------------------------------------------------------------------------
# _emit_retriever_input_edge
# -------------------------------------------------------------------------

RETRIEVER_RUN_ID = uuid.UUID("66666666-6666-6666-6666-666666666666")
LLM_RUN_ID = uuid.UUID("88888888-8888-8888-8888-888888888888")


def _make_retriever_run(
    ls_retriever_name: str = "keyword",
    ls_vector_store_provider: Optional[str] = None,
) -> LangSmithRun:
    metadata: dict = {"ls_retriever_name": ls_retriever_name}
    if ls_vector_store_provider:
        metadata["ls_vector_store_provider"] = ls_vector_store_provider
    return LangSmithRun(
        id=RETRIEVER_RUN_ID,
        name="KeywordRetriever",
        run_type="retriever",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME,
        tags=[],
        prompt_tokens=None,
        completion_tokens=None,
        total_tokens=None,
        feedback_stats=None,
        app_path=None,
        inputs={"query": "what is datahub"},
        outputs={"documents": []},
        extra={"metadata": metadata},
        parent_run_id=RUN_ID,
    )


def _make_llm_run(
    parent_run_id: uuid.UUID,
    run_id: uuid.UUID = None,
    ls_model_name: str = "claude-haiku-4-5-20251001",
    ls_provider: Optional[str] = "anthropic",
    ls_model_type: Optional[str] = "chat",
) -> LangSmithRun:
    metadata: dict = {"ls_model_name": ls_model_name}
    if ls_provider:
        metadata["ls_provider"] = ls_provider
    if ls_model_type:
        metadata["ls_model_type"] = ls_model_type
    return LangSmithRun(
        id=run_id or LLM_RUN_ID,
        name=ls_model_name,
        run_type="llm",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME,
        tags=[],
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        feedback_stats=None,
        app_path=None,
        inputs={},
        outputs={},
        extra={"metadata": metadata},
        parent_run_id=parent_run_id,
    )


def test_retriever_span_emits_input_edge():
    """_collect_run_edges for a retriever run populates input_edge_urns with a dataset edge."""
    source = _make_source()
    project = _make_project()
    run = _make_retriever_run()
    input_edge_urns = {}
    list(source._collect_run_edges(project, run, input_edge_urns))

    assert len(input_edge_urns) == 1
    edge = list(input_edge_urns.values())[0]
    assert "keyword" in edge.destinationUrn
    assert "my-project" in edge.destinationUrn
    assert source.report.retriever_datasets_emitted == 1


def test_retriever_span_emits_stub_dataset():
    """_build_retriever_stub returns dataset URN and workunits for the upstream source."""
    source = _make_source()
    project = _make_project()
    run = _make_retriever_run()
    ds_urn, stub_wus = source._build_retriever_stub(project, run)

    wus = list(stub_wus)
    assert len(wus) > 0
    assert all(wu.metadata.entityUrn.startswith("urn:li:dataset:") for wu in wus)
    assert "keyword" in ds_urn
    assert "my-project" in ds_urn


def test_non_retriever_span_no_input_edge():
    """_emit_run must never emit DataProcessInstanceInputClass for any run type."""
    source = _make_source()
    project = _make_project()

    for run_type in ("llm", "chain", "tool", "parser", "retriever"):
        run = _make_child_run(parent_run_id=RUN_ID, run_type=run_type)
        wus = list(
            source._emit_run(project, run, parent_dpi_urn=source._make_dpi_urn(RUN_ID))
        )
        input_aspects = [
            wu.metadata.aspect
            for wu in wus
            if isinstance(wu.metadata.aspect, DataProcessInstanceInputClass)
        ]
        assert input_aspects == [], (
            f"Expected no input edge in _emit_run for run_type={run_type}"
        )


def test_retriever_uses_vector_store_provider_as_platform():
    """When ls_vector_store_provider is present, it becomes the dataset platform."""
    source = _make_source()
    project = _make_project()
    run = _make_retriever_run(ls_vector_store_provider="Chroma")
    ds_urn, _ = source._build_retriever_stub(project, run)
    assert "chroma" in ds_urn


def test_retriever_input_edge_entity_urn_is_root_when_provided():
    """Input edge must land on root trace DPI when retriever is a child span."""
    source = _make_source(_make_config(include_child_spans=True))
    project = _make_project()
    root_run = _make_run()
    retriever_run = _make_retriever_run()

    source.client.list_runs = MagicMock(
        side_effect=[
            iter([root_run]),
            iter([retriever_run]),
        ]
    )

    wus = list(source._get_trace_workunits(project))
    root_dpi_urn = source._make_dpi_urn(RUN_ID)

    input_wus = {
        wu.metadata.entityUrn: wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceInputClass)
    }
    assert len(input_wus) == 1
    assert root_dpi_urn in input_wus
    assert len(input_wus[root_dpi_urn].inputEdges) == 1


def test_get_child_span_workunits_retriever_edge_on_root_trace():
    """Input edge lands on root trace DPI when retriever is a child span (end-to-end)."""
    source = _make_source(_make_config(include_child_spans=True))
    project = _make_project()
    root_run = _make_run()
    retriever_run = _make_retriever_run()

    source.client.list_runs = MagicMock(
        side_effect=[
            iter([root_run]),
            iter([retriever_run]),
        ]
    )

    wus = list(source._get_trace_workunits(project))
    root_dpi_urn = source._make_dpi_urn(RUN_ID)

    input_wus = {
        wu.metadata.entityUrn: wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceInputClass)
    }
    assert len(input_wus) == 1
    assert root_dpi_urn in input_wus
    assert len(input_wus[root_dpi_urn].inputEdges) == 1


# -------------------------------------------------------------------------
# MLModel stub and lineage tests
# -------------------------------------------------------------------------


def test_build_model_stub_with_metadata():
    """_build_model_stub returns (urn, workunits) when ls_model_name is present."""
    source = _make_source()
    run = _make_llm_run(parent_run_id=RUN_ID)
    result = source._build_model_stub(run)
    assert result is not None
    model_urn, wus = result
    assert "claude-haiku-4-5-20251001" in model_urn
    assert "anthropic" in model_urn
    assert "mlModel" in model_urn
    assert len(list(wus)) > 0


def test_build_model_stub_no_metadata_returns_none():
    """_build_model_stub returns None when ls_model_name is absent."""
    source = _make_source()
    run = _make_child_run(parent_run_id=RUN_ID, run_type="llm")  # no extra/metadata
    result = source._build_model_stub(run)
    assert result is None


def test_build_model_stub_provider_fallback():
    """_build_model_stub infers provider from model name when ls_provider is absent."""
    source = _make_source()
    run = _make_llm_run(parent_run_id=RUN_ID, ls_provider=None)
    result = source._build_model_stub(run)
    assert result is not None
    model_urn, _ = result
    # Claude model -> anthropic inferred
    assert "anthropic" in model_urn


def test_build_model_stub_invocation_params_fallback():
    """_build_model_stub falls back to invocation_params.model when ls_model_name absent."""
    source = _make_source()
    run = LangSmithRun(
        id=LLM_RUN_ID,
        name="ChatAnthropic",
        run_type="llm",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME,
        tags=[],
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        feedback_stats=None,
        app_path=None,
        inputs={},
        outputs={},
        extra={"invocation_params": {"model": "claude-haiku-4-5-20251001"}},
        parent_run_id=RUN_ID,
    )
    result = source._build_model_stub(run)
    assert result is not None
    model_urn, _ = result
    assert "claude-haiku-4-5-20251001" in model_urn
    assert "anthropic" in model_urn


def test_build_model_stub_metadata_model_fallback():
    """_build_model_stub falls back to metadata.model when ls_model_name absent."""
    source = _make_source()
    run = LangSmithRun(
        id=LLM_RUN_ID,
        name="ChatAnthropic",
        run_type="llm",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME,
        tags=[],
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        feedback_stats=None,
        app_path=None,
        inputs={},
        outputs={},
        extra={"metadata": {"model": "claude-haiku-4-5-20251001", "query_index": "0"}},
        parent_run_id=RUN_ID,
    )
    result = source._build_model_stub(run)
    assert result is not None
    model_urn, _ = result
    assert "claude-haiku-4-5-20251001" in model_urn
    assert "anthropic" in model_urn


def test_retriever_and_llm_produce_combined_edge():
    """Trace with both retriever + LLM children produces a single DataProcessInstanceInput
    with 2 inputEdges on the root DPI."""
    source = _make_source(_make_config(include_child_spans=True))
    project = _make_project()
    root_run = _make_run()
    retriever_run = _make_retriever_run()
    llm_run = _make_llm_run(parent_run_id=RUN_ID)

    source.client.list_runs = MagicMock(
        side_effect=[
            iter([root_run]),
            iter([retriever_run, llm_run]),
        ]
    )

    wus = list(source._get_trace_workunits(project))
    root_dpi_urn = source._make_dpi_urn(RUN_ID)

    input_wus = {
        wu.metadata.entityUrn: wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceInputClass)
    }
    assert len(input_wus) == 1
    assert root_dpi_urn in input_wus
    edge_urns = {e.destinationUrn for e in input_wus[root_dpi_urn].inputEdges}
    assert len(edge_urns) == 2
    assert any("dataset" in u for u in edge_urns)
    assert any("mlModel" in u for u in edge_urns)


def test_duplicate_model_calls_deduplicated():
    """Multiple LLM children calling the same model produce only 1 model edge."""
    source = _make_source(_make_config(include_child_spans=True))
    project = _make_project()
    root_run = _make_run()
    llm_run_1 = _make_llm_run(
        parent_run_id=RUN_ID,
        run_id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
    )
    llm_run_2 = _make_llm_run(
        parent_run_id=RUN_ID,
        run_id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002"),
    )

    source.client.list_runs = MagicMock(
        side_effect=[
            iter([root_run]),
            iter([llm_run_1, llm_run_2]),
        ]
    )

    wus = list(source._get_trace_workunits(project))
    root_dpi_urn = source._make_dpi_urn(RUN_ID)

    input_wus = {
        wu.metadata.entityUrn: wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceInputClass)
    }
    assert len(input_wus) == 1
    assert len(input_wus[root_dpi_urn].inputEdges) == 1
    assert source.report.models_emitted == 2  # both processed; edge deduped


def test_llm_child_without_model_metadata_no_edge():
    """LLM child with no ls_model_name in metadata produces no input edge."""
    source = _make_source(_make_config(include_child_spans=True))
    project = _make_project()
    root_run = _make_run()
    llm_run = _make_child_run(parent_run_id=RUN_ID, run_type="llm")  # no extra

    source.client.list_runs = MagicMock(
        side_effect=[
            iter([root_run]),
            iter([llm_run]),
        ]
    )

    wus = list(source._get_trace_workunits(project))

    input_aspects = [
        wu.metadata.aspect
        for wu in wus
        if isinstance(wu.metadata.aspect, DataProcessInstanceInputClass)
    ]
    assert len(input_aspects) == 0


# -------------------------------------------------------------------------
# Assertion tests
# -------------------------------------------------------------------------

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:langsmith,eval-dataset,PROD)"


def _make_source_with_assertions(**extra_kwargs) -> LangSmithSource:
    config = _make_config(include_assertions=True, **extra_kwargs)
    source = _make_source(config)
    source._dataset_urns["eval-dataset"] = DATASET_URN
    return source


def test_config_include_assertions_default_false():
    config = _make_config()
    assert config.include_assertions is False


def test_config_assertion_score_threshold_default():
    config = _make_config()
    assert config.assertion_score_threshold == 0.7


def test_assertion_emitted_for_feedback():
    """A run with feedback_stats emits AssertionInfo + AssertionRunEvent."""
    source = _make_source_with_assertions()
    run = _make_run(feedback_stats={"correctness": {"avg": 0.85, "n": 5}})

    wus = list(source._emit_assertion_workunits(run, DATASET_URN))

    info_wus = [w for w in wus if isinstance(w.metadata.aspect, AssertionInfoClass)]
    event_wus = [w for w in wus if isinstance(w.metadata.aspect, AssertionRunEventClass)]

    assert len(info_wus) == 1
    assert len(event_wus) == 1
    assert source.report.assertions_emitted == 1
    assert source.report.assertion_run_events_emitted == 1


def test_assertion_result_success_above_threshold():
    """Score 0.85 >= threshold 0.7 produces SUCCESS."""
    source = _make_source_with_assertions(assertion_score_threshold=0.7)
    run = _make_run(feedback_stats={"correctness": {"avg": 0.85, "n": 5}})

    wus = list(source._emit_assertion_workunits(run, DATASET_URN))
    event = next(w.metadata.aspect for w in wus if isinstance(w.metadata.aspect, AssertionRunEventClass))
    assert event.result.type == AssertionResultTypeClass.SUCCESS


def test_assertion_result_failure_below_threshold():
    """Score 0.5 < threshold 0.7 produces FAILURE."""
    source = _make_source_with_assertions(assertion_score_threshold=0.7)
    run = _make_run(feedback_stats={"correctness": {"avg": 0.5, "n": 5}})

    wus = list(source._emit_assertion_workunits(run, DATASET_URN))
    event = next(w.metadata.aspect for w in wus if isinstance(w.metadata.aspect, AssertionRunEventClass))
    assert event.result.type == AssertionResultTypeClass.FAILURE


def test_assertion_result_exact_threshold_is_success():
    """Score exactly equal to threshold is SUCCESS (>=)."""
    source = _make_source_with_assertions(assertion_score_threshold=0.7)
    run = _make_run(feedback_stats={"correctness": {"avg": 0.7, "n": 5}})

    wus = list(source._emit_assertion_workunits(run, DATASET_URN))
    event = next(w.metadata.aspect for w in wus if isinstance(w.metadata.aspect, AssertionRunEventClass))
    assert event.result.type == AssertionResultTypeClass.SUCCESS


def test_assertion_actual_agg_value():
    """AssertionResult.actualAggValue carries the avg score."""
    source = _make_source_with_assertions()
    run = _make_run(feedback_stats={"correctness": {"avg": 0.85, "n": 5}})

    wus = list(source._emit_assertion_workunits(run, DATASET_URN))
    event = next(w.metadata.aspect for w in wus if isinstance(w.metadata.aspect, AssertionRunEventClass))
    assert event.result.actualAggValue == pytest.approx(0.85)


def test_assertion_info_emitted_per_run_name():
    """Two runs with different names but same feedback key produce 2 AssertionInfo + 2 AssertionRunEvents."""
    source = _make_source_with_assertions()
    run1 = _make_run(name="test-chain", feedback_stats={"correctness": {"avg": 0.9, "n": 3}})
    run2 = LangSmithRun(
        id=uuid.UUID("55555555-5555-5555-5555-555555555555"),
        name="run-2",
        run_type="chain",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME,
        tags=[],
        feedback_stats={"correctness": {"avg": 0.6, "n": 2}},
        app_path=None,
        inputs={},
        outputs={},
    )

    wus1 = list(source._emit_assertion_workunits(run1, DATASET_URN))
    wus2 = list(source._emit_assertion_workunits(run2, DATASET_URN))
    all_wus = wus1 + wus2

    info_wus = [w for w in all_wus if isinstance(w.metadata.aspect, AssertionInfoClass)]
    event_wus = [w for w in all_wus if isinstance(w.metadata.aspect, AssertionRunEventClass)]

    assert len(info_wus) == 2
    assert len(event_wus) == 2
    assert source.report.assertions_emitted == 2
    assert source.report.assertion_run_events_emitted == 2


def test_assertion_info_dedup_same_name():
    """Two runs with same name and same feedback key produce 1 AssertionInfo + 2 AssertionRunEvents."""
    source = _make_source_with_assertions()
    run1 = _make_run(name="test-chain", feedback_stats={"correctness": {"avg": 0.9, "n": 3}})
    run2 = LangSmithRun(
        id=uuid.UUID("55555555-5555-5555-5555-555555555555"),
        name="test-chain",
        run_type="chain",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME,
        tags=[],
        feedback_stats={"correctness": {"avg": 0.6, "n": 2}},
        app_path=None,
        inputs={},
        outputs={},
    )

    wus1 = list(source._emit_assertion_workunits(run1, DATASET_URN))
    wus2 = list(source._emit_assertion_workunits(run2, DATASET_URN))
    all_wus = wus1 + wus2

    info_wus = [w for w in all_wus if isinstance(w.metadata.aspect, AssertionInfoClass)]
    event_wus = [w for w in all_wus if isinstance(w.metadata.aspect, AssertionRunEventClass)]

    assert len(info_wus) == 1
    assert len(event_wus) == 2
    assert source.report.assertions_emitted == 1
    assert source.report.assertion_run_events_emitted == 2


def test_assertion_run_name_in_description():
    """AssertionInfo description is '<run_name>: <key> score >= <threshold>'."""
    source = _make_source_with_assertions()
    run = _make_run(name="test-chain", feedback_stats={"correctness": {"avg": 0.85, "n": 5}})
    wus = list(source._emit_assertion_workunits(run, DATASET_URN))
    info = next(w.metadata.aspect for w in wus if isinstance(w.metadata.aspect, AssertionInfoClass))
    assert info.description == "test-chain: correctness score >= 0.7"


def test_assertion_run_name_in_custom_properties():
    """AssertionInfo customProperties includes run_name."""
    source = _make_source_with_assertions()
    run = _make_run(name="test-chain", feedback_stats={"correctness": {"avg": 0.85, "n": 5}})
    wus = list(source._emit_assertion_workunits(run, DATASET_URN))
    info = next(w.metadata.aspect for w in wus if isinstance(w.metadata.aspect, AssertionInfoClass))
    assert info.customProperties.get("run_name") == "test-chain"




def test_assertion_skipped_when_no_dataset():
    """With no dataset URN available, assertions are skipped and counter incremented."""
    source = _make_source(_make_config(include_assertions=True))
    # _dataset_urns is empty; no assertion_dataset_urn configured
    project = _make_project()
    run = _make_run(feedback_stats={"correctness": {"avg": 0.9, "n": 5}})

    source.client.list_runs = MagicMock(return_value=iter([run]))

    list(source._get_trace_workunits(project))

    assert source.report.assertions_skipped_no_dataset == 1
    assert source.report.assertions_emitted == 0


def test_assertion_uses_explicit_dataset_urn():
    """assertion_dataset_urn config overrides auto-detected dataset."""
    explicit_urn = "urn:li:dataset:(urn:li:dataPlatform:langsmith,explicit-ds,PROD)"
    source = _make_source(_make_config(
        include_assertions=True,
        assertion_dataset_urn=explicit_urn,
    ))
    # Also populate _dataset_urns - explicit should win
    source._dataset_urns["other-dataset"] = DATASET_URN

    assert source._resolve_assertion_dataset_urn() == explicit_urn


def test_assertion_urn_deterministic():
    """Same inputs always produce the same assertion URN."""
    source1 = _make_source_with_assertions()
    source2 = _make_source_with_assertions()
    run = _make_run(feedback_stats={"correctness": {"avg": 0.9, "n": 5}})

    wus1 = list(source1._emit_assertion_workunits(run, DATASET_URN))
    wus2 = list(source2._emit_assertion_workunits(run, DATASET_URN))

    urns1 = {w.metadata.entityUrn for w in wus1}
    urns2 = {w.metadata.entityUrn for w in wus2}
    assert urns1 == urns2


def test_assertions_not_emitted_when_disabled():
    """include_assertions=False produces no assertion MCPs even with feedback."""
    source = _make_source(_make_config(include_assertions=False))
    source._dataset_urns["eval-dataset"] = DATASET_URN
    project = _make_project()
    run = _make_run(feedback_stats={"correctness": {"avg": 0.9, "n": 5}})

    source.client.list_runs = MagicMock(return_value=iter([run]))

    wus = list(source._get_trace_workunits(project))

    assert not any(isinstance(w.metadata.aspect, AssertionInfoClass) for w in wus)
    assert not any(isinstance(w.metadata.aspect, AssertionRunEventClass) for w in wus)
