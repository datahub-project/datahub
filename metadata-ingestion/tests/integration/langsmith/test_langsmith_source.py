import uuid
from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from langsmith.schemas import (
    Dataset as LangSmithDataset,
    Run as LangSmithRun,
    TracerSession as LangSmithProject,
)

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

# Fixed UUIDs for deterministic output
PROJECT_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
TENANT_ID = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
RUN_ID_1 = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
RUN_ID_2 = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
# Child span UUIDs (children of RUN_ID_1)
SPAN_RETRIEVER_ID = uuid.UUID("11111111-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SPAN_LLM_ID = uuid.UUID("22222222-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
# Child span UUID (child of RUN_ID_2)
SPAN_LLM_2_ID = uuid.UUID("33333333-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
DATASET_ID = uuid.UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")

START_TIME = datetime(2024, 6, 1, 9, 0, 0, tzinfo=timezone.utc)
END_TIME_1 = datetime(2024, 6, 1, 9, 0, 3, tzinfo=timezone.utc)  # 3s
END_TIME_2 = datetime(2024, 6, 1, 9, 0, 10, tzinfo=timezone.utc)  # 10s


def _make_mock_client() -> MagicMock:
    client = MagicMock()

    project = LangSmithProject(
        id=PROJECT_ID,
        name="qa-pipeline",
        description="LangSmith connector demo project",
        start_time=START_TIME,
        tenant_id=TENANT_ID,
        reference_dataset_id=None,
    )

    run_1 = LangSmithRun(
        id=RUN_ID_1,
        name="QA Chain",
        run_type="chain",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME_1,
        tags=["production", "v1"],
        prompt_tokens=512,
        completion_tokens=128,
        total_tokens=640,
        feedback_stats={"correctness": {"avg": 0.9, "n": 5}},
        app_path=f"/o/{TENANT_ID}/projects/p/{PROJECT_ID}/r/{RUN_ID_1}",
        inputs={"question": "What is DataHub?"},
        outputs={"answer": "DataHub is a metadata platform."},
        extra={"metadata": {"user_id": "u123", "version": "1.0"}},
    )

    run_2 = LangSmithRun(
        id=RUN_ID_2,
        name="Summarization Chain",
        run_type="chain",
        status="error",
        start_time=START_TIME,
        end_time=END_TIME_2,
        tags=[],
        prompt_tokens=1024,
        completion_tokens=0,
        total_tokens=1024,
        feedback_stats=None,
        app_path=f"/o/{TENANT_ID}/projects/p/{PROJECT_ID}/r/{RUN_ID_2}",
        inputs={"text": "Long document..."},
        outputs=None,
        error="OpenAI rate limit exceeded",
        extra=None,
    )

    # Child spans for run_1: a retriever call followed by an LLM call
    span_retriever = LangSmithRun(
        id=SPAN_RETRIEVER_ID,
        name="KeywordRetriever",
        run_type="retriever",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME_1,
        tags=[],
        prompt_tokens=None,
        completion_tokens=None,
        total_tokens=None,
        feedback_stats=None,
        app_path=f"/o/{TENANT_ID}/projects/p/{PROJECT_ID}/r/{SPAN_RETRIEVER_ID}",
        inputs={"query": "what is datahub"},
        outputs={"documents": []},
        extra={"metadata": {"ls_retriever_name": "keyword"}},
        parent_run_id=RUN_ID_1,
    )
    span_llm = LangSmithRun(
        id=SPAN_LLM_ID,
        name="claude-haiku",
        run_type="llm",
        status="success",
        start_time=START_TIME,
        end_time=END_TIME_1,
        tags=[],
        prompt_tokens=512,
        completion_tokens=128,
        total_tokens=640,
        feedback_stats=None,
        app_path=f"/o/{TENANT_ID}/projects/p/{PROJECT_ID}/r/{SPAN_LLM_ID}",
        inputs={},
        outputs={},
        extra={
            "metadata": {
                "ls_model_name": "claude-haiku-4-5-20251001",
                "ls_provider": "anthropic",
                "ls_model_type": "chat",
            }
        },
        parent_run_id=RUN_ID_1,
    )

    # Child span for run_2: one LLM call that errored
    span_llm_2 = LangSmithRun(
        id=SPAN_LLM_2_ID,
        name="claude-haiku",
        run_type="llm",
        status="error",
        start_time=START_TIME,
        end_time=END_TIME_2,
        tags=[],
        prompt_tokens=1024,
        completion_tokens=0,
        total_tokens=1024,
        feedback_stats=None,
        app_path=f"/o/{TENANT_ID}/projects/p/{PROJECT_ID}/r/{SPAN_LLM_2_ID}",
        inputs={},
        outputs={},
        extra={
            "metadata": {
                "ls_model_name": "claude-haiku-4-5-20251001",
                "ls_provider": "anthropic",
                "ls_model_type": "chat",
            }
        },
        parent_run_id=RUN_ID_2,
    )

    dataset = LangSmithDataset(
        id=DATASET_ID,
        name="qa-eval-dataset",
        description="Q&A evaluation pairs",
        created_at=START_TIME,
        data_type="kv",
    )

    project_children = {
        RUN_ID_1: [span_retriever, span_llm],
        RUN_ID_2: [span_llm_2],
    }

    def _list_runs_side_effect(**kwargs):
        if kwargs.get("is_root") is False:
            trace_id = kwargs.get("trace_id")
            return iter(project_children.get(trace_id, []))
        return iter([run_1, run_2])

    client.list_projects.return_value = iter([project])
    client.list_runs.side_effect = _list_runs_side_effect
    client.list_datasets.return_value = iter([dataset])

    return client


@pytest.fixture
def sink_file_path(tmp_path) -> str:
    return str(tmp_path / "langsmith_mcps.json")


@pytest.fixture
def pipeline_config(sink_file_path: str) -> Dict[str, Any]:
    return {
        "run_id": "langsmith-source-test",
        "source": {
            "type": "langsmith",
            "config": {
                "api_key": "test-api-key-not-used",
                "include_child_spans": True,
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": sink_file_path,
            },
        },
    }


def test_ingestion(
    pytestconfig,
    mock_time,
    sink_file_path,
    pipeline_config,
):
    golden_file_path = (
        pytestconfig.rootpath / "tests/integration/langsmith/langsmith_mcps_golden.json"
    )

    mock_client = _make_mock_client()

    with patch("langsmith.Client", return_value=mock_client):
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=sink_file_path,
        golden_path=golden_file_path,
    )
