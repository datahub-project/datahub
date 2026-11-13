from typing import Any, Dict, List, Optional, Set, Tuple, cast
from unittest.mock import MagicMock, patch

import google.cloud.aiplatform as aiplatform
import pytest
from google.api_core.exceptions import NotFound, RetryError, ServiceUnavailable

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai.vertexai import VertexAISource
from datahub.ingestion.source.vertexai.vertexai_config import VertexAIConfig
from datahub.ingestion.source.vertexai.vertexai_constants import (
    LINKS_LOST_ON_STOP,
    DetailCalls,
)
from tests.integration.vertexai.mock_vertexai import (
    gen_mock_dataset,
    gen_mock_model,
    gen_mock_model_version,
    gen_mock_training_automl_job,
    gen_mock_training_custom_job,
)

_TRAINING_MODULE = "datahub.ingestion.source.vertexai.vertexai_training_extractor"
_MODEL_MODULE = "datahub.ingestion.source.vertexai.vertexai_model_extractor"


def test_only_a_stopped_pipeline_lookup_holds_back_the_models() -> None:
    """Models are written whole, links included, so a stopped read that drops a
    model link must either rebuild it or hold the models back. A kept training
    job's links are rebuilt from its listing (the tests below), and experiment
    links never reach a written model (the tripwire below). Only a pipeline's
    task details name the models its tasks use, so only its stop holds back."""
    assert {DetailCalls.PIPELINE_LOOKUP} == LINKS_LOST_ON_STOP


def _source() -> VertexAISource:
    source = VertexAISource(
        ctx=PipelineContext(run_id="model-links"),
        config=VertexAIConfig.model_validate(
            {
                "project_ids": ["proj-one"],
                "regions": ["us-central1"],
                "include_models": True,
                "include_evaluations": False,
                "include_pipelines": False,
                "include_experiments": False,
                "use_ml_metadata_for_lineage": False,
                "extract_execution_metrics": False,
            }
        ),
    )
    source._current_project_id = "proj-one"
    source._current_region = "us-central1"
    source.training_extractor._carry = MagicMock()
    source.model_extractor._carry = MagicMock()
    return source


def _automl_job(model_num: int, version: str, suffix: str = "") -> Any:
    job = gen_mock_training_automl_job()
    config = cast(MagicMock, job.to_dict).return_value
    # The SDK names a model by the last segment of its resource name.
    config["modelToUpload"]["name"] = (
        f"projects/123/locations/us-central1/models/"
        f"{gen_mock_model(model_num).name}{suffix}"
    )
    config["modelToUpload"]["versionId"] = version
    return job


# job, listed models, output model the lookup finds (None: deleted), datasets
_CASES: Dict[str, Tuple[Any, List[int], Optional[int], bool]] = {
    "automl": (lambda: _automl_job(1, "1"), [1], 1, True),
    "automl_version_2": (lambda: _automl_job(2, "2"), [2], 2, True),
    "automl_name_with_version": (lambda: _automl_job(1, "1", "@1"), [1], 1, True),
    "automl_unlisted_dataset": (lambda: _automl_job(1, "1"), [1], 1, False),
    "automl_model_deleted": (lambda: _automl_job(1, "1"), [], None, True),
    "automl_version_gone": (lambda: _automl_job(1, "3"), [1], 1, True),
    "custom": (gen_mock_training_custom_job, [1], None, True),
}


def _run(case: str, lookup_stopped: bool) -> Tuple[Dict[str, Any], Set[str]]:
    """Training then the model step; returns the links on the written models
    and groups, and the URNs that got a lineage-only write."""
    make_job, models, output, datasets = _CASES[case]
    job = make_job()
    source = _source()
    automl = "AutoML" in type(job).__name__
    training_lists = {
        "AutoMLTabularTrainingJob": [job] if automl else [],
        "CustomJob": [] if automl else [job],
        "TabularDataset": [gen_mock_dataset()] if datasets else [],
    }
    listed_models = [gen_mock_model(n) for n in models]

    def training_list(cls: type, *_args: object, **_kw: object) -> List[Any]:
        return training_lists.get(cls.__name__, [])

    def model_list(cls: type, *_args: object, **_kw: object) -> List[Any]:
        return listed_models if cls is aiplatform.Model else []

    stop = RetryError("retries exhausted", cause=ServiceUnavailable("busy"))
    with (
        patch(f"{_TRAINING_MODULE}.rate_limited_gapic_list", side_effect=training_list),
        patch(
            f"{_TRAINING_MODULE}.Model",
            **(
                {"return_value": gen_mock_model(output)}
                if output
                else {"side_effect": NotFound("deleted")}
            ),
        ),
        patch(
            f"{_TRAINING_MODULE}.get_job_result_status",
            **({"side_effect": stop} if lookup_stopped else {"return_value": None}),
        ),
        patch(f"{_MODEL_MODULE}.rate_limited_gapic_list", side_effect=model_list),
    ):
        list(source.training_extractor.get_workunits())
        list(source.model_extractor.get_model_workunits())
        lineage_only = {
            wu.get_urn() for wu in source.model_extractor.emit_pending_lineage_updates()
        }
    tracker = source.model_usage_tracker
    links = {
        urn: (
            sorted(rel.training_jobs),
            sorted(rel.training_data_urns),
            sorted(rel.downstream_jobs),
        )
        for urn, rel in tracker.relationships.items()
        if urn in tracker.emitted_urns
    }
    return links, lineage_only


@pytest.mark.parametrize("case", sorted(_CASES))
def test_a_kept_job_gives_the_written_models_the_links_a_clean_read_gives(
    case: str,
) -> None:
    clean = _run(case, lookup_stopped=False)
    kept = _run(case, lookup_stopped=True)

    # A kept job's links are rebuilt from its listing, so the models can be
    # written without erasing them, and a model the step does not write gets no
    # lineage-only write.
    assert kept == clean


def test_experiment_links_do_not_reach_written_model_entities() -> None:
    source = _source()
    model = gen_mock_model(1)
    artifact = MagicMock(uri=model.resource_name)

    source.experiment_extractor._extract_edges_from_artifacts(
        [artifact], "urn:li:dataProcessInstance:run-execution", is_input=True
    )

    written = source.urn_builder.make_ml_model_urn(
        gen_mock_model_version(model),
        model_name=source.name_formatter.format_model_name(model.name),
    )
    assert source.model_usage_tracker.relationships, "the link was not recorded"
    # When this fails, experiment links reach real models: a stopped experiment
    # read must then hold the models back (LINKS_LOST_ON_STOP and the ledger's
    # hold-back), or rebuild the links, or a throttle will erase them.
    assert written not in source.model_usage_tracker.relationships
