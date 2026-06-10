import datetime
from pathlib import Path
from typing import Any, Union

import pytest
from mlflow import MlflowClient
from mlflow.entities import (
    Dataset as MLflowDataset,
    DatasetInput,
    InputTag,
    Run,
    RunData,
    RunInfo,
    RunInputs,
)
from mlflow.entities.model_registry import RegisteredModel
from mlflow.entities.model_registry.model_version import ModelVersion
from mlflow.store.entities import PagedList

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.mlflow import MLflowConfig, MLflowSource
from datahub.metadata.schema_classes import UpstreamLineageClass


@pytest.fixture
def tracking_uri(tmp_path: Path) -> str:
    return str(tmp_path / "mlruns")


@pytest.fixture
def source(tracking_uri: str) -> MLflowSource:
    return MLflowSource(
        ctx=PipelineContext(run_id="mlflow-source-test"),
        config=MLflowConfig(tracking_uri=tracking_uri),
    )


@pytest.fixture
def registered_model(source: MLflowSource) -> RegisteredModel:
    model_name = "abc"
    return RegisteredModel(name=model_name)


@pytest.fixture
def model_version(
    source: MLflowSource,
    registered_model: RegisteredModel,
) -> ModelVersion:
    version = "1"
    return ModelVersion(
        name=registered_model.name,
        version=version,
        creation_timestamp=datetime.datetime.now(),
    )


@pytest.fixture
def mlflow_dataset() -> MLflowDataset:
    return MLflowDataset(
        name="test_dataset",
        digest="test_digest",
        source="test_source",
        source_type="snowflake",
        schema='{"mlflow_colspec":[{"name":"col1", "type":"string"}]}',
    )


@pytest.fixture
def mlflow_local_dataset() -> MLflowDataset:
    return MLflowDataset(
        name="test_local_dataset",
        digest="test_digest",
        source="test_source",
        source_type="local",
        schema='{"mlflow_colspec":[{"name":"col1", "type":"string"}]}',
    )


@pytest.fixture
def mlflow_unsupported_dataset() -> MLflowDataset:
    return MLflowDataset(
        name="test_unsupported_dataset",
        digest="test_digest",
        source="test_source",
        source_type="unsupported_platform",
        schema='{"mlflow_colspec":[{"name":"col1", "type":"string"}]}',
    )


def dummy_search_func(page_token: Union[None, str], **kwargs: Any) -> PagedList[str]:
    dummy_pages = dict(
        page_1=PagedList(items=["a", "b"], token="page_2"),
        page_2=PagedList(items=["c", "d"], token="page_3"),
        page_3=PagedList(items=["e"], token=None),
    )
    if page_token is None:
        page_to_return = dummy_pages["page_1"]
    else:
        page_to_return = dummy_pages[page_token]
    if kwargs.get("case", "") == "upper":
        page_to_return = PagedList(
            items=[e.upper() for e in page_to_return.to_list()],
            token=page_to_return.token,
        )
    return page_to_return


def test_stages(source):
    mlflow_registered_model_stages = {
        "Production",
        "Staging",
        "Archived",
        None,
    }
    workunits = source._get_tags_workunits()
    names = [wu.metadata.aspect.name for wu in workunits]

    assert len(names) == len(mlflow_registered_model_stages)
    assert set(names) == {
        "mlflow_" + str(stage).lower() for stage in mlflow_registered_model_stages
    }


def test_config_model_name_separator(source, model_version):
    name_version_sep = "+"
    source.config.model_name_separator = name_version_sep
    expected_model_name = (
        f"{model_version.name}{name_version_sep}{model_version.version}"
    )
    expected_urn = f"urn:li:mlModel:(urn:li:dataPlatform:mlflow,{expected_model_name},{source.config.env})"

    urn = source._make_ml_model_urn(model_version)

    assert urn == expected_urn


def test_model_without_run(source, registered_model, model_version):
    run = source._get_mlflow_run(model_version)
    wu = source._get_ml_model_properties_workunit(
        registered_model=registered_model,
        model_version=model_version,
        run=run,
    )
    aspect = wu.metadata.aspect

    assert aspect.hyperParams is None
    assert aspect.trainingMetrics is None


def test_traverse_mlflow_search_func(source):
    expected_items = ["a", "b", "c", "d", "e"]

    items = list(source._traverse_mlflow_search_func(dummy_search_func))

    assert items == expected_items


def test_traverse_mlflow_search_func_with_kwargs(source):
    expected_items = ["A", "B", "C", "D", "E"]

    items = list(source._traverse_mlflow_search_func(dummy_search_func, case="upper"))

    assert items == expected_items


def test_make_external_link_local(source, model_version):
    expected_url = None

    url = source._make_external_url(model_version)

    assert url == expected_url


def test_make_external_link_remote(source, model_version):
    tracking_uri_remote = "https://dummy-mlflow-tracking-server.org"
    source.client = MlflowClient(tracking_uri=tracking_uri_remote)
    expected_url = f"{tracking_uri_remote}/#/models/{model_version.name}/versions/{model_version.version}"

    url = source._make_external_url(model_version)

    assert url == expected_url


def test_make_external_link_remote_via_config(source, model_version):
    custom_base_url = "https://custom-server.org"
    source.config.base_external_url = custom_base_url
    source.client = MlflowClient(
        tracking_uri="https://dummy-mlflow-tracking-server.org"
    )
    expected_url = f"{custom_base_url}/#/models/{model_version.name}/versions/{model_version.version}"

    url = source._make_external_url(model_version)

    assert url == expected_url


def test_local_dataset_reference_creation(source, mlflow_local_dataset):
    """Test that local dataset reference is always created for local source type"""
    run = Run(
        RunInfo(
            run_id="test_run",
            run_uuid="test_uuid",
            experiment_id="test_exp",
            user_id="test_user",
            status="FINISHED",
            start_time=1234567890,
            end_time=1234567891,
            artifact_uri="s3://test-bucket/test",
            lifecycle_stage="active",
        ),
        RunData(metrics={}, params={}, tags=[]),
        RunInputs(
            dataset_inputs=[
                DatasetInput(
                    dataset=mlflow_local_dataset,
                    tags=[InputTag(key="key", value="value")],
                )
            ]
        ),
    )
    workunits = list(source._get_dataset_input_workunits(run))

    # Should create local dataset reference
    assert len(workunits) > 0
    assert any(
        "urn:li:dataset:(urn:li:dataPlatform:mlflow" in wu.id for wu in workunits
    )


def test_materialization_disabled_with_supported_platform(source, mlflow_dataset):
    """
    Test when materialize_dataset_inputs=False and platform is supported:
    - Should create dataset reference
    - Should not create hosted dataset
    - Should try to link with existing upstream if exists
    """
    source.config.materialize_dataset_inputs = False

    # Mock the graph to simulate existing dataset
    class MockGraph:
        def get_aspect_v2(self, *args, **kwargs):
            return {}  # Return non-None to simulate existing dataset

    source.ctx.graph = MockGraph()

    run = Run(
        RunInfo(
            run_id="test_run",
            run_uuid="test_uuid",
            experiment_id="test_exp",
            user_id="test_user",
            status="FINISHED",
            start_time=1234567890,
            end_time=1234567891,
            artifact_uri="s3://test-bucket/test",
            lifecycle_stage="active",
        ),
        RunData(metrics={}, params={}, tags=[]),
        RunInputs(
            dataset_inputs=[
                DatasetInput(
                    dataset=mlflow_dataset,
                    tags=[InputTag(key="key", value="value")],
                )
            ]
        ),
    )
    workunits = list(source._get_dataset_input_workunits(run))

    # Should create dataset reference
    assert len(workunits) > 0
    # Should have upstream lineage
    assert any(isinstance(wu.metadata.aspect, UpstreamLineageClass) for wu in workunits)
    # Should not create hosted dataset (no snowflake platform URN)
    assert not any(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake" in wu.id for wu in workunits
    )


def test_materialization_disabled_with_unsupported_platform(
    source, mlflow_unsupported_dataset
):
    """
    Test when materialize_dataset_inputs=False and platform is not supported:
    - Should create dataset reference
    - Should not create hosted dataset
    - Should not try to link upstream
    """
    source.config.materialize_dataset_inputs = False

    run = Run(
        RunInfo(
            run_id="test_run",
            run_uuid="test_uuid",
            experiment_id="test_exp",
            user_id="test_user",
            status="FINISHED",
            start_time=1234567890,
            end_time=1234567891,
            artifact_uri="s3://test-bucket/test",
            lifecycle_stage="active",
        ),
        RunData(metrics={}, params={}, tags=[]),
        RunInputs(
            dataset_inputs=[
                DatasetInput(
                    dataset=mlflow_unsupported_dataset,
                    tags=[InputTag(key="key", value="value")],
                )
            ]
        ),
    )
    workunits = list(source._get_dataset_input_workunits(run))

    # Should create dataset reference
    assert len(workunits) > 0
    # Should not have upstream lineage
    assert not any(
        isinstance(wu.metadata.aspect, UpstreamLineageClass) for wu in workunits
    )


def test_materialization_enabled_with_supported_platform(source, mlflow_dataset):
    """
    Test when materialize_dataset_inputs=True and platform is supported:
    - Should create dataset reference
    - Should create hosted dataset
    """
    source.config.materialize_dataset_inputs = True
    source.config.source_mapping_to_platform = {"snowflake": "snowflake"}

    run = Run(
        RunInfo(
            run_id="test_run",
            run_uuid="test_uuid",
            experiment_id="test_exp",
            user_id="test_user",
            status="FINISHED",
            start_time=1234567890,
            end_time=1234567891,
            artifact_uri="s3://test-bucket/test",
            lifecycle_stage="active",
        ),
        RunData(metrics={}, params={}, tags=[]),
        RunInputs(
            dataset_inputs=[
                DatasetInput(
                    dataset=mlflow_dataset,
                    tags=[InputTag(key="key", value="value")],
                )
            ]
        ),
    )
    workunits = list(source._get_dataset_input_workunits(run))

    # Should create both dataset reference and hosted dataset
    assert len(workunits) > 0
    assert any(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake" in wu.id for wu in workunits
    )


def test_materialization_enabled_with_unsupported_platform(
    source, mlflow_unsupported_dataset
):
    """
    Test when materialize_dataset_inputs=True and platform is not supported:
    - Should report error about missing platform mapping
    """
    source.config.materialize_dataset_inputs = True
    source.config.source_mapping_to_platform = {}

    run = Run(
        RunInfo(
            run_id="test_run",
            run_uuid="test_uuid",
            experiment_id="test_exp",
            user_id="test_user",
            status="FINISHED",
            start_time=1234567890,
            end_time=1234567891,
            artifact_uri="s3://test-bucket/test",
            lifecycle_stage="active",
        ),
        RunData(metrics={}, params={}, tags=[]),
        RunInputs(
            dataset_inputs=[
                DatasetInput(
                    dataset=mlflow_unsupported_dataset,
                    tags=[InputTag(key="key", value="value")],
                )
            ]
        ),
    )
    workunits = list(source._get_dataset_input_workunits(run))

    # Should not create any workunits due to error
    assert len(workunits) == 0
    # Should report error about missing platform mapping
    assert any(
        "No mapping dataPlatform found" in report.message
        for report in source.report.failures
    )


def test_materialization_enabled_with_custom_mapping(
    source, mlflow_unsupported_dataset
):
    """
    Test when materialize_dataset_inputs=True with custom platform mapping:
    - Should create dataset reference
    - Should create hosted dataset with mapped platform
    """
    source.config.materialize_dataset_inputs = True
    source.config.source_mapping_to_platform = {"unsupported_platform": "snowflake"}

    run = Run(
        RunInfo(
            run_id="test_run",
            run_uuid="test_uuid",
            experiment_id="test_exp",
            user_id="test_user",
            status="FINISHED",
            start_time=1234567890,
            end_time=1234567891,
            artifact_uri="s3://test-bucket/test",
            lifecycle_stage="active",
        ),
        RunData(metrics={}, params={}, tags=[]),
        RunInputs(
            dataset_inputs=[
                DatasetInput(
                    dataset=mlflow_unsupported_dataset,
                    tags=[InputTag(key="key", value="value")],
                )
            ]
        ),
    )
    workunits = list(source._get_dataset_input_workunits(run))

    # Should create both dataset reference and hosted dataset
    assert len(workunits) > 0
    assert any(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake" in wu.id for wu in workunits
    )
