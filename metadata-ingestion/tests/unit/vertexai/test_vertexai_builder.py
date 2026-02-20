from unittest.mock import MagicMock

import pytest
from google.cloud.aiplatform.models import VersionInfo

from datahub.ingestion.source.vertexai.vertexai_builder import (
    VertexAIURIParser,
    VertexAIUrnBuilder,
)
from datahub.ingestion.source.vertexai.vertexai_config import PlatformDetail
from datahub.ingestion.source.vertexai.vertexai_models import ArtifactURNs


class TestVertexAIUrnBuilder:
    @pytest.fixture
    def builder(self) -> VertexAIUrnBuilder:
        return VertexAIUrnBuilder(
            platform="vertexai",
            env="PROD",
            get_project_id_fn=lambda: "test-project",
            platform_instance=None,
        )

    def test_creates_urns_with_correct_prefixes_and_names(
        self, builder: VertexAIUrnBuilder
    ) -> None:
        version_info = MagicMock(spec=VersionInfo)
        version_info.version_id = "v1"
        model_urn = builder.make_ml_model_urn(version_info, "my-model")
        assert "urn:li:mlModel:(urn:li:dataPlatform:vertexai," in model_urn
        assert "my-model_v1" in model_urn

        job_urn = builder.make_training_job_urn("job-123")
        assert "urn:li:dataProcessInstance:" in job_urn
        assert "test-project.job.job-123" in job_urn

        dataset_urn = builder.make_dataset_urn("my-dataset")
        assert "urn:li:dataset:(urn:li:dataPlatform:vertexai," in dataset_urn
        assert "test-project.dataset.my-dataset" in dataset_urn


class TestVertexAIURIParser:
    @pytest.fixture
    def parser(self) -> VertexAIURIParser:
        return VertexAIURIParser(
            env="PROD",
            platform="vertexai",
            platform_instance=None,
        )

    @pytest.fixture
    def parser_with_platform_map(self) -> VertexAIURIParser:
        return VertexAIURIParser(
            env="PROD",
            platform="vertexai",
            platform_instance=None,
            platform_instance_map={
                "gcs": PlatformDetail(
                    platform_instance="prod-gcs",
                    env="PROD",
                    convert_urns_to_lowercase=False,
                ),
                "bigquery": PlatformDetail(
                    platform_instance="prod-bq",
                    env="PROD",
                    convert_urns_to_lowercase=True,
                ),
            },
        )

    @pytest.fixture
    def parser_with_normalization(self) -> VertexAIURIParser:
        return VertexAIURIParser(
            env="PROD",
            platform="vertexai",
            normalize_paths=True,
            partition_patterns=[r"/year=\d{4}", r"/month=\d{2}", r"/day=\d{2}"],
        )

    def test_gcs_uri_parsing(self, parser: VertexAIURIParser) -> None:
        urns = parser.dataset_urns_from_artifact_uri("gs://my-bucket/path/to/data")
        assert len(urns) == 1
        assert "urn:li:dataset:(urn:li:dataPlatform:gcs," in urns[0]
        assert "my-bucket/path/to/data" in urns[0]

    def test_bigquery_bq_protocol_parsing(self, parser: VertexAIURIParser) -> None:
        urns = parser.dataset_urns_from_artifact_uri("bq://project.dataset.table")
        assert len(urns) == 1
        assert "urn:li:dataset:(urn:li:dataPlatform:bigquery," in urns[0]
        assert "project.dataset.table" in urns[0]

    def test_bigquery_api_path_parsing(self, parser: VertexAIURIParser) -> None:
        urns = parser.dataset_urns_from_artifact_uri(
            "projects/my-proj/datasets/my-ds/tables/my-table"
        )
        assert len(urns) == 1
        assert "urn:li:dataset:(urn:li:dataPlatform:bigquery," in urns[0]
        assert "my-proj.my-ds.my-table" in urns[0]

    def test_s3_uri_parsing_all_variants(self, parser: VertexAIURIParser) -> None:
        for prefix in ["s3://", "s3a://", "s3n://"]:
            urns = parser.dataset_urns_from_artifact_uri(
                f"{prefix}my-bucket/path/to/data"
            )
            assert len(urns) == 1
            assert "urn:li:dataset:(urn:li:dataPlatform:s3," in urns[0]

    def test_azure_uri_parsing_both_protocols(self, parser: VertexAIURIParser) -> None:
        for prefix in ["wasbs://", "abfss://"]:
            uri = f"{prefix}container@account.blob.core.windows.net/path/to/data"
            urns = parser.dataset_urns_from_artifact_uri(uri)
            assert len(urns) == 1
            assert "urn:li:dataset:(urn:li:dataPlatform:abs," in urns[0]
            assert "container/path/to/data" in urns[0]

    def test_snowflake_uri_parsing(self, parser: VertexAIURIParser) -> None:
        urns = parser.dataset_urns_from_artifact_uri(
            "snowflake://account.database.schema.table"
        )
        assert len(urns) == 1
        assert "urn:li:dataset:(urn:li:dataPlatform:snowflake," in urns[0]
        assert "account.database.schema.table" in urns[0]

    def test_invalid_and_none_uris_return_empty(
        self, parser: VertexAIURIParser
    ) -> None:
        assert len(parser.dataset_urns_from_artifact_uri("not-a-valid-uri")) == 0
        assert len(parser.dataset_urns_from_artifact_uri(None)) == 0

    def test_lowercase_conversion_from_map(
        self, parser_with_platform_map: VertexAIURIParser
    ) -> None:
        urns = parser_with_platform_map.dataset_urns_from_artifact_uri(
            "bq://MyProject.MyDataset.MyTable"
        )
        assert len(urns) == 1
        assert "myproject.mydataset.mytable" in urns[0]

    def test_partition_normalization_year_month_day(
        self, parser_with_normalization: VertexAIURIParser
    ) -> None:
        urns = parser_with_normalization.dataset_urns_from_artifact_uri(
            "gs://bucket/data/year=2024/month=01/day=15/file.parquet"
        )
        assert len(urns) == 1
        assert "bucket/data/" in urns[0]
        assert "year=" not in urns[0]
        assert "month=" not in urns[0]
        assert "day=" not in urns[0]

    def test_partition_normalization_preserves_non_partitioned(
        self, parser_with_normalization: VertexAIURIParser
    ) -> None:
        urns = parser_with_normalization.dataset_urns_from_artifact_uri(
            "gs://bucket/data/regular-folder/file.parquet"
        )
        assert len(urns) == 1
        assert "bucket/data/regular-folder" in urns[0]

    def test_model_urn_from_artifact_uri(self, parser: VertexAIURIParser) -> None:
        valid_uri = "projects/my-proj/locations/us-central1/models/12345"
        urn = parser.model_urn_from_artifact_uri(valid_uri)
        assert urn is not None
        assert "urn:li:mlModel:" in urn
        assert "12345" in urn

        assert parser.model_urn_from_artifact_uri("not-a-model-uri") is None
        assert parser.model_urn_from_artifact_uri(None) is None

    def test_extract_external_uris_from_job(self, parser: VertexAIURIParser) -> None:
        mock_job = MagicMock()
        mock_job.to_dict.return_value = {
            "inputDataConfig": {
                "inputData": "gs://bucket/input/data.csv",
            },
            "outputConfig": {
                "outputPath": "gs://bucket/output/",
            },
        }
        result = parser.extract_external_uris_from_job(mock_job)
        assert isinstance(result, ArtifactURNs)
        assert len(result.input_urns) > 0
        assert len(result.output_urns) > 0
        assert any("bucket/input/data" in urn for urn in result.input_urns)
        assert any("bucket/output" in urn for urn in result.output_urns)

    def test_extract_external_uris_handles_jobs_without_uris(
        self, parser: VertexAIURIParser
    ) -> None:
        for job_config in [
            {},
            {"workerPoolSpecs": [{"containerSpec": {"args": ["--epochs=10"]}}]},
        ]:
            mock_job = MagicMock()
            mock_job.to_dict.return_value = job_config
            result = parser.extract_external_uris_from_job(mock_job)
            assert len(result.input_urns) == 0
            assert len(result.output_urns) == 0
