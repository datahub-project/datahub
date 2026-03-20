"""Tests for Dataplex ID and name mapping utilities."""

import pytest

from datahub.ingestion.source.common.subtypes import DataplexSubTypes
from datahub.ingestion.source.dataplex.dataplex_ids import (
    extract_project_id_from_resource,
    get_datahub_platform,
    get_datahub_subtype,
    get_dataset_id_from_fqn,
)


class TestGetDatahubPlatform:
    """Tests for get_datahub_platform function."""

    @pytest.mark.parametrize(
        "system,expected_platform",
        [
            ("BIGQUERY", "bigquery"),
            ("CLOUD_PUBSUB", "pubsub"),
            ("CLOUD_STORAGE", "gcs"),
            ("CLOUD_BIGTABLE", "bigtable"),
            ("CLOUD_SPANNER", "spanner"),
            ("DATAPROC_METASTORE", "hive"),
            ("UNKNOWN_SYSTEM", None),
        ],
    )
    def test_system_to_platform_mapping(self, system, expected_platform):
        """Test mapping from Google Cloud system types to DataHub platforms."""
        assert get_datahub_platform(system) == expected_platform


class TestGetDatahubSubtype:
    """Tests for get_datahub_subtype function."""

    @pytest.mark.parametrize(
        "system,expected_subtype",
        [
            ("BIGQUERY", DataplexSubTypes.TABLE),
            ("CLOUD_PUBSUB", DataplexSubTypes.TOPIC),
            ("CLOUD_BIGTABLE", DataplexSubTypes.TABLE),
            ("CLOUD_SPANNER", DataplexSubTypes.TABLE),
            ("DATAPROC_METASTORE", DataplexSubTypes.TABLE),
            ("CLOUD_STORAGE", None),
            ("UNKNOWN_SYSTEM", None),
        ],
    )
    def test_system_to_subtype_mapping(self, system, expected_subtype):
        """Test mapping from Google Cloud system types to DataHub subtypes."""
        assert get_datahub_subtype(system) == expected_subtype


class TestExtractProjectIdFromResource:
    """Tests for extract_project_id_from_resource function."""

    @pytest.mark.parametrize(
        "resource,expected_project_id",
        [
            (
                "projects/harshal-playground-306419/datasets/test_views",
                "harshal-playground-306419",
            ),
            ("projects/acryl-staging/topics/observe-staging-obs", "acryl-staging"),
            ("projects/my-project/buckets/my-bucket", "my-project"),
            ("datasets/test_views", None),  # No 'projects/' prefix
            ("", None),  # Empty string
        ],
    )
    def test_extract_project_id(self, resource, expected_project_id):
        """Test extraction of project ID from resource strings."""
        assert extract_project_id_from_resource(resource) == expected_project_id


class TestGetDatasetIdFromFqn:
    """Tests for get_dataset_id_from_fqn function."""

    @pytest.mark.parametrize(
        "fqn,system,project_id,subtype,expected_dataset_id",
        [
            # BigQuery table tests
            (
                "bigquery:harshal-playground-306419.test_dataset.test_table",
                "BIGQUERY",
                "harshal-playground-306419",
                "table",
                "harshal-playground-306419.test_dataset.test_table",
            ),
            # BigQuery view tests
            (
                "bigquery:my-project.my_dataset.my_view",
                "BIGQUERY",
                "my-project",
                "view",
                "my-project.my_dataset.my_view",
            ),
            # BigQuery without subtype (should match any)
            (
                "bigquery:my-project.my_dataset.my_table",
                "BIGQUERY",
                "my-project",
                None,
                "my-project.my_dataset.my_table",
            ),
            # BigQuery project mismatch (should still return dataset_id with warning)
            (
                "bigquery:other-project.test_dataset.test_table",
                "BIGQUERY",
                "my-project",
                None,
                "other-project.test_dataset.test_table",
            ),
            # BigQuery invalid format (dataset only - 2 parts, needs 3)
            (
                "bigquery:harshal-playground-306419.test_views",
                "BIGQUERY",
                "harshal-playground-306419",
                None,
                None,
            ),
            # BigQuery invalid format (single part)
            ("bigquery:test_table", "BIGQUERY", "my-project", None, None),
            # Bigtable table tests
            (
                "bigtable:my-project.my-instance.my-table",
                "CLOUD_BIGTABLE",
                "my-project",
                "table",
                "my-project.my-instance.my-table",
            ),
            # Bigtable without subtype
            (
                "bigtable:my-project.my-instance.my-table",
                "CLOUD_BIGTABLE",
                "my-project",
                None,
                "my-project.my-instance.my-table",
            ),
            # Spanner table tests
            (
                "spanner:my-project.regional-us-central1.my-instance.my-db.my-table",
                "CLOUD_SPANNER",
                "my-project",
                "table",
                "my-project.regional-us-central1.my-instance.my-db.my-table",
            ),
            # Spanner view tests
            (
                "spanner:my-project.regional-us-central1.my-instance.my-db.my-view",
                "CLOUD_SPANNER",
                "my-project",
                "view",
                "my-project.regional-us-central1.my-instance.my-db.my-view",
            ),
            # Spanner without subtype (should match any)
            (
                "spanner:my-project.regional-us-central1.my-instance.my-db.resource",
                "CLOUD_SPANNER",
                "my-project",
                None,
                "my-project.regional-us-central1.my-instance.my-db.resource",
            ),
            # Pub/Sub topic tests
            (
                "pubsub:topic:acryl-staging.observe-staging-obs",
                "CLOUD_PUBSUB",
                "acryl-staging",
                None,
                "acryl-staging.observe-staging-obs",
            ),
            # Pub/Sub invalid format (missing topic: prefix)
            (
                "pubsub:acryl-staging.observe-staging-obs",
                "CLOUD_PUBSUB",
                "acryl-staging",
                None,
                None,
            ),
            # Pub/Sub invalid format (single part)
            ("pubsub:topic:my-topic", "CLOUD_PUBSUB", "my-project", None, None),
            # Pub/Sub project mismatch
            (
                "pubsub:topic:other-project.my-topic",
                "CLOUD_PUBSUB",
                "my-project",
                None,
                "other-project.my-topic",
            ),
            # GCS bucket and path (valid)
            (
                "gcs:my-bucket.path.to.file",
                "CLOUD_STORAGE",
                "my-project",
                None,
                "my-bucket.path.to.file",
            ),
            # GCS bucket only (invalid - bucket alone is a container, not a dataset)
            ("gcs:my-bucket", "CLOUD_STORAGE", "my-project", None, None),
            # Unknown system
            ("unknown:some.identifier", "UNKNOWN_SYSTEM", "my-project", None, None),
            # Invalid FQN format (no colon)
            ("bigquery_test_table", "BIGQUERY", "my-project", None, None),
        ],
    )
    def test_fqn_parsing(self, fqn, system, project_id, subtype, expected_dataset_id):
        """Test FQN parsing for various platforms and formats."""
        dataset_id = get_dataset_id_from_fqn(fqn, system, project_id, subtype)
        assert dataset_id == expected_dataset_id
