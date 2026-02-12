"""Unit tests for Dataplex container generation utilities."""

import pytest

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_containers import (
    gen_bigquery_containers,
    gen_bigquery_dataset_container,
    gen_bigquery_project_container,
    track_bigquery_container,
)


class TestTrackBigQueryContainer:
    """Test track_bigquery_container function."""

    @pytest.fixture
    def config(self):
        """Create a default DataplexConfig for testing."""
        return DataplexConfig(project_ids=["test-project"], env="PROD")

    def test_track_new_project(self, config):
        """Test tracking dataset for a new project."""
        bq_containers: dict[str, set[str]] = {}

        container_urn = track_bigquery_container(
            project_id="test-project",
            dataset_id="test_dataset",
            bq_containers=bq_containers,
            config=config,
        )

        assert "test-project" in bq_containers
        assert "test_dataset" in bq_containers["test-project"]
        assert container_urn is not None
        assert "urn:li:" in container_urn

    def test_track_existing_project(self, config):
        """Test tracking dataset for an existing project."""
        bq_containers = {"test-project": {"dataset1"}}

        container_urn = track_bigquery_container(
            project_id="test-project",
            dataset_id="dataset2",
            bq_containers=bq_containers,
            config=config,
        )

        assert "test-project" in bq_containers
        assert "dataset1" in bq_containers["test-project"]
        assert "dataset2" in bq_containers["test-project"]
        assert len(bq_containers["test-project"]) == 2
        assert container_urn is not None

    def test_track_duplicate_dataset(self, config):
        """Test tracking same dataset twice."""
        bq_containers = {"test-project": {"test_dataset"}}

        container_urn = track_bigquery_container(
            project_id="test-project",
            dataset_id="test_dataset",
            bq_containers=bq_containers,
            config=config,
        )

        # Should still have only one entry (set behavior)
        assert len(bq_containers["test-project"]) == 1
        assert container_urn is not None


class TestGenBigQueryProjectContainer:
    """Test gen_bigquery_project_container function."""

    @pytest.fixture
    def config(self):
        """Create a default DataplexConfig for testing."""
        return DataplexConfig(project_ids=["test-project"], env="PROD")

    def test_generate_project_container(self, config):
        """Test generating project container."""
        work_units = list(
            gen_bigquery_project_container(project_id="test-project", config=config)
        )

        assert len(work_units) > 0
        # Should have container properties
        assert any(wu.metadata for wu in work_units)


class TestGenBigQueryDatasetContainer:
    """Test gen_bigquery_dataset_container function."""

    @pytest.fixture
    def config(self):
        """Create a default DataplexConfig for testing."""
        return DataplexConfig(project_ids=["test-project"], env="PROD")

    def test_generate_dataset_container(self, config):
        """Test generating dataset container."""
        work_units = list(
            gen_bigquery_dataset_container(
                project_id="test-project", dataset_id="test_dataset", config=config
            )
        )

        assert len(work_units) > 0
        # Should have container properties
        assert any(wu.metadata for wu in work_units)


class TestGenBigQueryContainers:
    """Test gen_bigquery_containers function."""

    @pytest.fixture
    def config(self):
        """Create a default DataplexConfig for testing."""
        return DataplexConfig(project_ids=["test-project"], env="PROD")

    def test_generate_containers_no_datasets(self, config):
        """Test generation when no datasets are present."""
        bq_containers: dict[str, set[str]] = {}

        work_units = list(
            gen_bigquery_containers(
                project_id="test-project", bq_containers=bq_containers, config=config
            )
        )

        assert len(work_units) == 0

    def test_generate_containers_single_dataset(self, config):
        """Test generation with a single dataset."""
        bq_containers = {"test-project": {"test_dataset"}}

        work_units = list(
            gen_bigquery_containers(
                project_id="test-project", bq_containers=bq_containers, config=config
            )
        )

        assert len(work_units) > 0

    def test_generate_containers_multiple_datasets(self, config):
        """Test generation with multiple datasets."""
        bq_containers = {"test-project": {"dataset1", "dataset2", "dataset3"}}

        work_units = list(
            gen_bigquery_containers(
                project_id="test-project", bq_containers=bq_containers, config=config
            )
        )

        # Should have project container + dataset containers
        assert len(work_units) > 0

    def test_generate_containers_wrong_project(self, config):
        """Test generation for project not in containers dict."""
        bq_containers: dict[str, set[str]] = {"other-project": {"dataset1"}}

        work_units = list(
            gen_bigquery_containers(
                project_id="test-project", bq_containers=bq_containers, config=config
            )
        )

        assert len(work_units) == 0

    def test_generate_containers_empty_dataset_set(self, config):
        """Test generation with empty dataset set."""
        bq_containers: dict[str, set[str]] = {"test-project": set()}

        work_units = list(
            gen_bigquery_containers(
                project_id="test-project", bq_containers=bq_containers, config=config
            )
        )

        assert len(work_units) == 0
