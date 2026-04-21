"""Unit tests for Dataplex entry processing."""

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, cast
from unittest.mock import Mock, patch

import pytest
from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_entries import (
    DataplexEntriesProcessor,
    DataplexEntriesReport,
)
from datahub.ingestion.source.dataplex.dataplex_ids import DataplexCloudSqlMySqlDatabase
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset


class TestDataplexEntriesProcessorDesign:
    """Tests for DataplexEntriesProcessor interface behavior."""

    @pytest.fixture
    def processor(self) -> DataplexEntriesProcessor:
        config = DataplexConfig(project_ids=["test-project"], env="PROD")
        report = DataplexEntriesReport()
        source_report = Mock()
        catalog_client = Mock(spec=dataplex_v1.CatalogServiceClient)

        return DataplexEntriesProcessor(
            config=config,
            catalog_client=catalog_client,
            report=report,
            entry_data=[],
            source_report=source_report,
        )

    def test_entries_report_tracks_counts_and_default_sampling(self) -> None:
        report = DataplexEntriesReport()

        report.report_entry_group("eg-pass", filtered=False)
        for index in range(12):
            report.report_entry_group(f"eg-{index}", filtered=True)

        for index in range(12):
            report.report_entry(
                f"entry-{index}",
                filtered_missing_fqn=False,
                filtered_fqn=False,
                filtered_name=True,
            )

        for index in range(12):
            report.report_entry(
                f"fqn-{index}",
                filtered_missing_fqn=False,
                filtered_fqn=True,
                filtered_name=False,
            )

        for index in range(12):
            report.report_entry(
                f"missing-fqn-{index}",
                filtered_missing_fqn=True,
                filtered_fqn=False,
                filtered_name=False,
            )

        for index in range(12):
            report.report_entry(
                f"processed-{index}",
                filtered_missing_fqn=False,
                filtered_fqn=False,
                filtered_name=False,
            )

        assert report.entry_groups_seen == 13
        assert report.entry_groups_filtered == 12
        assert report.entry_group_filtered_samples.total_elements == 12
        assert len(list(report.entry_group_filtered_samples)) == 10
        assert set(report.entry_group_filtered_samples).issubset(
            {f"eg-{index}" for index in range(12)}
        )
        assert report.entry_groups_processed == 1
        assert report.entry_group_processed_samples.total_elements == 1
        assert list(report.entry_group_processed_samples) == ["eg-pass"]

        assert report.entries_seen == 48
        assert report.entries_filtered_by_pattern == 12
        assert report.entry_pattern_filtered_samples.total_elements == 12
        assert len(list(report.entry_pattern_filtered_samples)) == 10
        assert set(report.entry_pattern_filtered_samples).issubset(
            {f"entry-{index}" for index in range(12)}
        )

        assert report.entries_filtered_by_missing_fqn == 12
        assert report.entry_missing_fqn_samples.total_elements == 12
        assert len(list(report.entry_missing_fqn_samples)) == 10
        assert set(report.entry_missing_fqn_samples).issubset(
            {f"missing-fqn-{index}" for index in range(12)}
        )

        assert report.entries_filtered_by_fqn_pattern == 12
        assert report.entry_fqn_filtered_samples.total_elements == 12
        assert len(list(report.entry_fqn_filtered_samples)) == 10
        assert set(report.entry_fqn_filtered_samples).issubset(
            {f"fqn-{index}" for index in range(12)}
        )

        assert report.entries_processed == 12
        assert report.entries_processed_samples.total_elements == 12
        assert len(list(report.entries_processed_samples)) == 10
        assert set(report.entries_processed_samples).issubset(
            {f"processed-{index}" for index in range(12)}
        )

    def test_processor_utility_methods(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.entry_type = "projects/p/locations/global/entryTypes/unknown-type"
        entry.fully_qualified_name = ""
        entry.parent_entry = ""

        # No entry_groups filter configured yet -> allow all.
        assert processor.should_process_entry_group(
            "projects/p/locations/us/entryGroups/g"
        )

        # Unsupported/invalid entries should be skipped safely.
        assert processor.build_entity_for_entry(entry) is None
        assert processor.build_entry_container_key(entry) is None

    def test_should_process_entry_uses_full_entry_name_for_pattern(self) -> None:
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={"entries": {"pattern": {"allow": [r"^projects/.*$"]}}},
        )
        processor = DataplexEntriesProcessor(
            config=config,
            catalog_client=Mock(spec=dataplex_v1.CatalogServiceClient),
            report=DataplexEntriesReport(),
            entry_data=[],
            source_report=Mock(),
        )

        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.fully_qualified_name = "bigquery:p.ds.table"

        assert processor.should_process_entry(entry)

        entry.fully_qualified_name = ""
        assert not processor.should_process_entry(entry)

    def test_extract_display_name_falls_back_to_last_name_segment(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry_source = Mock()
        entry_source.display_name = ""
        entry.entry_source = entry_source

        assert processor._extract_display_name(entry) == "e1"

    def test_extract_display_name_falls_back_when_display_name_is_non_string(
        self,
        processor: DataplexEntriesProcessor,
    ) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry_source = Mock()
        entry_source.display_name = Mock()
        entry.entry_source = entry_source

        assert processor._extract_display_name(entry) == "e1"

    def test_build_entity_for_entry_dataset_and_container_paths(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        processor.config.include_schema = True

        dataset_entry = Mock(spec=dataplex_v1.Entry)
        dataset_entry.name = (
            "projects/p/locations/us/entryGroups/@bigquery/entries/"
            "bigquery.googleapis.com/projects/p/datasets/ds/tables/t"
        )
        dataset_entry.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-table"
        )
        dataset_entry.fully_qualified_name = "bigquery:p.ds.t"
        dataset_entry.parent_entry = (
            "projects/p/locations/us/entryGroups/@bigquery/entries/"
            "bigquery.googleapis.com/projects/p/datasets/ds"
        )
        dataset_entry.entry_source = None

        with (
            patch(
                "datahub.ingestion.source.dataplex.dataplex_entries.extract_entry_custom_properties",
                return_value={"k": "v"},
            ),
            patch(
                "datahub.ingestion.source.dataplex.dataplex_entries.extract_schema_from_entry_aspects",
                return_value=None,
            ),
            patch(
                "datahub.ingestion.source.dataplex.dataplex_entries.extract_graph_schema_from_entry_aspects",
                return_value=None,
            ),
        ):
            dataset_entity = processor.build_entity_for_entry(dataset_entry)

        assert isinstance(dataset_entity, Dataset)
        assert str(dataset_entity.parent_container).startswith("urn:li:container:")

        container_entry = Mock(spec=dataplex_v1.Entry)
        container_entry.name = (
            "projects/p/locations/us/entryGroups/@bigquery/entries/"
            "bigquery.googleapis.com/projects/p/datasets/ds"
        )
        container_entry.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-dataset"
        )
        container_entry.fully_qualified_name = "bigquery:p.ds"
        container_entry.parent_entry = ""
        container_entry.entry_source = None

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_entry_custom_properties",
            return_value={"k": "v"},
        ):
            container_entity = processor.build_entity_for_entry(container_entry)
        assert isinstance(container_entity, Container)
        assert str(container_entity.parent_container).startswith("urn:li:container:")

        cloudsql_database_container = Mock(spec=dataplex_v1.Entry)
        cloudsql_database_container.name = (
            "projects/p/locations/us-west2/entryGroups/@cloudsql/entries/"
            "cloudsql.googleapis.com/projects/p/locations/us-west2/instances/i/databases/d"
        )
        cloudsql_database_container.entry_type = (
            "projects/123/locations/global/entryTypes/cloudsql-mysql-database"
        )
        cloudsql_database_container.fully_qualified_name = (
            "cloudsql_mysql:p.us-west2.i.d"
        )
        cloudsql_database_container.parent_entry = (
            "projects/p/locations/us-west2/entryGroups/@cloudsql/entries/"
            "cloudsql.googleapis.com/projects/p/locations/us-west2/instances/i"
        )
        cloudsql_database_container.entry_source = None

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_entry_custom_properties",
            return_value={"k": "v"},
        ):
            cloudsql_container_entity = processor.build_entity_for_entry(
                cloudsql_database_container
            )
        assert isinstance(cloudsql_container_entity, Container)
        assert str(cloudsql_container_entity.parent_container).startswith(
            "urn:li:container:"
        )

    def test_build_project_container_for_entry(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.entry_type = (
            "projects/123/locations/global/entryTypes/cloud-spanner-table"
        )
        entry.fully_qualified_name = "spanner:harshal-playground-306419.regional-us-west2.sergio-test.cymbal.Users"

        project_container = processor.build_project_container_for_entry(entry)
        assert isinstance(project_container, Container)
        assert project_container.display_name == "harshal-playground-306419"
        assert project_container.parent_container is None

    def test_build_entity_for_entry_handles_invalid_and_unsupported_inputs(
        self,
        processor: DataplexEntriesProcessor,
    ) -> None:
        missing_fqn = Mock(spec=dataplex_v1.Entry)
        missing_fqn.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        missing_fqn.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-table"
        )
        missing_fqn.fully_qualified_name = ""
        missing_fqn.parent_entry = ""
        assert processor.build_entity_for_entry(missing_fqn) is None

        unsupported = Mock(spec=dataplex_v1.Entry)
        unsupported.name = "projects/p/locations/us/entryGroups/g/entries/e2"
        unsupported.entry_type = "projects/123/locations/global/entryTypes/unknown"
        unsupported.fully_qualified_name = "unknown:p.ds.t"
        unsupported.parent_entry = ""
        assert processor.build_entity_for_entry(unsupported) is None

        invalid_dataset_fqn = Mock(spec=dataplex_v1.Entry)
        invalid_dataset_fqn.name = "projects/p/locations/us/entryGroups/g/entries/e3"
        invalid_dataset_fqn.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-table"
        )
        invalid_dataset_fqn.fully_qualified_name = "bigquery"
        invalid_dataset_fqn.parent_entry = ""
        invalid_dataset_fqn.entry_source = None
        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_entry_custom_properties",
            return_value={},
        ):
            assert processor.build_entity_for_entry(invalid_dataset_fqn) is None

    def test_build_entity_for_entry_dataset_without_parent_container(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        processor.config.include_schema = False

        dataset_entry = Mock(spec=dataplex_v1.Entry)
        dataset_entry.name = (
            "projects/p/locations/us/entryGroups/@bigquery/entries/"
            "bigquery.googleapis.com/projects/p/datasets/ds/tables/t"
        )
        dataset_entry.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-table"
        )
        dataset_entry.fully_qualified_name = "bigquery:p.ds.t"
        dataset_entry.parent_entry = ""
        dataset_entry.entry_source = None

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_entry_custom_properties",
            return_value={"k": "v"},
        ):
            dataset_entity = processor.build_entity_for_entry(dataset_entry)

        assert isinstance(dataset_entity, Dataset)
        assert dataset_entity.parent_container is None

    def test_build_entity_for_entry_warns_when_parent_expected_but_missing(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        processor.config.include_schema = False

        dataset_entry = Mock(spec=dataplex_v1.Entry)
        dataset_entry.name = (
            "projects/p/locations/us/entryGroups/@bigquery/entries/"
            "bigquery.googleapis.com/projects/p/datasets/ds/tables/t"
        )
        dataset_entry.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-table"
        )
        dataset_entry.fully_qualified_name = "bigquery:p.ds.t"
        dataset_entry.parent_entry = ""
        dataset_entry.entry_source = None

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_entry_custom_properties",
            return_value={"k": "v"},
        ):
            dataset_entity = processor.build_entity_for_entry(dataset_entry)

        assert isinstance(dataset_entity, Dataset)
        assert dataset_entity.parent_container is None
        source_report = cast(Mock, processor.source_report)
        source_report.warning.assert_called_once()
        warning_kwargs = source_report.warning.call_args.kwargs
        assert warning_kwargs["title"] == "Missing Dataplex parent_entry"

    def test_build_entity_for_vertexai_dataset_uses_project_parent_and_display_name(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        processor.config.include_schema = False

        dataset_entry = Mock(spec=dataplex_v1.Entry)
        dataset_entry.name = (
            "projects/p/locations/us-west2/entryGroups/@vertexai/entries/"
            "aiplatform.googleapis.com/projects/p/locations/us-west2/datasets/5135361416504541184"
        )
        dataset_entry.entry_type = (
            "projects/123/locations/global/entryTypes/vertexai-dataset"
        )
        dataset_entry.fully_qualified_name = (
            "vertex_ai:dataset:p.us-west2.5135361416504541184"
        )
        dataset_entry.parent_entry = ""
        dataset_entry.entry_source = Mock()
        dataset_entry.entry_source.display_name = "sergio-dataplex-test"
        dataset_entry.entry_source.description = ""
        dataset_entry.entry_source.create_time = None
        dataset_entry.entry_source.update_time = None

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_entry_custom_properties",
            return_value={"k": "v"},
        ):
            dataset_entity = processor.build_entity_for_entry(dataset_entry)

        assert isinstance(dataset_entity, Dataset)
        assert dataset_entity.urn.urn() == (
            "urn:li:dataset:(urn:li:dataPlatform:vertexai,"
            "p.us-west2.5135361416504541184,PROD)"
        )
        assert dataset_entity.display_name == "sergio-dataplex-test"
        assert dataset_entity.parent_container is not None
        assert str(dataset_entity.parent_container).startswith("urn:li:container:")

    def test_build_entity_for_pubsub_topic_uses_project_parent_container(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        processor.config.include_schema = False

        dataset_entry = Mock(spec=dataplex_v1.Entry)
        dataset_entry.name = (
            "projects/p/locations/us-west2/entryGroups/@pubsub/entries/"
            "pubsub.googleapis.com/projects/acryl-staging/topics/observe-staging-obs"
        )
        dataset_entry.entry_type = (
            "projects/123/locations/global/entryTypes/pubsub-topic"
        )
        dataset_entry.fully_qualified_name = (
            "pubsub:topic:acryl-staging.observe-staging-obs"
        )
        dataset_entry.parent_entry = ""
        dataset_entry.entry_source = None

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.extract_entry_custom_properties",
            return_value={"k": "v"},
        ):
            dataset_entity = processor.build_entity_for_entry(dataset_entry)

        assert isinstance(dataset_entity, Dataset)
        assert dataset_entity.urn.urn() == (
            "urn:li:dataset:(urn:li:dataPlatform:pubsub,"
            "acryl-staging.observe-staging-obs,PROD)"
        )
        assert dataset_entity.parent_container is not None
        assert str(dataset_entity.parent_container).startswith("urn:li:container:")

    def test_extract_helpers_cover_display_description_datetime_and_group_id(
        self,
        processor: DataplexEntriesProcessor,
    ) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.entry_source = Mock()
        entry.entry_source.display_name = "Display"
        entry.entry_source.description = "Description"
        ts = Mock()
        ts.timestamp.return_value = 123.0
        entry.entry_source.create_time = ts
        entry.entry_source.update_time = ts

        assert processor._extract_display_name(entry) == "Display"
        assert processor._extract_description(entry) == "Description"
        assert processor._extract_entry_group_id(entry.name) == "g"
        assert processor._extract_datetime(entry, "create_time") is not None
        assert processor._extract_datetime(entry, "update_time") is not None

        no_source_entry = Mock(spec=dataplex_v1.Entry)
        no_source_entry.name = "projects/p/locations/us/entries/e2"
        no_source_entry.entry_source = None
        assert processor._extract_description(no_source_entry) == ""
        assert processor._extract_datetime(no_source_entry, "create_time") is None
        assert processor._extract_entry_group_id(no_source_entry.name) == "unknown"

    def test_build_entry_container_key_warns_for_invalid_entry_type(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.entry_type = "invalid-entry-type"
        entry.parent_entry = "projects/p/locations/us/entryGroups/g/entries/parent"

        assert processor.build_entry_container_key(entry) is None
        source_report = cast(Mock, processor.source_report)
        source_report.warning.assert_called_once()

    def test_track_entry_for_lineage_warns_for_invalid_entry_type(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.entry_type = "invalid-entry-type"
        entry.fully_qualified_name = "bigquery:p.ds.table"

        processor._track_entry_for_lineage("us", entry)

        source_report = cast(Mock, processor.source_report)
        source_report.warning.assert_called_once()
        assert len(processor.entry_data) == 0

    def test_build_entry_container_key_and_lineage_tracking(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        no_parent = Mock(spec=dataplex_v1.Entry)
        no_parent.name = "projects/p/locations/us/entryGroups/g/entries/no-parent"
        no_parent.entry_type = "projects/123/locations/global/entryTypes/bigquery-table"
        no_parent.parent_entry = ""
        assert processor.build_entry_container_key(no_parent) is None

        with_parent = Mock(spec=dataplex_v1.Entry)
        with_parent.entry_type = (
            "projects/123/locations/global/entryTypes/cloudsql-mysql-table"
        )
        with_parent.parent_entry = (
            "projects/p/locations/us-west2/entryGroups/@cloudsql/entries/"
            "cloudsql.googleapis.com/projects/p/locations/us-west2/instances/i/databases/d"
        )
        parent_key = processor.build_entry_container_key(with_parent)
        assert isinstance(parent_key, DataplexCloudSqlMySqlDatabase)
        assert parent_key.as_urn().startswith("urn:li:container:")

        dataset_entry = Mock(spec=dataplex_v1.Entry)
        dataset_entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        dataset_entry.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-table"
        )
        dataset_entry.fully_qualified_name = "bigquery:p.ds.table1"
        processor._track_entry_for_lineage("us", dataset_entry)
        assert len(processor.entry_data) == 1
        tracked = processor.entry_data[0]
        assert tracked.datahub_dataset_name == "p.ds.table1"
        assert tracked.datahub_platform == "bigquery"
        assert tracked.dataplex_entry_fqn == "bigquery:p.ds.table1"

        non_dataset_entry = Mock(spec=dataplex_v1.Entry)
        non_dataset_entry.name = "projects/p/locations/us/entryGroups/g/entries/e2"
        non_dataset_entry.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-dataset"
        )
        non_dataset_entry.fully_qualified_name = "bigquery:p.ds"
        processor._track_entry_for_lineage("us", non_dataset_entry)
        assert len(processor.entry_data) == 1


class TestDataplexParallelEntries:
    """Tests for the parallel entry processing path (process_entries)."""

    @pytest.fixture
    def processor(self) -> DataplexEntriesProcessor:
        config = DataplexConfig(
            project_ids=["proj-1"],
            entries_locations=["us"],
            env="PROD",
        )
        return DataplexEntriesProcessor(
            config=config,
            catalog_client=Mock(spec=dataplex_v1.CatalogServiceClient),
            report=DataplexEntriesReport(),
            entry_data=[],
            source_report=Mock(),
        )

    def test_process_entries_collects_all_entries(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entity_a = Mock()
        entity_b = Mock()

        with (
            patch.object(
                processor, "_list_entry_stubs", return_value=["entry-a", "entry-b"]
            ),
            patch.object(
                processor,
                "_fetch_and_build_entry",
                side_effect=[[entity_a], [entity_b]],
            ),
            patch.object(processor, "_process_spanner_entries", return_value=[]),
        ):
            entities = list(
                processor.process_entries(project_ids=["proj-1"], max_workers=2)
            )

        assert set(entities) == {entity_a, entity_b}

    def test_process_entries_includes_spanner_entities(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        spanner_entity = Mock()

        with (
            patch.object(processor, "_list_entry_stubs", return_value=[]),
            patch.object(
                processor, "_process_spanner_entries", return_value=[spanner_entity]
            ),
        ):
            entities = list(
                processor.process_entries(project_ids=["proj-1"], max_workers=2)
            )

        assert spanner_entity in entities

    def test_process_entries_handles_fetch_failure(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entity_ok = Mock()

        with (
            patch.object(
                processor,
                "_list_entry_stubs",
                return_value=["entry-ok", "entry-fail"],
            ),
            patch.object(
                processor,
                "_fetch_and_build_entry",
                side_effect=[[entity_ok], Exception("fetch failed")],
            ),
            patch.object(processor, "_process_spanner_entries", return_value=[]),
        ):
            entities = list(
                processor.process_entries(project_ids=["proj-1"], max_workers=2)
            )

        assert entity_ok in entities
        source_report = cast(Mock, processor.source_report)
        source_report.warning.assert_called_once()

    def test_process_entries_skips_failing_project_location_pair(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        """A PermissionDenied on one (project, location) must not abort the others."""
        entity_ok = Mock()

        def stub_side_effect(project_id: str, location: str) -> List[str]:
            if project_id == "proj-fail":
                raise PermissionError("403 PermissionDenied")
            return ["entry-ok"]

        with (
            patch.object(processor, "_list_entry_stubs", side_effect=stub_side_effect),
            patch.object(
                processor,
                "_fetch_and_build_entry",
                return_value=[entity_ok],
            ),
            patch.object(processor, "_process_spanner_entries", return_value=[]),
        ):
            entities = list(
                processor.process_entries(
                    project_ids=["proj-fail", "proj-ok"], max_workers=2
                )
            )

        assert entity_ok in entities
        source_report = cast(Mock, processor.source_report)
        source_report.warning.assert_called_once()

    def test_build_entities_deduplicates_project_container_across_threads(
        self,
    ) -> None:
        """Two parallel workers encountering the same project container emit it once."""
        processor = DataplexEntriesProcessor(
            config=DataplexConfig(project_ids=["proj-1"], env="PROD"),
            catalog_client=Mock(spec=dataplex_v1.CatalogServiceClient),
            report=DataplexEntriesReport(),
            entry_data=[],
            source_report=Mock(),
        )

        project_container = Mock()
        project_container.urn.urn.return_value = "urn:li:container:same-project"
        entity_1 = Mock()
        entity_2 = Mock()
        entry_1 = Mock(spec=dataplex_v1.Entry)
        entry_2 = Mock(spec=dataplex_v1.Entry)

        with (
            patch.object(
                processor,
                "build_project_container_for_entry",
                return_value=project_container,
            ),
            patch.object(
                processor,
                "build_entity_for_entry",
                side_effect=[entity_1, entity_2],
            ),
            patch.object(processor, "_track_entry_for_lineage"),
        ):
            barrier = threading.Barrier(2)

            def call_build(entry: dataplex_v1.Entry) -> list:
                barrier.wait()
                return processor._build_entities_for_entry(entry, "us")

            with ThreadPoolExecutor(max_workers=2) as executor:
                f1 = executor.submit(call_build, entry_1)
                f2 = executor.submit(call_build, entry_2)
            results = f1.result() + f2.result()

        containers = [e for e in results if e is project_container]
        assert len(containers) == 1
        assert entity_1 in results
        assert entity_2 in results

    def test_report_thread_safety(self) -> None:
        """Concurrent calls to report methods produce consistent counter totals."""
        report = DataplexEntriesReport()
        n_threads = 50
        barrier = threading.Barrier(n_threads)

        def bump() -> None:
            barrier.wait()
            report.report_entry_group("eg", filtered=True)

        threads = [threading.Thread(target=bump) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert report.entry_groups_seen == n_threads
        assert report.entry_groups_filtered == n_threads
