"""Unit tests for Dataplex entry processing."""

from threading import Lock
from typing import cast
from unittest.mock import Mock, patch

from google.cloud import dataplex_v1

from datahub.ingestion.api.report import Report
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

    def _build_processor(self) -> DataplexEntriesProcessor:
        config = DataplexConfig(project_ids=["test-project"], env="PROD")
        report = DataplexEntriesReport()
        source_report = Mock()
        catalog_client = Mock(spec=dataplex_v1.CatalogServiceClient)

        return DataplexEntriesProcessor(
            config=config,
            catalog_client=catalog_client,
            report=report,
            entry_data_by_project={},
            entry_data_lock=Lock(),
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

    def test_entries_report_serialization_preserves_sample_values(self) -> None:
        report = DataplexEntriesReport()
        report.report_entry_group("projects/p/locations/us/entryGroups/@bigquery", True)
        report.report_entry(
            "projects/p/locations/us/entryGroups/@bigquery/entries/e1",
            filtered_missing_fqn=False,
            filtered_fqn=False,
            filtered_name=False,
        )

        as_obj = Report.to_pure_python_obj(report)
        assert as_obj["entry_group_filtered_samples"]
        assert as_obj["entries_processed_samples"]

    def test_processor_initialization_sets_dependencies(self) -> None:
        processor = self._build_processor()
        assert processor.config.project_ids == ["test-project"]
        assert processor.entry_data_by_project == {}

    def test_processor_utility_methods(self) -> None:
        processor = self._build_processor()
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
            entry_data_by_project={},
            entry_data_lock=Lock(),
            source_report=Mock(),
        )

        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.fully_qualified_name = "bigquery:p.ds.table"

        assert processor.should_process_entry(entry)

        entry.fully_qualified_name = ""
        assert not processor.should_process_entry(entry)

    def test_extract_display_name_falls_back_to_last_name_segment(self) -> None:
        processor = self._build_processor()
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry_source = Mock()
        entry_source.display_name = ""
        entry.entry_source = entry_source

        assert processor._extract_display_name(entry) == "e1"

    def test_extract_display_name_falls_back_when_display_name_is_non_string(
        self,
    ) -> None:
        processor = self._build_processor()
        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry_source = Mock()
        entry_source.display_name = Mock()
        entry.entry_source = entry_source

        assert processor._extract_display_name(entry) == "e1"

    def test_collect_entries_streams_before_spanner_search(self) -> None:
        catalog_client = Mock()
        processor = DataplexEntriesProcessor(
            config=DataplexConfig(project_ids=["test-project"], env="PROD"),
            catalog_client=catalog_client,
            report=DataplexEntriesReport(),
            entry_data_by_project={},
            entry_data_lock=Lock(),
            source_report=Mock(),
        )

        entry_group = Mock(spec=dataplex_v1.EntryGroup)
        entry_group.name = "projects/p/locations/us/entryGroups/g1"

        listed_entry = Mock(spec=dataplex_v1.Entry)
        listed_entry.name = "projects/p/locations/us/entryGroups/g1/entries/e1"

        detailed_entry = Mock(spec=dataplex_v1.Entry)
        detailed_entry.name = listed_entry.name

        catalog_client.list_entries.return_value = [listed_entry]
        catalog_client.get_entry.return_value = detailed_entry
        catalog_client.search_entries.return_value = []

        with (
            patch.object(
                processor, "list_entry_groups", return_value=[entry_group]
            ) as mock_list_entry_groups,
            patch(
                "datahub.ingestion.source.dataplex.dataplex_entries.dataplex_v1.ListEntriesRequest"
            ) as mock_list_entries_request,
            patch(
                "datahub.ingestion.source.dataplex.dataplex_entries.dataplex_v1.GetEntryRequest"
            ) as mock_get_entry_request,
        ):
            mock_list_entries_request.return_value = Mock()
            mock_get_entry_request.return_value = Mock()
            entries_iter = processor.collect_entries("p", "us")
            first_entry = next(iter(entries_iter))

        assert first_entry is detailed_entry
        mock_list_entry_groups.assert_called_once_with("p", "us")
        catalog_client.search_entries.assert_not_called()

    def test_process_project_uses_entries_locations(self) -> None:
        processor = self._build_processor()
        processor.config.entries_locations = ["us", "eu"]

        with patch.object(
            processor, "process_location", side_effect=[[Mock()], [Mock()]]
        ) as process_location_mock:
            entities = list(processor.process_project("project-1"))

        assert len(entities) == 2
        assert process_location_mock.call_args_list[0].args == ("project-1", "us")
        assert process_location_mock.call_args_list[1].args == ("project-1", "eu")

    def test_process_location_filters_and_yields_entity(self) -> None:
        processor = self._build_processor()

        missing_fqn_entry = Mock(spec=dataplex_v1.Entry)
        missing_fqn_entry.name = "projects/p/locations/us/entryGroups/g/entries/missing"
        missing_fqn_entry.fully_qualified_name = ""

        filtered_by_name = Mock(spec=dataplex_v1.Entry)
        filtered_by_name.name = "projects/p/locations/us/entryGroups/g/entries/deny"
        filtered_by_name.fully_qualified_name = "bigquery:p.ds.deny"

        accepted = Mock(spec=dataplex_v1.Entry)
        accepted.name = "projects/p/locations/us/entryGroups/g/entries/allow"
        accepted.fully_qualified_name = "bigquery:p.ds.allow"

        entity = Mock()
        with (
            patch.object(
                processor,
                "collect_entries",
                return_value=[missing_fqn_entry, filtered_by_name, accepted],
            ),
            patch.object(processor, "_entry_name_allowed", side_effect=[False, True]),
            patch.object(processor, "_entry_fqn_allowed", side_effect=[True, True]),
            patch.object(
                processor, "build_project_container_for_entry", return_value=None
            ),
            patch.object(processor, "build_entity_for_entry", return_value=entity),
            patch.object(processor, "_track_entry_for_lineage") as track_lineage_mock,
        ):
            entities = list(processor.process_location("project-1", "us"))

        report = cast(DataplexEntriesReport, processor.report)
        assert entities == [entity]
        assert report.entries_seen == 3
        assert report.entries_filtered_by_missing_fqn == 1
        assert report.entries_filtered_by_pattern == 1
        assert report.entries_processed == 1
        track_lineage_mock.assert_called_once_with(
            project_id="project-1", entry=accepted
        )

    def test_collect_entries_covers_group_filtering_and_exception_paths(self) -> None:
        catalog_client = Mock()
        processor = DataplexEntriesProcessor(
            config=DataplexConfig(project_ids=["test-project"], env="PROD"),
            catalog_client=catalog_client,
            report=DataplexEntriesReport(),
            entry_data_by_project={},
            entry_data_lock=Lock(),
            source_report=Mock(),
        )

        skipped_group = Mock(spec=dataplex_v1.EntryGroup)
        skipped_group.name = "projects/p/locations/us/entryGroups/skip"
        accepted_group = Mock(spec=dataplex_v1.EntryGroup)
        accepted_group.name = "projects/p/locations/us/entryGroups/accept"

        listed_ok = Mock(spec=dataplex_v1.Entry)
        listed_ok.name = "projects/p/locations/us/entryGroups/accept/entries/ok"
        listed_fail = Mock(spec=dataplex_v1.Entry)
        listed_fail.name = "projects/p/locations/us/entryGroups/accept/entries/fail"

        detailed_ok = Mock(spec=dataplex_v1.Entry)
        detailed_ok.name = listed_ok.name
        spanner_entry = Mock(spec=dataplex_v1.Entry)
        spanner_entry.name = "projects/p/locations/us/entryGroups/@spanner/entries/e1"

        catalog_client.list_entries.return_value = [listed_ok, listed_fail]
        catalog_client.get_entry.side_effect = [detailed_ok, Exception("boom")]

        with (
            patch.object(
                processor,
                "list_entry_groups",
                return_value=[skipped_group, accepted_group],
            ),
            patch.object(
                processor,
                "should_process_entry_group",
                side_effect=[False, True],
            ),
            patch.object(
                processor, "collect_spanner_entries", return_value=[spanner_entry]
            ),
            patch(
                "datahub.ingestion.source.dataplex.dataplex_entries.dataplex_v1.ListEntriesRequest"
            ) as list_entries_request_mock,
            patch(
                "datahub.ingestion.source.dataplex.dataplex_entries.dataplex_v1.GetEntryRequest"
            ) as get_entry_request_mock,
        ):
            list_entries_request_mock.return_value = Mock()
            get_entry_request_mock.return_value = Mock()
            entries = list(processor.collect_entries("p", "us"))

        report = cast(DataplexEntriesReport, processor.report)
        assert entries == [detailed_ok, spanner_entry]
        assert report.entry_groups_seen == 2
        assert report.entry_groups_filtered == 1

    def test_collect_spanner_entries_success_and_exception(self) -> None:
        catalog_client = Mock()
        processor = DataplexEntriesProcessor(
            config=DataplexConfig(project_ids=["test-project"], env="PROD"),
            catalog_client=catalog_client,
            report=DataplexEntriesReport(),
            entry_data_by_project={},
            entry_data_lock=Lock(),
            source_report=Mock(),
        )

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.dataplex_v1.SearchEntriesRequest"
        ) as search_request_mock:
            search_request_mock.return_value = Mock()
            result_with_entry = Mock()
            result_with_entry.dataplex_entry = Mock(spec=dataplex_v1.Entry)
            result_with_entry.dataplex_entry.name = "projects/p/locations/us/entries/e1"
            result_without_entry = Mock()
            result_without_entry.dataplex_entry = None
            catalog_client.search_entries.return_value = [
                result_with_entry,
                result_without_entry,
            ]

            entries = list(processor.collect_spanner_entries("p", "us"))
            assert len(entries) == 1
            assert entries[0].name == "projects/p/locations/us/entries/e1"

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.dataplex_v1.SearchEntriesRequest"
        ) as search_request_mock:
            search_request_mock.return_value = Mock()
            catalog_client.search_entries.side_effect = Exception("search failed")
            assert list(processor.collect_spanner_entries("p", "us")) == []

    def test_build_entity_for_entry_dataset_and_container_paths(self) -> None:
        processor = self._build_processor()
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
                return_value=[],
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

    def test_build_project_container_for_entry(self) -> None:
        processor = self._build_processor()

        entry = Mock(spec=dataplex_v1.Entry)
        entry.entry_type = (
            "projects/123/locations/global/entryTypes/cloud-spanner-table"
        )
        entry.fully_qualified_name = "spanner:harshal-playground-306419.regional-us-west2.sergio-test.cymbal.Users"

        project_container = processor.build_project_container_for_entry(entry)
        assert isinstance(project_container, Container)
        assert project_container.display_name == "harshal-playground-306419"
        assert project_container.parent_container is None

    def test_process_location_emits_project_container_once_per_project(self) -> None:
        processor = self._build_processor()

        entry_one = Mock(spec=dataplex_v1.Entry)
        entry_one.name = "projects/p/locations/us/entryGroups/g/entries/one"
        entry_one.fully_qualified_name = "bigquery:p.ds.one"
        entry_two = Mock(spec=dataplex_v1.Entry)
        entry_two.name = "projects/p/locations/us/entryGroups/g/entries/two"
        entry_two.fully_qualified_name = "bigquery:p.ds.two"

        project_container = Mock()
        project_container.urn.urn.return_value = "urn:li:container:project"
        dataset_entity_one = Mock()
        dataset_entity_two = Mock()

        with (
            patch.object(
                processor, "collect_entries", return_value=[entry_one, entry_two]
            ),
            patch.object(processor, "_entry_name_allowed", return_value=True),
            patch.object(processor, "_entry_fqn_allowed", return_value=True),
            patch.object(
                processor,
                "build_project_container_for_entry",
                return_value=project_container,
            ),
            patch.object(
                processor,
                "build_entity_for_entry",
                side_effect=[dataset_entity_one, dataset_entity_two],
            ),
            patch.object(processor, "_track_entry_for_lineage"),
        ):
            entities = list(processor.process_location("project-1", "us"))

        assert entities == [project_container, dataset_entity_one, dataset_entity_two]

    def test_build_entity_for_entry_handles_invalid_and_unsupported_inputs(
        self,
    ) -> None:
        processor = self._build_processor()

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

    def test_build_entity_for_entry_dataset_without_parent_container(self) -> None:
        processor = self._build_processor()
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

    def test_extract_helpers_cover_display_description_datetime_and_group_id(
        self,
    ) -> None:
        processor = self._build_processor()

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

    def test_build_entry_container_key_warns_for_invalid_entry_type(self) -> None:
        processor = self._build_processor()

        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.entry_type = "invalid-entry-type"
        entry.parent_entry = "projects/p/locations/us/entryGroups/g/entries/parent"

        assert processor.build_entry_container_key(entry) is None
        source_report = cast(Mock, processor.source_report)
        source_report.warning.assert_called_once()

    def test_track_entry_for_lineage_warns_for_invalid_entry_type(self) -> None:
        processor = self._build_processor()

        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.entry_type = "invalid-entry-type"
        entry.fully_qualified_name = "bigquery:p.ds.table"

        processor._track_entry_for_lineage("project-1", entry)

        source_report = cast(Mock, processor.source_report)
        source_report.warning.assert_called_once()
        assert "project-1" not in processor.entry_data_by_project

    def test_build_entry_container_key_and_lineage_tracking(self) -> None:
        processor = self._build_processor()

        no_parent = Mock(spec=dataplex_v1.Entry)
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
        processor._track_entry_for_lineage("project-1", dataset_entry)
        assert "project-1" in processor.entry_data_by_project
        tracked = next(iter(processor.entry_data_by_project["project-1"]))
        assert tracked.datahub_dataset_name == "p.ds.table1"
        assert tracked.datahub_platform == "bigquery"
        assert tracked.dataplex_entry_fqn == "bigquery:p.ds.table1"

        non_dataset_entry = Mock(spec=dataplex_v1.Entry)
        non_dataset_entry.name = "projects/p/locations/us/entryGroups/g/entries/e2"
        non_dataset_entry.entry_type = (
            "projects/123/locations/global/entryTypes/bigquery-dataset"
        )
        non_dataset_entry.fully_qualified_name = "bigquery:p.ds"
        processor._track_entry_for_lineage("project-2", non_dataset_entry)
        assert "project-2" not in processor.entry_data_by_project
