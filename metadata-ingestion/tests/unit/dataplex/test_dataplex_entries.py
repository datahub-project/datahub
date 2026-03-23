"""Unit tests for Dataplex entry processing."""

from threading import Lock
from unittest.mock import Mock, patch

from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_entries import (
    DataplexEntriesProcessor,
    DataplexEntriesReport,
)


class TestDataplexEntriesProcessorDesign:
    """Tests for DataplexEntriesProcessor interface behavior."""

    def _build_processor(self) -> DataplexEntriesProcessor:
        config = DataplexConfig(project_ids=["test-project"], env="PROD")
        report = DataplexEntriesReport()
        catalog_client = Mock(spec=dataplex_v1.CatalogServiceClient)

        return DataplexEntriesProcessor(
            config=config,
            catalog_client=catalog_client,
            report=report,
            entry_data_by_project={},
            entry_data_lock=Lock(),
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
        assert processor.build_entry_container_urn(entry) is None

    def test_should_process_entry_uses_full_entry_name_for_pattern(self) -> None:
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={
                "entries": {"dataset_pattern": {"allow": [r"^projects/.*$"]}}
            },
        )
        processor = DataplexEntriesProcessor(
            config=config,
            catalog_client=Mock(spec=dataplex_v1.CatalogServiceClient),
            report=DataplexEntriesReport(),
            entry_data_by_project={},
            entry_data_lock=Lock(),
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

    def test_collect_entries_streams_before_spanner_search(self) -> None:
        catalog_client = Mock()
        processor = DataplexEntriesProcessor(
            config=DataplexConfig(project_ids=["test-project"], env="PROD"),
            catalog_client=catalog_client,
            report=DataplexEntriesReport(),
            entry_data_by_project={},
            entry_data_lock=Lock(),
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
