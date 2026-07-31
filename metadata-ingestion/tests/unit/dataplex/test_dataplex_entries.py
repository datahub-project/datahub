"""Unit tests for Dataplex entry processing (orchestration).

Per-entry-type mapping behavior lives in ``test_dataplex_mappers.py``. This
module covers the orchestration in ``DataplexEntriesProcessor``: filtering,
reporting, parallel processing, project-container dedup, and the lineage
side-channel.
"""

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, cast
from unittest.mock import Mock, patch

import pytest
from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
from datahub.ingestion.source.dataplex.dataplex_entries import (
    DataplexEntriesProcessor,
    DataplexEntriesReport,
)
from datahub.ingestion.source.dataplex.dataplex_mappers import EntryMappingResult
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset


def _make_entry(
    *, short_name: str, fqn: str, parent_entry: str = "", name: Optional[str] = None
) -> dataplex_v1.Entry:
    entry = Mock(spec=dataplex_v1.Entry)
    entry.name = name or f"projects/p/locations/us/entryGroups/g/entries/{short_name}"
    entry.entry_type = f"projects/123/locations/global/entryTypes/{short_name}"
    entry.fully_qualified_name = fqn
    entry.parent_entry = parent_entry
    entry.entry_source = None
    entry.aspects = {}
    return entry


class TestDataplexEntriesProcessorDesign:
    """Tests for DataplexEntriesProcessor interface behavior."""

    @pytest.fixture
    def processor(self) -> DataplexEntriesProcessor:
        config = DataplexConfig(project_ids=["test-project"], env="PROD")
        report = DataplexEntriesReport()
        source_report = Mock()
        catalog_client = Mock(spec=dataplex_v1.CatalogServiceClient)
        ctx = DataplexContext(config=config, credentials=None)

        return DataplexEntriesProcessor(
            config=config,
            catalog_client=catalog_client,
            report=report,
            source_report=source_report,
            ctx=ctx,
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
        assert report.entry_groups_processed == 1
        assert list(report.entry_group_processed_samples) == ["eg-pass"]

        assert report.entries_seen == 48
        assert report.entries_filtered_by_pattern == 12
        assert report.entries_filtered_by_missing_fqn == 12
        assert report.entries_filtered_by_fqn_pattern == 12
        assert report.entries_processed == 12
        assert report.entries_processed_samples.total_elements == 12
        assert len(list(report.entries_processed_samples)) == 10

    def test_should_process_entry_group_defaults_to_allow(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        assert processor.should_process_entry_group(
            "projects/p/locations/us/entryGroups/g"
        )

    def test_should_process_entry_uses_full_entry_name_for_pattern(self) -> None:
        config = DataplexConfig(
            project_ids=["test-project"],
            filter_config={"entries": {"pattern": {"allow": [r"^projects/.*$"]}}},
        )
        processor = DataplexEntriesProcessor(
            config=config,
            catalog_client=Mock(spec=dataplex_v1.CatalogServiceClient),
            report=DataplexEntriesReport(),
            source_report=Mock(),
            ctx=DataplexContext(config=config, credentials=None),
        )

        entry = Mock(spec=dataplex_v1.Entry)
        entry.name = "projects/p/locations/us/entryGroups/g/entries/e1"
        entry.fully_qualified_name = "bigquery:p.ds.table"

        assert processor.should_process_entry(entry)

        entry.fully_qualified_name = ""
        assert not processor.should_process_entry(entry)

    def test_build_entities_for_unsupported_type_returns_empty_and_warns(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entry = _make_entry(short_name="unknown-type", fqn="unknown:p.ds.t")
        assert processor._build_entities_for_entry(entry, "us") == []
        cast(Mock, processor.source_report).warning.assert_called_once()

    def test_build_entities_emits_project_container_and_dataset_with_lineage(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        entry = _make_entry(
            short_name="bigquery-table",
            fqn="bigquery:my-project.my_dataset.my_table",
            parent_entry=(
                "projects/my-project/locations/us/entryGroups/@bigquery/entries/"
                "bigquery.googleapis.com/projects/my-project/datasets/my_dataset"
            ),
        )

        results = processor._build_entities_for_entry(entry, "us-west2")

        containers = [e for e in results if isinstance(e, Container)]
        datasets = [e for e in results if isinstance(e, Dataset)]
        assert len(containers) == 1
        assert len(datasets) == 1
        # Lineage side-channel populated for the dataset entry.
        assert len(processor.entry_data) == 1
        assert processor.entry_data[0].datahub_dataset_name == (
            "my-project.my_dataset.my_table"
        )
        assert processor.entry_data[0].dataplex_location == "us-west2"

    def test_build_entities_dedups_project_container_across_entries(
        self, processor: DataplexEntriesProcessor
    ) -> None:
        first = _make_entry(
            short_name="bigquery-dataset", fqn="bigquery:my-project.dataset_a"
        )
        second = _make_entry(
            short_name="bigquery-dataset", fqn="bigquery:my-project.dataset_b"
        )

        first_results = processor._build_entities_for_entry(first, "us")
        second_results = processor._build_entities_for_entry(second, "us")

        # First entry emits project container + its own container.
        assert sum(isinstance(e, Container) for e in first_results) == 2
        # Second entry (same project) drops the already-emitted project container.
        assert len(second_results) == 1


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
            source_report=Mock(),
            ctx=DataplexContext(config=config, credentials=None),
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
        config = DataplexConfig(project_ids=["proj-1"], env="PROD")
        processor = DataplexEntriesProcessor(
            config=config,
            catalog_client=Mock(spec=dataplex_v1.CatalogServiceClient),
            report=DataplexEntriesReport(),
            source_report=Mock(),
            ctx=DataplexContext(config=config, credentials=None),
        )

        project_container = Mock()
        project_container.urn.urn.return_value = "urn:li:container:same-project"
        main_1 = Mock()
        main_2 = Mock()

        fake_mapper = Mock()
        fake_mapper.map.side_effect = [
            EntryMappingResult(
                main_entity=main_1, additional_entities=[project_container]
            ),
            EntryMappingResult(
                main_entity=main_2, additional_entities=[project_container]
            ),
        ]

        entry_1 = _make_entry(short_name="bigquery-table", fqn="bigquery:p.d.t1")
        entry_2 = _make_entry(short_name="bigquery-table", fqn="bigquery:p.d.t2")

        with patch(
            "datahub.ingestion.source.dataplex.dataplex_entries.get_entry_mapper",
            return_value=fake_mapper,
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
        assert main_1 in results
        assert main_2 in results

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
