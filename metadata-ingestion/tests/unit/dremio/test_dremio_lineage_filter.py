"""Pin the contract that query/view lineage cannot emit URNs the catalog
walk would have dropped — otherwise lineage edges create ghost datasets."""

from datetime import datetime
from unittest.mock import Mock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioCatalog,
    DremioQuery,
)
from datahub.ingestion.source.dremio.dremio_source import (
    DREMIO_DATABASE_NAME,
    DremioSource,
    passes_dremio_filters,
)


class TestPassesDremioFilters:
    def test_in_catalog_short_circuits_all_other_gates(self):
        # Catalog membership wins over restrictive patterns.
        assert passes_dremio_filters(
            name="myspace.folder.table1",
            catalog_dataset_names={"myspace.folder.table1"},
            dataset_pattern=AllowDenyPattern(allow=["nothing_matches"]),
            schema_pattern=AllowDenyPattern(allow=["nothing_matches"]),
        )

    def test_strips_dremio_prefix_before_matching(self):
        # SqlParsingAggregator hands names through with the platform prefix.
        assert passes_dremio_filters(
            name=f"{DREMIO_DATABASE_NAME}.myspace.folder.table1",
            catalog_dataset_names={"myspace.folder.table1"},
            dataset_pattern=AllowDenyPattern.allow_all(),
            schema_pattern=AllowDenyPattern.allow_all(),
        )

    def test_accelerator_reflection_rejected(self):
        assert not passes_dremio_filters(
            name="_accelerator_.reflection_id.something",
            catalog_dataset_names=set(),
            dataset_pattern=AllowDenyPattern.allow_all(),
            schema_pattern=AllowDenyPattern.allow_all(),
        )

    def test_dataset_pattern_deny_blocks_emission(self):
        assert not passes_dremio_filters(
            name="myspace.folder.temp_table",
            catalog_dataset_names=set(),
            dataset_pattern=AllowDenyPattern(allow=[".*"], deny=[".*temp.*"]),
            schema_pattern=AllowDenyPattern.allow_all(),
        )

    def test_schema_pattern_deny_blocks_emission(self):
        assert not passes_dremio_filters(
            name="other_space.folder.table",
            catalog_dataset_names=set(),
            dataset_pattern=AllowDenyPattern.allow_all(),
            schema_pattern=AllowDenyPattern(allow=["^myspace.*"]),
        )

    def test_schema_pattern_allow_admits_emission(self):
        assert passes_dremio_filters(
            name="myspace.folder.table",
            catalog_dataset_names=set(),
            dataset_pattern=AllowDenyPattern.allow_all(),
            schema_pattern=AllowDenyPattern(allow=["^myspace.*"]),
        )

    def test_single_segment_name_skips_schema_check(self):
        # No container segment → schema_pattern doesn't apply, dataset_pattern does.
        assert passes_dremio_filters(
            name="standalone",
            catalog_dataset_names=set(),
            dataset_pattern=AllowDenyPattern.allow_all(),
            schema_pattern=AllowDenyPattern(allow=["^nothing.*"]),
        )


class TestDremioSourceLineageFilter:
    @pytest.fixture
    def mock_config(self):
        return DremioSourceConfig(
            hostname="test-host",
            port=9047,
            tls=False,
            username="test-user",
            password="test-password",
            schema_pattern=AllowDenyPattern(allow=["^myspace.*"]),
            dataset_pattern=AllowDenyPattern(allow=[".*"], deny=[".*temp.*"]),
        )

    @pytest.fixture
    def source(self, mock_config, monkeypatch):
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200

        mock_ctx = Mock()
        mock_ctx.run_id = "test-run-id"
        mock_ctx.graph = None

        src = DremioSource(mock_config, mock_ctx)
        src.dremio_catalog = Mock(spec=DremioCatalog)
        src.dremio_catalog.dremio_api = Mock()

        src.catalog_dataset_names = {
            "myspace.folder.allowed_table",
            "myspace.folder.allowed_view",
        }

        # Spy aggregator — assert calls without the real SQL parser.
        src.sql_parsing_aggregator = Mock()
        return src

    def test_is_allowed_table_accepts_catalog_entry(self, source):
        assert source._is_allowed_table("myspace.folder.allowed_table")

    def test_is_allowed_table_rejects_filtered_schema(self, source):
        before = source.report.lineage_dropped_filtered
        assert not source._is_allowed_table("other_space.folder.x")
        assert source.report.lineage_dropped_filtered == before + 1

    def test_is_allowed_table_rejects_dataset_pattern_deny(self, source):
        # Container allowed, dataset name denied.
        before = source.report.lineage_dropped_filtered
        assert not source._is_allowed_table("myspace.folder.staging_temp_data")
        assert source.report.lineage_dropped_filtered == before + 1

    def test_is_allowed_table_rejects_reflection(self, source):
        before = source.report.lineage_dropped_filtered
        assert not source._is_allowed_table("_accelerator_.reflection_id.some_view")
        assert source.report.lineage_dropped_filtered == before + 1

    def test_process_query_drops_filtered_upstream(self, source):
        query = Mock(spec=DremioQuery)
        query.job_id = "job1"
        query.query = (
            "SELECT * FROM other_space.folder.filtered "
            "UNION ALL SELECT * FROM myspace.folder.allowed_table"
        )
        query.affected_dataset = "myspace.folder.result"
        query.queried_datasets = [
            "other_space.folder.filtered",
            "myspace.folder.allowed_table",
        ]
        query.username = "u"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        before_dropped = source.report.lineage_dropped_filtered
        source.process_query(query)

        source.sql_parsing_aggregator.add_known_query_lineage.assert_called_once()
        info = source.sql_parsing_aggregator.add_known_query_lineage.call_args[0][0]
        assert len(info.upstreams) == 1
        assert "myspace.folder.allowed_table" in info.upstreams[0]
        assert "other_space.folder.filtered" not in info.upstreams[0]

        # Observed query always registers; aggregator gates usage via is_allowed_table.
        source.sql_parsing_aggregator.add_observed_query.assert_called_once()

        assert source.report.lineage_dropped_filtered == before_dropped + 1

    def test_process_query_skips_known_lineage_when_downstream_filtered(self, source):
        query = Mock(spec=DremioQuery)
        query.job_id = "job2"
        query.query = (
            "INSERT INTO other_space.result SELECT * FROM myspace.folder.allowed_table"
        )
        query.affected_dataset = "other_space.result"
        query.queried_datasets = ["myspace.folder.allowed_table"]
        query.username = "u"
        query.submitted_ts = datetime(2024, 1, 1, 12, 0, 0)

        source.process_query(query)

        source.sql_parsing_aggregator.add_known_query_lineage.assert_not_called()
        source.sql_parsing_aggregator.add_observed_query.assert_called_once()

    def test_generate_view_lineage_filters_parents(self, source):
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:dremio,"
            "dremio.myspace.folder.allowed_view,PROD)"
        )
        parents = [
            "myspace.folder.allowed_table",
            "other_space.folder.filtered",  # blocked by schema_pattern
            "myspace.folder.staging_temp_data",  # blocked by dataset_pattern
        ]

        before_dropped = source.report.lineage_dropped_filtered
        workunits = list(source.generate_view_lineage(dataset_urn, parents))

        assert len(workunits) == 1
        upstreams = workunits[0].metadata.aspect.upstreams
        assert len(upstreams) == 1
        assert "myspace.folder.allowed_table" in upstreams[0].dataset

        assert source.sql_parsing_aggregator.add_known_lineage_mapping.call_count == 1
        assert source.report.lineage_dropped_filtered == before_dropped + 2

    def test_generate_view_lineage_emits_nothing_when_all_parents_filtered(
        self, source
    ):
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:dremio,"
            "dremio.myspace.folder.allowed_view,PROD)"
        )
        parents = [
            "other_space.folder.a",
            "other_space.folder.b",
        ]

        workunits = list(source.generate_view_lineage(dataset_urn, parents))

        assert workunits == []
        source.sql_parsing_aggregator.add_known_lineage_mapping.assert_not_called()
