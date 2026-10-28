from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    QueryEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryDataset,
    BigqueryTableSnapshot,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BigQueryFilter,
    BigQueryIdentifierBuilder,
)
from datahub.ingestion.source.bigquery_v2.lineage import (
    BigqueryLineageExtractor,
    LineageEdge,
    LineageEdgeColumnMapping,
    _merge_lineage_edge_columns,
)
from datahub.metadata.schema_classes import UpstreamLineageClass
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.file_backed_collections import FileBackedDict
from tests.unit.bigquery.test_bigquery_audit_log_api import (
    DATASET,
    PROJECT,
    FakeLogEntry,
    bq_ref,
    exported_row,
    table_ref,
    v1_query_entry,
    v2_job_metadata,
    v2_query_entry,
)

START = datetime(2023, 1, 1, tzinfo=timezone.utc)
END = datetime(2023, 1, 2, tzinfo=timezone.utc)


def make_extractor(
    config: BigQueryV2Config,
    redundant_run_skip_handler: Optional[MagicMock] = None,
) -> BigqueryLineageExtractor:
    report = BigQueryV2Report()
    return BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
        filters=BigQueryFilter(config, report),
        redundant_run_skip_handler=redundant_run_skip_handler,
    )


def query_event(
    destination: Optional[str],
    referenced_tables: Sequence[str] = (),
    referenced_views: Sequence[str] = (),
    query: str = "testQuery",
    statement_type: str = "SELECT",
) -> QueryEvent:
    return QueryEvent(
        timestamp=START,
        actor_email="user@example.com",
        query=query,
        statementType=statement_type,
        project_id=PROJECT,
        end_time=START,
        destinationTable=bq_ref(destination) if destination else None,
        referencedTables=[bq_ref(t) for t in referenced_tables],
        referencedViews=[bq_ref(v) for v in referenced_views],
    )


def upstream_tables(edges: Set[LineageEdge]) -> Set[str]:
    return {edge.table for edge in edges}


def upstream_lineage_by_urn(
    workunits: Sequence[MetadataWorkUnit],
) -> Dict[str, UpstreamLineageClass]:
    result: Dict[str, UpstreamLineageClass] = {}
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, UpstreamLineageClass
        ):
            assert wu.metadata.entityUrn
            result[wu.metadata.entityUrn] = wu.metadata.aspect
    return result


def urn_for(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{PROJECT}.{DATASET}.{table},PROD)"


class TestCreateLineageMap:
    def test_events_without_destination_or_outside_filters_are_skipped(self) -> None:
        config = BigQueryV2Config(
            table_pattern={"deny": [".*\\.denied_table"]},
            lineage_use_sql_parser=False,
        )
        extractor = make_extractor(config)

        lineage = extractor._create_lineage_map(
            [
                query_event(None, referenced_tables=["src"]),
                query_event("denied_table", referenced_tables=["src"]),
            ]
        )

        assert lineage == {}
        assert extractor.report.num_skipped_lineage_entries_missing_data[PROJECT] == 1
        assert extractor.report.num_skipped_lineage_entries_not_allowed[PROJECT] == 1

    def test_audit_log_references_are_used_when_sql_parser_is_disabled(self) -> None:
        config = BigQueryV2Config(
            lineage_use_sql_parser=False, upstream_lineage_in_report=True
        )
        extractor = make_extractor(config)
        event = query_event(
            "dest", referenced_tables=["src", "dest"], referenced_views=["view"]
        )

        lineage = extractor._create_lineage_map([event])

        edges = lineage[table_ref("dest")]
        # The destination referencing itself must not become a self-edge.
        assert upstream_tables(edges) == {table_ref("src"), table_ref("view")}
        assert all(edge.column_confidence == 0.1 for edge in edges)

        upstream = extractor.get_lineage_for_table(
            bq_table=bq_ref("dest"),
            bq_table_urn=urn_for("dest"),
            lineage_metadata=lineage,
        )
        assert upstream is not None
        assert {u.dataset for u in upstream.upstreams} == {
            urn_for("src"),
            urn_for("view"),
        }
        assert extractor.report.upstream_lineage[table_ref("dest")] == {
            table_ref("src"),
            table_ref("view"),
        }

    def test_event_without_parseable_sql_or_references_is_skipped(self) -> None:
        extractor = make_extractor(BigQueryV2Config())

        lineage = extractor._create_lineage_map([query_event("dest")])

        assert lineage == {}
        assert extractor.report.num_skipped_lineage_entries_missing_data[PROJECT] == 1

    def test_sql_parser_result_overrides_indirect_audit_log_references(self) -> None:
        extractor = make_extractor(BigQueryV2Config())
        # BigQuery lists the base table of a view in referencedTables; the
        # parsed SQL only references the view, which is the lineage we want.
        event = query_event(
            "dest",
            referenced_tables=["view_base_table"],
            referenced_views=["view"],
            query=f"SELECT id, name FROM `{PROJECT}.{DATASET}.view`",
            statement_type="SELECT",
        )

        lineage = extractor._create_lineage_map([event])

        edges = lineage[table_ref("dest")]
        assert upstream_tables(edges) == {table_ref("view")}
        (edge,) = edges
        assert {m.out_column: set(m.in_columns) for m in edge.column_mapping} == {
            "id": {"id"},
            "name": {"name"},
        }
        assert edge.column_confidence > 0.1

    def test_unparseable_sql_falls_back_to_audit_log_references(self) -> None:
        extractor = make_extractor(BigQueryV2Config())
        event = query_event(
            "dest",
            referenced_tables=["src"],
            query="this is not sql at all",
            statement_type="SCRIPT",
        )

        lineage = extractor._create_lineage_map([event])

        assert upstream_tables(lineage[table_ref("dest")]) == {table_ref("src")}
        assert extractor.report.num_lineage_entries_sql_parser_failure[PROJECT] == 1


class TestAuditLogParsing:
    def test_sharded_references_collapse_into_one_upstream(self) -> None:
        extractor = make_extractor(
            BigQueryV2Config(
                lineage_use_sql_parser=False, enable_legacy_sharded_table_support=False
            )
        )
        entries = [
            v1_query_entry(destination="daily", referenced_tables=["events_20230101"]),
            v1_query_entry(destination="daily", referenced_tables=["events_20230102"]),
        ]

        events = [extractor._parse_bigquery_log_entries(e) for e in entries]
        lineage = extractor._create_lineage_map([e for e in events if e])

        assert upstream_tables(lineage[table_ref("daily")]) == {
            table_ref("events_yyyymmdd")
        }

    def test_v1_v2_and_unknown_log_entries(self) -> None:
        extractor = make_extractor(BigQueryV2Config())

        v1 = extractor._parse_bigquery_log_entries(v1_query_entry(destination="a"))
        v2 = extractor._parse_bigquery_log_entries(v2_query_entry(destination="b"))
        unknown = extractor._parse_bigquery_log_entries(
            FakeLogEntry(payload={"foo": "bar"})
        )

        assert v1 is not None and v1.destinationTable == bq_ref("a")
        assert v2 is not None and v2.destinationTable == bq_ref("b")
        assert unknown is None

    def test_exported_rows(self) -> None:
        extractor = make_extractor(BigQueryV2Config())

        event = extractor._parse_exported_bigquery_audit_metadata(
            exported_row(v2_job_metadata(destination="a", referenced_tables=["s"]))
        )
        not_a_job = extractor._parse_exported_bigquery_audit_metadata(
            exported_row({"tableDataRead": {"fields": []}})
        )

        assert event is not None and event.referencedTables == [bq_ref("s")]
        assert not_a_job is None

    def test_gcp_logging_entries_are_counted_and_fetched_with_padded_window(
        self,
    ) -> None:
        config = BigQueryV2Config(
            start_time=START, end_time=END, max_query_duration=timedelta(hours=1)
        )
        extractor = make_extractor(config)
        logging_client = MagicMock()
        broken = v1_query_entry(destination="a")
        del broken.payload["serviceData"]["jobCompletedEvent"]["job"][
            "jobConfiguration"
        ]
        logging_client.list_entries.return_value = [
            v1_query_entry(destination="a"),
            FakeLogEntry(payload={"unrelated": True}),
            broken,
        ]

        with patch.object(
            BigQueryV2Config, "make_gcp_logging_client", return_value=logging_client
        ):
            events = list(extractor._get_parsed_audit_log_events(PROJECT))

        assert [e.destinationTable for e in events] == [bq_ref("a")]
        assert extractor.report.num_lineage_total_log_entries[PROJECT] == 3
        assert extractor.report.num_lineage_parsed_log_entries[PROJECT] == 1
        assert extractor.report.num_lineage_log_parse_failures[PROJECT] == 1
        # The window is widened so delayed job-completion events still join.
        assert extractor.report.log_entry_start_time == START - timedelta(hours=1)
        assert extractor.report.log_entry_end_time == END + timedelta(hours=1)
        log_filter = logging_client.list_entries.call_args.kwargs["filter_"]
        assert "2022-12-31T23:00:00Z" in log_filter
        assert "2023-01-02T01:00:00Z" in log_filter


class TestLineageWorkunits:
    def test_exported_audit_metadata_emits_lineage_only_for_requested_tables(
        self,
    ) -> None:
        config = BigQueryV2Config(
            use_exported_bigquery_audit_metadata=True,
            bigquery_audit_metadata_datasets=[f"{PROJECT}.audit"],
            lineage_use_sql_parser=False,
            start_time=START,
            end_time=END,
        )
        extractor = make_extractor(config)
        bq_client = MagicMock()
        bq_client.query.return_value = [
            exported_row(v2_job_metadata(destination="t1", referenced_tables=["src"])),
            exported_row(v2_job_metadata(destination="view", referenced_tables=["s2"])),
            exported_row(
                v2_job_metadata(destination="unknown", referenced_tables=["s3"])
            ),
        ]
        # Views get their lineage from their definition, not from the audit log.
        extractor.datasets_skip_audit_log_lineage.add(table_ref("view"))

        with patch.object(
            BigQueryV2Config, "get_bigquery_client", return_value=bq_client
        ):
            workunits = list(
                extractor.get_lineage_workunits(
                    projects=["ignored-when-exported"],
                    table_refs={table_ref("t1"), table_ref("view")},
                )
            )

        lineage = upstream_lineage_by_urn(workunits)
        assert set(lineage) == {urn_for("t1")}
        assert [u.dataset for u in lineage[urn_for("t1")].upstreams] == [urn_for("src")]
        assert extractor.report.lineage_metadata_entries == {"*": 3}

    def test_failed_project_is_reported_and_other_projects_continue(self) -> None:
        extractor = make_extractor(BigQueryV2Config(lineage_use_sql_parser=False))
        good_client = MagicMock()
        good_client.list_entries.return_value = [
            v1_query_entry(destination="t1", referenced_tables=["src"])
        ]

        def logging_client_for(project_id: str) -> MagicMock:
            if project_id == "broken-project":
                raise RuntimeError("logging api disabled")
            return good_client

        with patch.object(
            BigQueryV2Config, "make_gcp_logging_client", side_effect=logging_client_for
        ):
            workunits = list(
                extractor.get_lineage_workunits(
                    projects=["broken-project", PROJECT], table_refs={table_ref("t1")}
                )
            )

        assert list(extractor.report.lineage_failed_extraction) == ["broken-project"]
        assert set(upstream_lineage_by_urn(workunits)) == {urn_for("t1")}

    def test_redundant_run_is_skipped_without_touching_checkpoint(self) -> None:
        handler = MagicMock()
        handler.suggest_run_time_window.return_value = (START, END)
        handler.should_skip_this_run.return_value = True
        extractor = make_extractor(
            BigQueryV2Config(), redundant_run_skip_handler=handler
        )

        workunits = list(extractor.get_lineage_workunits([PROJECT], {table_ref("t1")}))

        assert workunits == []
        handler.update_state.assert_not_called()
        assert len(extractor.report.warnings) == 1

    def test_views_and_snapshots_get_lineage_from_definitions(self) -> None:
        extractor = make_extractor(BigQueryV2Config())
        view_definitions = FileBackedDict[str]()
        view_definitions[table_ref("my_view")] = (
            f"SELECT id FROM `{PROJECT}.{DATASET}.base_table`"
        )
        snapshots = FileBackedDict[BigqueryTableSnapshot]()
        snapshots[table_ref("my_snapshot")] = BigqueryTableSnapshot(
            name="my_snapshot",
            comment=None,
            created=None,
            last_altered=None,
            size_in_bytes=None,
            rows_count=None,
            base_table_identifier=BigqueryTableIdentifier(
                PROJECT, DATASET, "base_table"
            ),
        )

        workunits = list(
            extractor.get_lineage_workunits_for_views_and_snapshots(
                projects=[PROJECT],
                view_refs_by_project={PROJECT: {table_ref("my_view")}},
                view_definitions=view_definitions,
                snapshot_refs_by_project={PROJECT: {table_ref("my_snapshot")}},
                snapshots_by_ref=snapshots,
            )
        )

        lineage = upstream_lineage_by_urn(workunits)
        assert [u.dataset for u in lineage[urn_for("my_view")].upstreams] == [
            urn_for("base_table")
        ]
        assert [u.dataset for u in lineage[urn_for("my_snapshot")].upstreams] == [
            urn_for("base_table")
        ]
        # Audit-log lineage must not later overwrite these.
        assert extractor.datasets_skip_audit_log_lineage == {
            table_ref("my_view"),
            table_ref("my_snapshot"),
        }


class TestCapabilityProbe:
    def test_exported_probe_requests_a_single_row(self) -> None:
        config = BigQueryV2Config(
            use_exported_bigquery_audit_metadata=True,
            bigquery_audit_metadata_datasets=[f"{PROJECT}.audit"],
        )
        extractor = make_extractor(config)
        bq_client = MagicMock()
        bq_client.query.return_value = []

        with patch.object(
            BigQueryV2Config, "get_bigquery_client", return_value=bq_client
        ):
            extractor.test_capability(PROJECT)

        assert "limit 1" in bq_client.query.call_args.args[0]

    def test_gcp_logging_probe_requests_a_single_entry(self) -> None:
        extractor = make_extractor(BigQueryV2Config())
        logging_client = MagicMock()
        logging_client.list_entries.return_value = []

        with patch.object(
            BigQueryV2Config, "make_gcp_logging_client", return_value=logging_client
        ):
            extractor.test_capability(PROJECT)

        assert logging_client.list_entries.call_args.kwargs["max_results"] == 1


class TestCatalogLineageApi:
    @pytest.fixture
    def google_clients(self) -> Iterator[Dict[str, MagicMock]]:
        with (
            patch(
                "datahub.ingestion.source.bigquery_v2.lineage.datacatalog_lineage.LineageClient"
            ) as lineage_client_cls,
            patch(
                "datahub.ingestion.source.bigquery_v2.lineage.BigQuerySchemaApi"
            ) as schema_api_cls,
            patch.object(BigQueryV2Config, "get_bigquery_client"),
            patch.object(BigQueryV2Config, "get_projects_client"),
        ):
            yield {
                "lineage": lineage_client_cls.return_value,
                "schema": schema_api_cls.return_value,
            }

    @staticmethod
    def _table(table_id: str, table_type: str = "TABLE") -> SimpleNamespace:
        return SimpleNamespace(
            project=PROJECT,
            dataset_id=DATASET,
            table_id=table_id,
            table_type=table_type,
        )

    def test_upstreams_are_merged_across_regions_and_filtered(
        self, google_clients: Dict[str, MagicMock]
    ) -> None:
        google_clients["schema"].get_datasets_for_project_id.return_value = [
            BigqueryDataset(name=DATASET)
        ]
        google_clients["schema"].list_tables.return_value = [
            self._table("t1"),
            self._table("ext", table_type="EXTERNAL"),
            self._table("denied"),
            self._table("no_upstreams"),
        ]

        def search_links(request: Any) -> List[SimpleNamespace]:
            region = request.parent.rsplit("/", 1)[1]
            if request.target.fully_qualified_name.endswith(".t1"):
                return [
                    SimpleNamespace(
                        source=SimpleNamespace(
                            fully_qualified_name=f"bigquery:{PROJECT}.{DATASET}.src_{region}"
                        )
                    )
                ]
            return []

        google_clients["lineage"].search_links.side_effect = search_links
        extractor = make_extractor(
            BigQueryV2Config(
                extract_lineage_from_catalog=True,
                table_pattern={"deny": [".*\\.denied"]},
            )
        )

        lineage = extractor.lineage_via_catalog_lineage_api(PROJECT)

        assert set(lineage) == {table_ref("t1")}
        assert upstream_tables(lineage[table_ref("t1")]) == {
            table_ref("src_us"),
            table_ref("src_eu"),
        }
        assert extractor.report.num_skipped_lineage_entries_not_allowed[PROJECT] == 1
        # External tables are never looked up in the catalog.
        looked_up = {
            call.kwargs["request"].target.fully_qualified_name
            for call in google_clients["lineage"].search_links.call_args_list
        }
        assert f"bigquery:{PROJECT}.{DATASET}.ext" not in looked_up


class TestExternalTableLineage:
    """GCS lineage derived from an external table's DDL."""

    def _extractor(self, **config: Any) -> BigqueryLineageExtractor:
        return make_extractor(
            BigQueryV2Config(
                include_table_lineage=True,
                include_column_lineage_with_gcs=False,
                **config,
            )
        )

    def _urn(self) -> str:
        return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{PROJECT}.{DATASET}.ext,PROD)"

    def test_gcs_uris_in_ddl_become_upstreams(self) -> None:
        extractor = self._extractor()
        ddl = 'CREATE EXTERNAL TABLE ext OPTIONS(format="PARQUET", uris=["gs://bucket/path/a/*.parquet", "s3://ignored/x"])'

        wus = list(extractor.gen_lineage_workunits_for_external_table(self._urn(), ddl))

        assert len(wus) == 1
        aspect = wus[0].metadata.aspect  # type: ignore[union-attr]
        assert isinstance(aspect, UpstreamLineageClass)
        assert [u.dataset for u in aspect.upstreams] == [
            "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/path/a,PROD)"
        ]

    @pytest.mark.parametrize(
        "ddl",
        [
            None,
            "CREATE EXTERNAL TABLE ext OPTIONS(format='CSV')",
            "CREATE EXTERNAL TABLE ext OPTIONS(uris=[not json])",
        ],
    )
    def test_unusable_ddl_yields_no_lineage(self, ddl: Optional[str]) -> None:
        extractor = self._extractor()

        assert (
            list(extractor.gen_lineage_workunits_for_external_table(self._urn(), ddl))
            == []
        )
        assert extractor.report.num_skipped_external_table_lineage == (
            0 if ddl is None else 1
        )

    def test_strip_urls_keeps_only_the_directory_of_a_file_path(self) -> None:
        extractor = self._extractor(gcs_lineage_config={"strip_urls": True})
        assert (
            extractor._get_gcs_path("gs://bucket/dir/file.parquet") == "gs://bucket/dir"
        )

        extractor = self._extractor(gcs_lineage_config={"strip_urls": False})
        assert (
            extractor._get_gcs_path("gs://bucket/dir/file.parquet")
            == "gs://bucket/dir/file.parquet"
        )


def test_lineage_edges_for_the_same_table_merge_their_column_mappings() -> None:
    a = LineageEdge(
        table=table_ref("src"),
        column_mapping=frozenset(
            {LineageEdgeColumnMapping(out_column="x", in_columns=frozenset({"a"}))}
        ),
        auditStamp=START,
    )
    b = LineageEdge(
        table=table_ref("src"),
        column_mapping=frozenset(
            {
                LineageEdgeColumnMapping(out_column="x", in_columns=frozenset({"b"})),
                LineageEdgeColumnMapping(out_column="y", in_columns=frozenset({"c"})),
            }
        ),
        auditStamp=START,
    )

    merged = _merge_lineage_edge_columns(a, b)

    assert {m.out_column: m.in_columns for m in merged.column_mapping} == {
        "x": frozenset({"a", "b"}),
        "y": frozenset({"c"}),
    }
    assert _merge_lineage_edge_columns(None, b) is b


def test_run_status_is_forwarded_to_redundant_run_handler() -> None:
    handler = MagicMock()
    handler.suggest_run_time_window.return_value = (START, END)
    extractor = make_extractor(BigQueryV2Config(), redundant_run_skip_handler=handler)

    extractor.report_status("lineage-ingestion", False)

    handler.report_current_run_status.assert_called_once_with(
        "lineage-ingestion", False
    )
