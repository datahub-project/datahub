"""Unit tests for lineage operation nodes built with SqlParsingAggregator."""

import datetime
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, Mock, call, patch

import google.cloud
import pytest
from google.api_core.exceptions import Forbidden
from google.cloud.datacatalog_lineage import Process

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_lineage import (
    FAILED_PARENTS_KEY,
    LINK_PROCESSES_KEY,
    UPSTREAM_LINKS_KEY,
    DataplexLineageExtractor,
    LineageEdge,
    UpstreamLinkInfo,
    _query_id,
)
from datahub.ingestion.source.dataplex.dataplex_operations import (
    BATCH_FAILURE_BREAKER_THRESHOLD,
    BigQueryJobSqlFetcher,
    LinkProcessChoice,
    ProcessInfo,
    build_process_query,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.metadata.schema_classes import (
    QueryPropertiesClass,
    StatusClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import MAX_UPSTREAM_TABLES_COUNT
from tests.unit.dataplex.test_dataplex_upstream_resolution import (
    make_dpms_table_entry,
)
from tests.unit.dataplex.workunit_assertions import aspect_of

SCAN_PAIRS = [("my-project", "us-central1")]
PARENT = "projects/my-project/locations/us-central1"
UPSTREAM_FQN = "bigquery:my-project.my_dataset.upstream_table"
DOWNSTREAM_FQN = "bigquery:my-project.my_dataset.my_table"
UPSTREAM_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
    "my-project.my_dataset.upstream_table,PROD)"
)
DOWNSTREAM_NAME = "my-project.my_dataset.my_table"
DOWNSTREAM_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my_dataset.my_table,PROD)"
)
OTHER_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my_dataset.other_table,PROD)"
GCS_BUCKET_URN = "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket,PROD)"
PROCESS_NAME = "projects/my-project/locations/us-central1/processes/my-process"
LINK_NAME = "projects/my-project/locations/us-central1/links/my-link"
SQL = "INSERT INTO my_dataset.my_table SELECT * FROM my_dataset.upstream_table"


@dataclass
class FakeEntityReference:
    fully_qualified_name: str = ""
    field: List[str] = field(default_factory=list)


@dataclass
class FakeLink:
    source: FakeEntityReference
    target: FakeEntityReference
    name: str = ""


@dataclass
class FakeProcessLinkInfo:
    link: str
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None


@dataclass
class FakeProcessLinks:
    process: str
    links: List[FakeProcessLinkInfo]


@dataclass
class FakeSourceType:
    name: str


@dataclass
class FakeOrigin:
    source_type: FakeSourceType
    name: str = ""


@dataclass
class FakeProcess:
    name: str
    origin: FakeOrigin
    attributes: Dict[str, str] = field(default_factory=dict)


def epoch(seconds: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc)


def make_entry(
    dataset_name: str = DOWNSTREAM_NAME, schema_field_paths: Tuple[str, ...] = ()
) -> EntryDataTuple:
    return EntryDataTuple(
        dataplex_entry_short_name=dataset_name.rsplit(".", 1)[-1],
        dataplex_entry_name=f"projects/p/locations/us/entryGroups/g/entries/{dataset_name}",
        dataplex_location="us",
        dataplex_entry_fqn=f"bigquery:{dataset_name}",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name=dataset_name,
        datahub_dataset_urn=(
            f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{dataset_name},PROD)"
        ),
        schema_field_paths=schema_field_paths,
    )


def make_extractor(
    lineage_client: MagicMock, graph: Optional[Mock] = None, **config_overrides: Any
) -> DataplexLineageExtractor:
    config_overrides.setdefault("include_lineage_operations", True)
    config = DataplexConfig(
        project_ids=["my-project"],
        entries_locations=["us"],
        lineage_locations=["us-central1"],
        include_lineage=True,
        **config_overrides,
    )
    return DataplexLineageExtractor(
        config=config,
        report=DataplexReport().lineage_report,
        source_report=Mock(),
        lineage_client=lineage_client,
        graph=graph,
    )


def wire_client(
    lineage_client: MagicMock,
    *,
    origin: str = "BIGQUERY",
    attributes: Optional[Dict[str, str]] = None,
) -> None:
    lineage_client.search_links.return_value = [
        FakeLink(
            source=FakeEntityReference(UPSTREAM_FQN),
            target=FakeEntityReference(DOWNSTREAM_FQN),
            name=LINK_NAME,
        )
    ]
    lineage_client.batch_search_link_processes.return_value = [
        FakeProcessLinks(
            process=PROCESS_NAME,
            links=[FakeProcessLinkInfo(link=LINK_NAME, end_time=epoch(2000))],
        )
    ]
    lineage_client.get_process.return_value = FakeProcess(
        name=PROCESS_NAME,
        origin=FakeOrigin(source_type=FakeSourceType(name=origin)),
        attributes=attributes if attributes is not None else {"sql": SQL},
    )


def warning_titles(extractor: DataplexLineageExtractor) -> List[str]:
    warning = extractor.source_report.warning
    assert isinstance(warning, Mock)
    return [warning_call.kwargs["title"] for warning_call in warning.call_args_list]


def aspects_named(workunits: List[MetadataWorkUnit], aspect_type: type) -> List[Any]:
    return [
        aspect_of(workunit)
        for workunit in workunits
        if isinstance(aspect_of(workunit), aspect_type)
    ]


@pytest.fixture
def lineage_client() -> MagicMock:
    return MagicMock()


class TestOperationNodes:
    def test_disabled_by_default_issues_no_extra_calls(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        extractor = make_extractor(lineage_client, include_lineage_operations=False)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        lineage_client.batch_search_link_processes.assert_not_called()
        lineage_client.get_process.assert_not_called()
        (lineage,) = aspects_named(workunits, UpstreamLineageClass)
        assert lineage.upstreams[0].query is None

    def test_process_with_sql_becomes_the_query_on_the_edge(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        extractor = make_extractor(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        query_urn = f"urn:li:query:{_query_id(PROCESS_NAME, DOWNSTREAM_URN)}"
        (properties,) = aspects_named(workunits, QueryPropertiesClass)
        assert properties.statement.value == SQL
        assert properties.customProperties["gcp_process_name"] == PROCESS_NAME
        assert any(workunit.get_urn() == query_urn for workunit in workunits)
        (lineage,) = aspects_named(workunits, UpstreamLineageClass)
        assert [
            (upstream.dataset, upstream.query) for upstream in lineage.upstreams
        ] == [(UPSTREAM_URN, query_urn)]
        assert extractor.report.num_lineage_relationships_created == 1
        assert extractor.report.num_lineage_entries_with_lineage == 1
        assert extractor.report.num_edges_with_operation == 1

    def test_process_without_sql_keeps_a_plain_edge(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client, attributes={"bigquery_job_id": "job_abc"})
        extractor = make_extractor(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        assert aspects_named(workunits, QueryPropertiesClass) == []
        (lineage,) = aspects_named(workunits, UpstreamLineageClass)
        assert [
            (upstream.dataset, upstream.query) for upstream in lineage.upstreams
        ] == [(UPSTREAM_URN, None)]
        assert extractor.report.num_operations_without_sql == 1
        assert extractor.report.num_edges_with_operation == 0

    def test_bigquery_sql_is_fetched_when_enabled(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client, attributes={"bigquery_job_id": "job_abc"})
        extractor = make_extractor(lineage_client, include_lineage_operation_sql=True)
        assert extractor._sql_fetcher is not None
        bigquery_client = MagicMock()
        bigquery_client.get_job.return_value = Mock(
            query="SELECT 1", job_type="query", total_bytes_processed=10, labels={}
        )
        extractor._sql_fetcher._client = bigquery_client

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        (properties,) = aspects_named(workunits, QueryPropertiesClass)
        assert properties.statement.value == "SELECT 1"
        assert properties.customProperties["bigquery_job_id"] == "job_abc"
        bigquery_client.get_job.assert_called_once_with(
            "job_abc", project="my-project", location="us-central1"
        )

    def test_bigquery_sql_fetch_failure_warns(self, lineage_client: MagicMock) -> None:
        wire_client(lineage_client, attributes={"bigquery_job_id": "job_abc"})
        extractor = make_extractor(lineage_client, include_lineage_operation_sql=True)
        assert extractor._sql_fetcher is not None
        bigquery_client = MagicMock()
        bigquery_client.get_job.side_effect = Forbidden("bigquery.jobs.get denied")
        extractor._sql_fetcher._client = bigquery_client

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        assert aspects_named(workunits, QueryPropertiesClass) == []
        assert extractor.report.num_operation_sql_fetch_failed == 1
        assert warning_titles(extractor) == [
            "Dataplex BigQuery job SQL fetch failed",
            "Dataplex lineage operation lookup degraded",
        ]

    def test_close_releases_the_bigquery_client(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client, include_lineage_operation_sql=True)
        assert extractor._sql_fetcher is not None
        bigquery_client = MagicMock()
        extractor._sql_fetcher._client = bigquery_client

        extractor.close()

        bigquery_client.close.assert_called_once()

    def test_denied_origin_keeps_the_edge_without_a_query(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        extractor = make_extractor(
            lineage_client, lineage_operation_origin_pattern={"deny": ["BIGQUERY"]}
        )

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        assert aspects_named(workunits, QueryPropertiesClass) == []
        assert len(aspects_named(workunits, UpstreamLineageClass)) == 1
        assert extractor.report.num_operations_filtered_by_origin == 1

    def test_one_get_process_per_distinct_process(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        extractor = make_extractor(lineage_client)

        list(
            extractor.get_lineage_workunits(
                [make_entry(), make_entry("my-project.my_dataset.other_table")],
                SCAN_PAIRS,
            )
        )

        assert lineage_client.get_process.call_count == 1

    def test_process_lookup_failure_degrades_and_warns(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        lineage_client.get_process.side_effect = RuntimeError("permission denied")
        extractor = make_extractor(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        assert len(aspects_named(workunits, UpstreamLineageClass)) == 1
        assert aspects_named(workunits, QueryPropertiesClass) == []
        warning = extractor.source_report.warning
        assert isinstance(warning, Mock)
        assert warning.call_args.kwargs["title"] == (
            "Dataplex lineage operation lookup degraded"
        )

    def test_downstream_too_large_for_the_aggregator_keeps_every_edge(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        upstream_count = MAX_UPSTREAM_TABLES_COUNT + 1
        # The first link has a SQL process, which would get a Query entity.
        lineage_client.search_links.return_value = [
            FakeLink(
                source=FakeEntityReference(
                    f"bigquery:my-project.my_dataset.events_{index:04d}"
                ),
                target=FakeEntityReference(DOWNSTREAM_FQN),
                name=LINK_NAME if index == 0 else f"{LINK_NAME}-{index}",
            )
            for index in range(upstream_count)
        ]
        extractor = make_extractor(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        (lineage,) = aspects_named(workunits, UpstreamLineageClass)
        assert len(lineage.upstreams) == upstream_count
        assert aspects_named(workunits, QueryPropertiesClass) == []
        lineage_client.get_process.assert_not_called()
        assert extractor.report.num_downstreams_too_large_for_operations == 1
        assert warning_titles(extractor) == [
            "Dataplex lineage operations skipped for a large downstream"
        ]


class TestColumnAttribution:
    # None ties with the plain query's missing timestamp.
    @pytest.mark.parametrize("operation_end_ms", [2_000_000, None])
    def test_columns_follow_the_query_that_covers_their_upstreams(
        self, lineage_client: MagicMock, operation_end_ms: Optional[int]
    ) -> None:
        wire_client(lineage_client)
        extractor = make_extractor(lineage_client)
        edges = {
            LineageEdge(
                upstream_datahub_urn=UPSTREAM_URN,
                audit_stamp=epoch(0),
                process_name=PROCESS_NAME,
                operation_end_ms=operation_end_ms,
            ),
            LineageEdge(upstream_datahub_urn=OTHER_URN, audit_stamp=epoch(0)),
        }

        extractor._register_with_aggregator(
            DOWNSTREAM_NAME,
            DOWNSTREAM_URN,
            edges,
            {
                "col_a": [(UPSTREAM_URN, "id")],
                "col_b": [(UPSTREAM_URN, "id"), (OTHER_URN, "id")],
            },
        )
        workunits = [
            change_proposal.as_workunit()
            for change_proposal in extractor._aggregators["bigquery"].gen_metadata()
        ]

        query_urn = f"urn:li:query:{_query_id(PROCESS_NAME, DOWNSTREAM_URN)}"
        (lineage,) = aspects_named(workunits, UpstreamLineageClass)
        assert {upstream.dataset: upstream.query for upstream in lineage.upstreams} == {
            UPSTREAM_URN: query_urn,
            OTHER_URN: None,
        }
        queries_by_column = {
            fine_grained.downstreams[0]
            .rsplit(",", 1)[-1]
            .rstrip(")"): fine_grained.query
            for fine_grained in lineage.fineGrainedLineages
        }
        assert queries_by_column["col_a"] == query_urn
        # Spans the query's upstream and a plain one, so no single query owns it.
        assert queries_by_column["col_b"] is None
        assert extractor.report.num_lineage_relationships_created == 2
        assert extractor.report.num_fine_grained_lineages_created == 2


class TestParentRules:
    def test_column_lookup_failure_withholds_the_entry(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        table_links = lineage_client.search_links.return_value

        def search_links(request: Any) -> List[FakeLink]:
            if request.targets.entities:
                raise RuntimeError("column lookup failed")
            return table_links

        lineage_client.search_links.side_effect = search_links
        extractor = make_extractor(lineage_client, include_column_lineage=True)

        workunits = list(
            extractor.get_lineage_workunits(
                [make_entry(schema_field_paths=("col_a",))], SCAN_PAIRS
            )
        )

        assert workunits == []
        lineage_client.batch_search_link_processes.assert_not_called()

    def test_withheld_storage_edge_is_not_registered(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client, include_storage_lineage=True)
        extractor.get_lineage_for_entry = Mock(  # type: ignore[method-assign]
            return_value={"upstream": [], FAILED_PARENTS_KEY: [PARENT]}
        )
        entry = make_dpms_table_entry(storage_gcs_bucket="my-bucket")

        assert list(extractor.get_lineage_workunits([entry], SCAN_PAIRS)) == []
        assert extractor.report.num_storage_lineage_withheld == 1

    def test_storage_edge_keeps_the_api_links_operation(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client, include_storage_lineage=True)
        lineage_data = {
            "upstream": ["gcs:my-bucket"],
            FAILED_PARENTS_KEY: [],
            UPSTREAM_LINKS_KEY: [
                UpstreamLinkInfo(
                    source_fqn="gcs:my-bucket", link_name=LINK_NAME, parent=PARENT
                )
            ],
            LINK_PROCESSES_KEY: {
                LINK_NAME: LinkProcessChoice(process_name=PROCESS_NAME)
            },
        }

        edges, _ = extractor._extract_lineage_edges_for_entry(
            make_dpms_table_entry(storage_gcs_bucket="my-bucket"), lineage_data
        )

        assert [(edge.upstream_datahub_urn, edge.process_name) for edge in edges] == [
            (GCS_BUCKET_URN, PROCESS_NAME)
        ]

    @pytest.mark.parametrize("newest_first", [False, True])
    def test_paths_in_one_bucket_keep_the_latest_operation(
        self, lineage_client: MagicMock, newest_first: bool
    ) -> None:
        extractor = make_extractor(lineage_client)
        paths = {
            "gcs:my-bucket.a/old.csv": ("processes/old", 1000),
            "gcs:my-bucket.b/new.csv": ("processes/new", 9000),
        }
        upstream = list(paths)[::-1] if newest_first else list(paths)
        lineage_data = {
            "upstream": upstream,
            FAILED_PARENTS_KEY: [],
            UPSTREAM_LINKS_KEY: [
                UpstreamLinkInfo(source_fqn=fqn, link_name=fqn, parent=PARENT)
                for fqn in upstream
            ],
            LINK_PROCESSES_KEY: {
                fqn: LinkProcessChoice(process_name=process, end_time_ms=end_ms)
                for fqn, (process, end_ms) in paths.items()
            },
        }

        edges, _ = extractor._extract_lineage_edges_for_entry(
            make_entry(), lineage_data
        )

        assert [
            (edge.upstream_datahub_urn, edge.process_name, edge.operation_end_ms)
            for edge in edges
        ] == [(GCS_BUCKET_URN, "processes/new", 9000)]

    def test_lineage_only_stub_is_written_for_aggregator_edges(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        extractor = make_extractor(
            lineage_client,
            graph=Mock(exists=Mock(return_value=False)),
            include_lineage_only_upstreams=True,
        )

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        assert [
            workunit.get_urn()
            for workunit in workunits
            if isinstance(aspect_of(workunit), StatusClass)
        ] == [UPSTREAM_URN]


class TestProcessResolution:
    def test_multi_process_link_collapses_to_the_latest(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        lineage_client.batch_search_link_processes.return_value = [
            FakeProcessLinks(
                process="processes/older",
                links=[FakeProcessLinkInfo(link=LINK_NAME, end_time=epoch(1000))],
            ),
            FakeProcessLinks(
                process="processes/newer",
                links=[FakeProcessLinkInfo(link=LINK_NAME, end_time=epoch(5000))],
            ),
        ]
        assert extractor._operation_resolver is not None

        choices = extractor._operation_resolver.resolve_links_to_processes(
            "projects/my-project/locations/us-central1", [LINK_NAME]
        )

        assert choices[LINK_NAME].process_name == "processes/newer"
        assert extractor.report.num_multi_process_links_collapsed == 1

    def test_repeated_batch_failures_open_the_breaker(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client, lineage_max_retries=1)
        lineage_client.batch_search_link_processes.side_effect = RuntimeError("denied")
        assert extractor._operation_resolver is not None
        parent = "projects/my-project/locations/us-central1"

        for _ in range(BATCH_FAILURE_BREAKER_THRESHOLD + 2):
            extractor._operation_resolver.resolve_links_to_processes(
                parent, [LINK_NAME]
            )

        assert (
            lineage_client.batch_search_link_processes.call_count
            == BATCH_FAILURE_BREAKER_THRESHOLD
        )

    def test_process_calls_charge_their_location(
        self, lineage_client: MagicMock
    ) -> None:
        wire_client(lineage_client)
        extractor = make_extractor(lineage_client)
        assert extractor._operation_resolver is not None
        limiter = Mock(wraps=extractor._rate_limiter)
        extractor._operation_resolver._rate_limiter = limiter

        list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        assert limiter.acquire.call_args_list == [call(PARENT), call(PROCESS_NAME)]

    def test_list_and_struct_attributes_are_stored_as_json(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        assert extractor._operation_resolver is not None
        lineage_client.get_process.return_value = Process(
            name=PROCESS_NAME,
            attributes={"tags": ["a", 1], "owner": {"team": "data", "tier": 2}},
        )

        process = extractor._operation_resolver.get_process_info(PROCESS_NAME)

        assert process is not None
        assert process.attribute("tags") == '["a", 1.0]'
        assert process.attribute("owner") == '{"team": "data", "tier": 2.0}'


def _process(attributes: Dict[str, str], origin: str = "DATAFLOW") -> ProcessInfo:
    return ProcessInfo(
        name=PROCESS_NAME,
        origin_source_type=origin,
        attributes=tuple(sorted(attributes.items())),
    )


def test_dbt_header_is_copied_to_query_properties() -> None:
    query = build_process_query(
        _process({"sql": '/* {"app": "dbt", "node_id": "model.my_model"} */ SELECT 1'}),
        DataplexConfig(project_ids=["my-project"]),
        sql_fetcher=None,
    )

    assert query is not None
    assert query.custom_properties["dbt_node_id"] == "model.my_model"


def test_only_allowlisted_attributes_are_copied() -> None:
    query = build_process_query(
        _process({"sql": "SELECT 1", "dag_id": "my_dag", "secret_token": "x"}),
        DataplexConfig(project_ids=["my-project"]),
        sql_fetcher=None,
    )

    assert query is not None
    assert query.custom_properties["dag_id"] == "my_dag"
    assert "secret_token" not in query.custom_properties


def test_bigquery_client_failure_warns_once_and_disables_the_fetcher() -> None:
    source_report = Mock()
    fetcher = BigQueryJobSqlFetcher(
        report=DataplexReport().lineage_report, source_report=source_report
    )

    error = OSError("no credentials")

    with patch("google.cloud.bigquery.Client", side_effect=error) as client_class:
        results = [
            fetcher.fetch(_process({"bigquery_job_id": job_id}, origin="BIGQUERY"))
            for job_id in ("job_a", "job_b")
        ]

    assert results == [None, None]
    client_class.assert_called_once()
    (warning_call,) = source_report.warning.call_args_list
    assert warning_call.kwargs["title"] == "Dataplex BigQuery client unavailable"
    assert "not installed" not in warning_call.args[0]
    assert warning_call.kwargs["exc"] is error


def test_missing_bigquery_package_warns_that_it_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(google.cloud, "bigquery", raising=False)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", None)
    source_report = Mock()
    fetcher = BigQueryJobSqlFetcher(
        report=DataplexReport().lineage_report, source_report=source_report
    )

    result = fetcher.fetch(_process({"bigquery_job_id": "job_a"}, origin="BIGQUERY"))

    assert result is None
    (warning_call,) = source_report.warning.call_args_list
    assert "google-cloud-bigquery package is not installed" in warning_call.args[0]
    assert isinstance(warning_call.kwargs["exc"], ImportError)


class TestOperationConfig:
    def test_operations_require_lineage(self) -> None:
        config = DataplexConfig(
            project_ids=["my-project"],
            include_lineage=False,
            include_lineage_operations=True,
        )
        assert not config.include_lineage_operations

    def test_sql_enrichment_requires_operations(self) -> None:
        config = DataplexConfig(
            project_ids=["my-project"], include_lineage_operation_sql=True
        )
        assert not config.include_lineage_operation_sql
