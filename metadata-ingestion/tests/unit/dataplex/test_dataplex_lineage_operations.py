"""Unit tests for Dataplex lineage operation nodes (Query entities)."""

import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, cast
from unittest.mock import MagicMock, Mock

import pytest

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_lineage import DataplexLineageExtractor
from datahub.ingestion.source.dataplex.dataplex_operations import (
    BATCH_FAILURE_BREAKER_THRESHOLD,
    MAX_QUERY_SUBJECTS,
    ProcessInfo,
    _parse_dbt_header,
    query_urn_for_process,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySubjectsClass,
)
from tests.unit.dataplex.workunit_assertions import (
    aspect_of,
    entity_urn,
    only_aspect,
)

SCAN_PAIRS = [("my-project", "us-central1")]
PROCESS_NAME = "projects/my-project/locations/us-central1/processes/my-process"
UPSTREAM_FQN = "bigquery:my-project.my_dataset.upstream_table"
DOWNSTREAM_FQN = "bigquery:my-project.my_dataset.my_table"


@dataclass
class FakeEntityReference:
    fully_qualified_name: str = ""
    field: List[str] = field(default_factory=list)


@dataclass
class FakeLink:
    source: FakeEntityReference
    target: FakeEntityReference
    name: str = ""
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None


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
    display_name: str
    origin: FakeOrigin
    attributes: Dict[str, str] = field(default_factory=dict)


def epoch(seconds: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc)


def make_entry(dataset_name: str = "my-project.my_dataset.my_table") -> EntryDataTuple:
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
    )


def make_extractor(
    lineage_client: MagicMock, **config_overrides: object
) -> DataplexLineageExtractor:
    config = DataplexConfig(
        project_ids=["my-project"],
        entries_locations=["us"],
        lineage_locations=["us-central1"],
        include_lineage=True,
        include_lineage_operations=True,
        **config_overrides,
    )
    return DataplexLineageExtractor(
        config=config,
        report=DataplexReport().lineage_report,
        source_report=Mock(),
        lineage_client=lineage_client,
    )


def wire_client(
    lineage_client: MagicMock,
    *,
    origin: str = "BIGQUERY",
    attributes: Optional[Dict[str, str]] = None,
    link_name: str = "projects/my-project/locations/us-central1/links/my-link",
    runs: Optional[List[object]] = None,
) -> None:
    lineage_client.search_links.return_value = [
        FakeLink(
            source=FakeEntityReference(UPSTREAM_FQN),
            target=FakeEntityReference(DOWNSTREAM_FQN),
            name=link_name,
            start_time=epoch(1000),
            end_time=epoch(2000),
        )
    ]
    lineage_client.batch_search_link_processes.return_value = [
        FakeProcessLinks(
            process=PROCESS_NAME,
            links=[
                FakeProcessLinkInfo(
                    link=link_name, start_time=epoch(1000), end_time=epoch(2000)
                )
            ],
        )
    ]
    lineage_client.get_process.return_value = FakeProcess(
        name=PROCESS_NAME,
        display_name="my daily load",
        origin=FakeOrigin(source_type=FakeSourceType(name=origin)),
        attributes=attributes or {"bigquery_job_id": "job_abc"},
    )
    pager = MagicMock()
    pager.pages = iter([MagicMock(runs=runs or [])])
    lineage_client.list_runs.return_value = pager


@pytest.fixture
def lineage_client() -> MagicMock:
    return MagicMock()


class TestOperationAttribution:
    def test_disabled_by_default_issues_no_extra_calls(
        self, lineage_client: MagicMock
    ) -> None:
        config = DataplexConfig(
            project_ids=["my-project"],
            lineage_locations=["us-central1"],
            include_lineage=True,
        )
        extractor = DataplexLineageExtractor(
            config=config,
            report=DataplexReport().lineage_report,
            source_report=Mock(),
            lineage_client=lineage_client,
        )
        wire_client(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        lineage_client.batch_search_link_processes.assert_not_called()
        assert len(workunits) == 1
        assert aspect_of(workunits[0]).upstreams[0].query is None

    def test_edge_carries_the_query_urn_and_creation_stamp(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        upstream = aspect_of(workunits[0]).upstreams[0]
        assert upstream.query == query_urn_for_process(PROCESS_NAME)
        assert upstream.created.time == 2000 * 1000
        assert extractor.report.num_edges_with_operation == 1

    def test_query_entity_is_emitted_after_the_pool_drains(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        query_urn = query_urn_for_process(PROCESS_NAME)
        query_workunits = [
            workunit for workunit in workunits if entity_urn(workunit) == query_urn
        ]
        aspect_types = {type(aspect_of(workunit)) for workunit in query_workunits}
        assert aspect_types == {
            QueryPropertiesClass,
            QuerySubjectsClass,
            DataPlatformInstanceClass,
        }
        properties = next(
            aspect_of(workunit)
            for workunit in query_workunits
            if isinstance(aspect_of(workunit), QueryPropertiesClass)
        )
        assert properties.name == "my daily load"
        assert properties.customProperties["gcp_process_name"] == PROCESS_NAME
        assert properties.customProperties["bigquery_job_id"] == "job_abc"
        assert extractor.report.num_query_entities_emitted == 1

    def test_query_subjects_cover_both_ends_of_the_edge(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        subjects = next(
            aspect_of(workunit)
            for workunit in workunits
            if isinstance(aspect_of(workunit), QuerySubjectsClass)
        )
        entities = [subject.entity for subject in subjects.subjects]
        assert entities == sorted(entities)
        assert set(entities) == {
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
            "my-project.my_dataset.upstream_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
            "my-project.my_dataset.my_table,PROD)",
        }

    def test_denied_origin_keeps_the_edge_but_drops_the_operation(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(
            lineage_client,
            lineage_operation_origin_types={"deny": ["BIGQUERY"]},
        )
        wire_client(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        lineage_aspect = aspect_of(workunits[0])
        assert len(lineage_aspect.upstreams) == 1
        assert lineage_aspect.upstreams[0].query is None
        assert extractor.report.num_operations_filtered_by_origin == 1
        assert extractor.report.num_query_entities_emitted == 0

    def test_one_get_process_per_distinct_process(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client)
        entries = [
            make_entry("my-project.my_dataset.table_one"),
            make_entry("my-project.my_dataset.table_two"),
        ]

        list(extractor.get_lineage_workunits(entries, SCAN_PAIRS))

        assert lineage_client.get_process.call_count == 1
        assert extractor.report.num_processes_fetched == 1
        assert extractor.report.num_process_cache_hits >= 1

    def test_process_lookup_failure_degrades_and_warns(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client)
        lineage_client.get_process.side_effect = RuntimeError("process unavailable")

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        assert aspect_of(workunits[0]).upstreams[0].query is None
        assert extractor.report.num_process_lookup_failed == 1
        assert cast(Mock, extractor.source_report.warning).called


class TestProcessResolution:
    def test_multi_process_link_collapses_to_the_latest(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client)
        link_name = "projects/my-project/locations/us-central1/links/my-link"
        lineage_client.batch_search_link_processes.return_value = [
            FakeProcessLinks(
                process="projects/my-project/locations/us-central1/processes/older",
                links=[FakeProcessLinkInfo(link=link_name, end_time=epoch(1000))],
            ),
            FakeProcessLinks(
                process=PROCESS_NAME,
                links=[FakeProcessLinkInfo(link=link_name, end_time=epoch(9000))],
            ),
        ]

        resolver = extractor._operation_resolver
        assert resolver is not None
        choices = resolver.resolve_links_to_processes(
            "projects/my-project/locations/us-central1", [link_name]
        )

        assert choices[link_name].process_name == PROCESS_NAME
        assert extractor.report.num_multi_process_links_collapsed == 1

    def test_repeated_batch_failures_open_the_breaker(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        lineage_client.batch_search_link_processes.side_effect = RuntimeError("denied")
        parent = "projects/my-project/locations/us-central1"

        resolver = extractor._operation_resolver
        assert resolver is not None
        for _ in range(BATCH_FAILURE_BREAKER_THRESHOLD + 2):
            resolver.resolve_links_to_processes(
                parent, ["projects/my-project/locations/us-central1/links/my-link"]
            )

        assert (
            lineage_client.batch_search_link_processes.call_count
            == BATCH_FAILURE_BREAKER_THRESHOLD
        )
        assert (
            extractor.report.num_link_process_batch_failures
            == BATCH_FAILURE_BREAKER_THRESHOLD
        )

    def test_query_urn_is_derived_from_the_process_name_only(self) -> None:
        first = query_urn_for_process(PROCESS_NAME)
        second = query_urn_for_process(PROCESS_NAME)
        other = query_urn_for_process(PROCESS_NAME + "-other")

        assert first == second
        assert first != other
        assert first.startswith("urn:li:query:dataplex_")


class TestStatementRendering:
    def test_bigquery_origin_without_sql_gets_a_readable_placeholder(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client)

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        properties = next(
            aspect_of(workunit)
            for workunit in workunits
            if isinstance(aspect_of(workunit), QueryPropertiesClass)
        )
        assert "job_abc" in properties.statement.value
        assert properties.statement.language == QueryLanguageClass.SQL

    def test_non_sql_origin_still_declares_sql(self, lineage_client: MagicMock) -> None:
        """A query node carrying any other language breaks every
        searchAcrossLineage whose path crosses it."""
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client, origin="DATAPROC", attributes={})

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        properties = next(
            aspect_of(workunit)
            for workunit in workunits
            if isinstance(aspect_of(workunit), QueryPropertiesClass)
        )
        assert properties.statement.language == QueryLanguageClass.SQL
        assert properties.statement.value.startswith("-- DATAPROC process:")

    def test_sql_attribute_is_used_verbatim(self, lineage_client: MagicMock) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(
            lineage_client,
            origin="CUSTOM",
            attributes={"sql": "SELECT col_a FROM my_database.my_table"},
        )

        workunits = list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        properties = next(
            aspect_of(workunit)
            for workunit in workunits
            if isinstance(aspect_of(workunit), QueryPropertiesClass)
        )
        assert properties.statement.value == "SELECT col_a FROM my_database.my_table"

    def test_unknown_origin_is_counted(self, lineage_client: MagicMock) -> None:
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client, origin="SOURCE_TYPE_UNSPECIFIED", attributes={})

        list(extractor.get_lineage_workunits([make_entry()], SCAN_PAIRS))

        assert extractor.report.num_processes_unknown_origin == 1


class TestQueryEntityAccumulator:
    def test_subjects_are_capped_deterministically(
        self, lineage_client: MagicMock
    ) -> None:
        """Which subjects survive the cap must be a property of the data, not
        of the order worker threads happened to finish in."""
        extractor = make_extractor(lineage_client)
        wire_client(lineage_client)
        accumulator = extractor._operation_accumulator
        assert accumulator is not None
        query_urn = query_urn_for_process(PROCESS_NAME)
        subjects = [f"urn:li:dataset:x{index:05d}" for index in range(1500)]

        # Two different arrival orders for the same set of subjects.
        accumulator.add(
            query_urn=query_urn,
            process_name=PROCESS_NAME,
            downstream_platform="bigquery",
            subject_urns=reversed(subjects),
            start_time_ms=1,
            end_time_ms=2,
        )
        accumulator.add(
            query_urn=query_urn,
            process_name=PROCESS_NAME,
            downstream_platform="bigquery",
            subject_urns=subjects,
            start_time_ms=1,
            end_time_ms=2,
        )

        resolver = extractor._operation_resolver
        assert resolver is not None
        workunits = list(accumulator.gen_workunits(resolver=resolver, sql_fetcher=None))
        emitted = [
            subject.entity
            for subject in only_aspect(workunits, QuerySubjectsClass).subjects
        ]
        assert emitted == sorted(subjects)[:MAX_QUERY_SUBJECTS]
        assert extractor.report.num_query_subjects_truncated == 500

    def test_platform_choice_is_deterministic(self, lineage_client: MagicMock) -> None:
        extractor = make_extractor(lineage_client)
        accumulator = extractor._operation_accumulator
        assert accumulator is not None
        query_urn = query_urn_for_process(PROCESS_NAME)

        for platform in ("pubsub", "bigquery", "spanner"):
            accumulator.add(
                query_urn=query_urn,
                process_name=PROCESS_NAME,
                downstream_platform=platform,
                subject_urns=[],
                start_time_ms=None,
                end_time_ms=None,
            )

        assert accumulator._by_query_urn[query_urn].downstream_platform == "bigquery"


class TestProcessInfo:
    def test_identity_fields_are_parsed_from_the_process_name(self) -> None:
        process = ProcessInfo(
            name=PROCESS_NAME,
            display_name="",
            origin_source_type="BIGQUERY",
            attributes=(("job_id", "job_abc"),),
        )

        assert process.project == "my-project"
        assert process.location == "us-central1"
        assert process.process_id == "my-process"
        assert process.bigquery_job_id == "job_abc"

    def test_unparseable_name_degrades_gracefully(self) -> None:
        process = ProcessInfo(
            name="not-a-process-path",
            display_name="",
            origin_source_type="CUSTOM",
            attributes=(),
        )

        assert process.project is None
        assert process.location is None
        assert process.process_id == "not-a-process-path"


class TestDbtHeader:
    def test_json_header_is_hoisted(self) -> None:
        sql = '/* {"app": "dbt", "dag_id": "my_dag"} */ SELECT 1'
        assert _parse_dbt_header(sql) == {"dbt_app": "dbt", "dbt_dag_id": "my_dag"}

    @pytest.mark.parametrize("sql", [None, "", "SELECT 1", "/* not json */ SELECT 1"])
    def test_non_header_sql_yields_nothing(self, sql: Optional[str]) -> None:
        assert _parse_dbt_header(sql) == {}


class TestOperationConfig:
    def test_operations_require_lineage(self) -> None:
        config = DataplexConfig(
            project_ids=["my-project"],
            include_lineage=False,
            include_lineage_operations=True,
        )
        assert config.include_lineage_operations is False

    def test_sql_enrichment_requires_operations(self) -> None:
        config = DataplexConfig(
            project_ids=["my-project"],
            include_lineage_operations=False,
            include_lineage_operation_sql=True,
        )
        assert config.include_lineage_operation_sql is False
