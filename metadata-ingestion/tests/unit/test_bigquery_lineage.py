import datetime
from typing import Dict, List, Set

import pytest

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigQueryTableRef,
    QueryEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.lineage import (
    BigqueryLineageExtractor,
    LineageEdge,
)
from datahub.metadata.schema_classes import UpstreamLineageClass
from datahub.sql_parsing.schema_resolver import SchemaResolver


@pytest.fixture
def lineage_entries() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="""
                INSERT INTO `my_project.my_dataset.my_table`
                SELECT first.a, second.b FROM `my_project.my_dataset.my_source_table1` first
                LEFT JOIN `my_project.my_dataset.my_source_table2` second ON first.id = second.id
            """,
            statementType="INSERT",
            project_id="proj_12344",
            end_time=None,
            referencedTables=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table1"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table2"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="testQuery",
            statementType="SELECT",
            project_id="proj_12344",
            end_time=datetime.datetime.fromtimestamp(
                1617295943.17321, tz=datetime.timezone.utc
            ),
            referencedTables=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table3"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="testQuery",
            statementType="SELECT",
            project_id="proj_12344",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_view1"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
    ]


def test_lineage_with_timestamps(lineage_entries: List[QueryEvent]) -> None:
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config, report, lambda x: builder.make_dataset_urn("bigquery", str(x))
    )

    bq_table = BigQueryTableRef.from_string_name(
        "projects/my_project/datasets/my_dataset/tables/my_table"
    )

    lineage_map: Dict[str, Set[LineageEdge]] = extractor._create_lineage_map(
        iter(lineage_entries),
        sql_parser_schema_resolver=SchemaResolver(platform="bigquery"),
    )

    upstream_lineage = extractor.get_lineage_for_table(
        bq_table=bq_table,
        bq_table_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        lineage_metadata=lineage_map,
    )
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 4


def test_column_level_lineage(lineage_entries: List[QueryEvent]) -> None:
    config = BigQueryV2Config(extract_column_lineage=True, incremental_lineage=False)
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config, report, lambda x: builder.make_dataset_urn("bigquery", str(x))
    )

    bq_table = BigQueryTableRef.from_string_name(
        "projects/my_project/datasets/my_dataset/tables/my_table"
    )

    lineage_map: Dict[str, Set[LineageEdge]] = extractor._create_lineage_map(
        lineage_entries[:1],
        sql_parser_schema_resolver=SchemaResolver(platform="bigquery"),
    )

    upstream_lineage = extractor.get_lineage_for_table(
        bq_table=bq_table,
        bq_table_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        lineage_metadata=lineage_map,
    )
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 2
    assert (
        upstream_lineage.fineGrainedLineages
        and len(upstream_lineage.fineGrainedLineages) == 2
    )


class MockSchemaResolver(SchemaResolver):
    def __init__(self, urns: Set[str]):
        self._urns = urns

    def get_urns(self) -> Set[str]:
        return self._urns


@pytest.fixture
def mock_schema_resolver() -> MockSchemaResolver:
    urns = {
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table3/file2.csv,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table2/part1/file1.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/sample1.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table3/file1.txt,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file2.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file1.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table2/part2/file2.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/bar.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/sample2.parquet,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept1/tests/table4/file1.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept1/tests/table4/file2.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept2/tests/table5/year=2023/month=06/file1.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept2/tests/table5/year=2023/month=07/file2.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept3/tests/table6/2023/06/29/file1.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept3/tests/table6/2023/06/30/file2.avro,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept4/tests/table7/2023/06/29/file1.txt,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept4/tests/table7/2023/06/29/file2.csv,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/level1/table8/2023/06/29/file1.txt,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/level1/table8/2023/06/29/file2.csv,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/level1/level2/table9/2023/06/29/file1.txt,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/level1/level2/table9/2023/06/29/file2.csv,PROD)",
    }
    return MockSchemaResolver(urns)


@pytest.fixture
def gcs_patterns() -> List[str]:
    return [
        "my-bucket/foo/tests/bar.avro",
        "my-bucket/foo/tests/*.*",
        "my-bucket/foo/tests/{table}/*.avro",
        "my-bucket/foo/tests/{table}/*/*.avro",
        "my-bucket/foo/tests/{table}/*.*",
        "my-bucket/{dept}/tests/{table}/*.avro",
        "my-bucket/{dept}/tests/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.avro",
        "my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.avro",
        "my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.*",
        "my-bucket/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.*",
        "my-bucket/*/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.*",
    ]


def validate_workunits(workunits, expected_upstreams):
    assert len(workunits) == 1
    assert workunits[0].get_aspect_of_type(UpstreamLineageClass)
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert len(upstream_lineage.upstreams) == len(expected_upstreams)
    upstream_urns = {upstream.dataset for upstream in upstream_lineage.upstreams}
    for expected_upstream in expected_upstreams:
        assert expected_upstream in upstream_urns


@pytest.mark.parametrize(
    "gcs_pattern, expected_upstreams",
    [
        (
            ["gs://my-bucket/foo/tests/bar.avro"],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/bar.avro,PROD)"
            ],
        ),
        (
            ["gs://my-bucket/foo/tests/*.*"],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/sample2.parquet,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/bar.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/sample1.avro,PROD)",
            ],
        ),
        (
            ["gs://my-bucket/foo/tests/{table}/*.avro"],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file2.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file1.avro,PROD)",
            ],
        ),
        (
            ["gs://my-bucket/foo/tests/{table}/*/*.avro"],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table2/part1/file1.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table2/part2/file2.avro,PROD)",
            ],
        ),
        (
            ["gs://my-bucket/foo/tests/{table}/*.*"],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table3/file2.csv,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table3/file1.txt,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file2.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file1.avro,PROD)",
            ],
        ),
        (
            ["gs://my-bucket/{dept}/tests/{table}/*.avro"],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept1/tests/table4/file1.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept1/tests/table4/file2.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file1.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file2.avro,PROD)",
            ],
        ),
        (
            [
                "gs://my-bucket/{dept}/tests/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.avro"
            ],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept2/tests/table5/year=2023/month=06/file1.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept2/tests/table5/year=2023/month=07/file2.avro,PROD)",
            ],
        ),
        (
            [
                "gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.avro"
            ],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept3/tests/table6/2023/06/29/file1.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept3/tests/table6/2023/06/30/file2.avro,PROD)",
            ],
        ),
        (
            [
                "gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.*"
            ],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept3/tests/table6/2023/06/30/file2.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept4/tests/table7/2023/06/29/file2.csv,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept3/tests/table6/2023/06/29/file1.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept4/tests/table7/2023/06/29/file1.txt,PROD)",
            ],
        ),
        (
            [
                "gs://my-bucket/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.*"
            ],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept2/tests/table5/year=2023/month=07/file2.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/level1/table8/2023/06/29/file1.txt,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept2/tests/table5/year=2023/month=06/file1.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/level1/table8/2023/06/29/file2.csv,PROD)",
            ],
        ),
        (
            [
                "gs://my-bucket/*/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.*"
            ],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept3/tests/table6/2023/06/30/file2.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept3/tests/table6/2023/06/29/file1.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/level1/level2/table9/2023/06/29/file2.csv,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept4/tests/table7/2023/06/29/file1.txt,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/dept4/tests/table7/2023/06/29/file2.csv,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/level1/level2/table9/2023/06/29/file1.txt,PROD)",
            ],
        ),
        (
            [
                "gs://my-bucket/foo/tests/bar.avro",
                "gs://my-bucket/foo/tests/{table}/*.avro",
            ],
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/bar.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file2.avro,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/foo/tests/table1/file1.avro,PROD)",
            ],
        ),
    ],
)
def test_gen_lineage_workunits_with_gcs(
    mock_schema_resolver: MockSchemaResolver,
    gcs_pattern: List[str],
    expected_upstreams: List[str],
) -> None:
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor = BigqueryLineageExtractor(
        config, report, lambda x: builder.make_dataset_urn("bigquery", str(x))
    )

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)"

    workunits = list(
        extractor.gen_lineage_workunits_with_gcs(
            mock_schema_resolver, dataset_urn, source_uris=gcs_pattern
        )
    )

    validate_workunits(workunits, expected_upstreams)
