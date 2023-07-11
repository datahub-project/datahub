import datetime
from typing import Dict, List, Set

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
from datahub.utilities.sqlglot_lineage import SchemaResolver


def test_lineage_with_timestamps():
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(config, report)
    lineage_entries: List[QueryEvent] = [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="testQuery",
            statementType="SELECT",
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
        platform="bigquery",
    )
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 4
