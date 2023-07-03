import datetime
from typing import Dict, List, Set

from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigQueryTableRef,
    QueryEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryView
from datahub.ingestion.source.bigquery_v2.lineage import (
    BigqueryLineageExtractor,
    LineageEdge,
)


def test_parse_view_lineage():
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor = BigqueryLineageExtractor(config, report)

    # ddl = "select * from some_dataset.sometable as a"
    ddl = """CREATE VIEW `my-project.my-dataset.test_table`
AS SELECT
  * REPLACE(
    myrandom(something) AS something)
FROM
  `my-project2.my-dataset2.test_physical_table`;
"""
    view = BigqueryView(
        name="test",
        created=datetime.datetime.now(tz=datetime.timezone.utc),
        last_altered=datetime.datetime.now(tz=datetime.timezone.utc),
        comment="",
        view_definition=ddl,
    )
    tables = extractor.parse_view_lineage("my_project", "my_dataset", view)
    assert tables is not None
    assert 1 == len(tables)
    assert "my-project2.my-dataset2.test_physical_table" == tables[0].get_table_name()


def test_parse_view_lineage_with_two_part_table_name():
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor = BigqueryLineageExtractor(config, report)

    ddl = "CREATE VIEW my_view as select * from some_dataset.sometable as a"
    view = BigqueryView(
        name="test",
        created=datetime.datetime.now(tz=datetime.timezone.utc),
        last_altered=datetime.datetime.now(tz=datetime.timezone.utc),
        comment="",
        view_definition=ddl,
    )
    tables = extractor.parse_view_lineage("my_project", "my_dataset", view)
    assert tables is not None
    assert 1 == len(tables)
    assert "my_project.some_dataset.sometable" == tables[0].get_table_name()


def test_one_part_table():
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor = BigqueryLineageExtractor(config, report)

    ddl = "CREATE VIEW my_view as select * from sometable as a"
    view = BigqueryView(
        name="test",
        created=datetime.datetime.now(tz=datetime.timezone.utc),
        last_altered=datetime.datetime.now(tz=datetime.timezone.utc),
        comment="",
        view_definition=ddl,
    )
    tables = extractor.parse_view_lineage("my_project", "my_dataset", view)
    assert tables is not None
    assert 1 == len(tables)
    assert "my_project.my_dataset.sometable" == tables[0].get_table_name()


def test_create_statement_with_multiple_table():
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor = BigqueryLineageExtractor(config, report)

    ddl = "CREATE VIEW my_view as select * from my_project_2.my_dataset_2.sometable union select * from my_project_2.my_dataset_2.sometable2 as a"
    view = BigqueryView(
        name="test",
        created=datetime.datetime.now(),
        last_altered=datetime.datetime.now(),
        comment="",
        view_definition=ddl,
    )
    tables = extractor.parse_view_lineage("my_project", "my_dataset", view)
    assert tables is not None

    tables.sort(key=lambda e: e.get_table_name())
    assert 2 == len(tables)
    assert "my_project_2.my_dataset_2.sometable" == tables[0].get_table_name()
    assert "my_project_2.my_dataset_2.sometable2" == tables[1].get_table_name()


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
        iter(lineage_entries)
    )

    upstream_lineage = extractor.get_lineage_for_table(
        bq_table=bq_table,
        lineage_metadata=lineage_map,
        platform="bigquery",
    )
    assert upstream_lineage
    assert len(upstream_lineage[0].upstreams) == 4
