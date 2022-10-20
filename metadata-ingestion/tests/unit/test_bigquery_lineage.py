import datetime

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryView
from datahub.ingestion.source.bigquery_v2.lineage import BigqueryLineageExtractor


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
        created=datetime.datetime.now(),
        last_altered=datetime.datetime.now(),
        comment="",
        ddl=ddl,
    )
    tables = extractor.parse_view_lineage("my_project", "my_dataset", view)
    assert 1 == len(tables)
    assert "my-project2.my-dataset2.test_physical_table" == tables[0].get_table_name()


def test_parse_view_lineage_with_two_part_table_name():
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor = BigqueryLineageExtractor(config, report)

    ddl = "CREATE VIEW my_view as select * from some_dataset.sometable as a"
    view = BigqueryView(
        name="test",
        created=datetime.datetime.now(),
        last_altered=datetime.datetime.now(),
        comment="",
        ddl=ddl,
    )
    tables = extractor.parse_view_lineage("my_project", "my_dataset", view)
    assert 1 == len(tables)
    assert "my_project.some_dataset.sometable" == tables[0].get_table_name()


def test_one_part_table():
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor = BigqueryLineageExtractor(config, report)

    ddl = "CREATE VIEW my_view as select * from sometable as a"
    view = BigqueryView(
        name="test",
        created=datetime.datetime.now(),
        last_altered=datetime.datetime.now(),
        comment="",
        ddl=ddl,
    )
    tables = extractor.parse_view_lineage("my_project", "my_dataset", view)
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
        ddl=ddl,
    )
    tables = extractor.parse_view_lineage("my_project", "my_dataset", view)
    tables.sort(key=lambda e: e.get_table_name())
    assert 2 == len(tables)
    assert "my_project_2.my_dataset_2.sometable" == tables[0].get_table_name()
    assert "my_project_2.my_dataset_2.sometable2" == tables[1].get_table_name()
