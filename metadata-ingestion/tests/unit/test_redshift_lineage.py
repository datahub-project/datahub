from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.lineage import RedshiftLineageExtractor
from datahub.ingestion.source.redshift.report import RedshiftReport


def test_get_sources_from_query():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    report = RedshiftReport()

    test_query = """
        select * from my_schema.my_table
    """
    lineageExtractor = RedshiftLineageExtractor(config, report)
    lineage_datasets = lineageExtractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]
    assert lineage.path == "test.my_schema.my_table"


def test_get_sources_from_query_with_only_table_name():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    report = RedshiftReport()

    test_query = """
        select * from my_table
    """
    lineageExtractor = RedshiftLineageExtractor(config, report)
    lineage_datasets = lineageExtractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]
    assert lineage.path == "test.public.my_table"


def test_get_sources_from_query_with_database():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    report = RedshiftReport()

    test_query = """
        select * from test.my_schema.my_table
    """
    lineageExtractor = RedshiftLineageExtractor(config, report)
    lineage_datasets = lineageExtractor._get_sources_from_query(
        db_name="test", query=test_query
    )
    assert len(lineage_datasets) == 1

    lineage = lineage_datasets[0]
    assert lineage.path == "test.my_schema.my_table"
