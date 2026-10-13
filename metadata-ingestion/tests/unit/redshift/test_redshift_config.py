from datahub.ingestion.agent.introspect import declares_qualifier
from datahub.ingestion.source.redshift.config import RedshiftConfig


def test_incremental_lineage_default_to_false():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    assert config.incremental_lineage is False


def test_extract_ownership_defaults_to_false():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    assert config.extract_ownership is False


def test_probe_qualifies_tables_through_the_database_field():
    # RedshiftSource is not a SQLAlchemySource, so the probe's get_identifier
    # shim cannot build `database.schema.table`; the Qualifier on `database`
    # declares it.
    assert declares_qualifier(
        RedshiftConfig(host_port="localhost:5439", database="test")
    )
