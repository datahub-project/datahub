from datahub.ingestion.source.redshift.config import RedshiftConfig


def test_incremental_lineage_default_to_false():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    assert config.incremental_lineage is False


def test_extract_ownership_defaults_to_false():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    assert config.extract_ownership is False
