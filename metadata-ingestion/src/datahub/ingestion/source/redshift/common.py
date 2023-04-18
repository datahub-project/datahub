from datahub.ingestion.source.redshift.config import RedshiftConfig

redshift_datetime_format = "%Y-%m-%d %H:%M:%S"


def get_db_name(config: RedshiftConfig) -> str:
    db_name = config.database
    db_alias = config.database_alias

    db_name = db_alias or db_name
    assert db_name is not None, "database name or alias must be specified"
    return db_name
