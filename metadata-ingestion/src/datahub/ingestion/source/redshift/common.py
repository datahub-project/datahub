from datahub.ingestion.source.redshift.config import RedshiftConfig

redshift_datetime_format = "%Y-%m-%d %H:%M:%S"


def get_db_name(config: RedshiftConfig) -> str:
    db_name = getattr(config, "database")
    db_alias = getattr(config, "database_alias")
    if db_alias:
        db_name = db_alias
    return db_name
