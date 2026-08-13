"""URI scheme -> DataHub platform mapping.

Its own module, with no imports from the rest of the plugin, because both `_config` (to
canonicalise configured connection keys) and `_connection_mapping` (to canonicalise keys
derived from URIs) need it. Keeping it here lets both import at module scope instead of
one of them hiding a function-local import to dodge a cycle.
"""

# Shared by every writer that names a dataset, so one `[datahub] asset_connections` entry
# serves them all. This matters because the same connection is spelled differently by
# each: an Airflow Asset uses the SQLAlchemy-style `postgresql://`, while an OpenLineage
# namespace uses `postgres://`. Canonicalising the scheme before building the lookup key
# means the user configures `postgres://host:5432` once.
SCHEME_TO_PLATFORM = {
    # object stores / filesystems
    "s3": "s3",
    "s3a": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "file": "file",
    "hdfs": "hdfs",
    "abfs": "adls",
    "abfss": "adls",
    # warehouses / databases
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "sqlserver": "mssql",
    "awsathena": "athena",
}


def platform_for_scheme(scheme: str) -> str:
    return SCHEME_TO_PLATFORM.get(scheme.lower(), scheme.lower())


def scheme_of(uri: object) -> str:
    """Scheme prefix of a URI, without parsing it.

    Groups warnings for URIs that cannot be parsed at all, where no connection key can be
    derived. Coerces to `str` first: `uri` comes from a user-authored Asset, and a
    non-string must not raise inside an error handler.
    """
    # Lowercased so dedup keys built from it are case-insensitive: S3:// and s3:// are the
    # same cause and should not warn twice.
    return str(uri).split("://", maxsplit=1)[0][:40].lower()
