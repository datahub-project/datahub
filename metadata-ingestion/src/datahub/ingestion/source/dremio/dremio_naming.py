"""Dremio dataset naming, kept free of Dremio client dependencies so the dbt
source can match the Dremio source's URNs."""

from typing import Iterable, Optional, Sequence

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.schema_resolver import SchemaResolver

DREMIO_PLATFORM = "dremio"

# Dremio uses 'dremio' as the default database name in all SQL contexts
DREMIO_DATABASE_NAME = "dremio"

# dbt-dremio stores a model with no folder under this schema and leaves it out of
# the SQL it renders, so the dataset sits directly in its space or source.
DBT_DREMIO_NO_SCHEMA = "no_schema"


def is_dremio_platform(platform: str) -> bool:
    return platform.lower() == DREMIO_PLATFORM


def dremio_dataset_name(path: Iterable[Optional[str]]) -> str:
    """The name the Dremio source gives the dataset at this path: ``dremio.<path>``,
    lowercased. Every name the Dremio source emits goes through here."""
    return ".".join([DREMIO_DATABASE_NAME, *(part for part in path if part)]).lower()


def dremio_dataset_name_from_sql(parts: Sequence[Optional[str]]) -> str:
    """The dataset a name in Dremio SQL refers to.

    A leading ``dremio`` is the default database, written in the SQL or filled in
    by sqlglot from default_db; it is not part of the dataset's path.
    """
    if parts and parts[0] and parts[0].lower() == DREMIO_DATABASE_NAME:
        parts = parts[1:]
    return dremio_dataset_name(parts)


class DremioSchemaResolver(SchemaResolver):
    """Custom schema resolver for Dremio multi-part table names.

    Dremio uses 'dremio' as the database in all URNs. For multi-part tables (>3 parts),
    uses table.parts to preserve the full hierarchy: dremio.part1.part2.part3.table

    Names are always lowercase, like every name the Dremio source emits, so
    ``get_urn_for_table``'s ``lower`` only affects the platform instance.

    ``strip_default_database`` drops a leading ``dremio`` from parsed names. The
    Dremio source needs it: it parses with ``default_db="dremio"``, so sqlglot
    fills that in. dbt must not strip: it passes no default_db and dbt-dremio
    renders full paths, so a leading ``dremio`` there is a real space or source
    (see DbtDremioSchemaResolver).

    Note: Folder names containing dots (e.g., "folder.with.dots") create ambiguous URNs
    in SQL parsing since dots are path delimiters. However, catalog-based ingestion
    correctly handles these via the Dremio API's incremental path resolution. See
    dremio_api.DremioAPIOperations.get_dataset_id() for implementation details.
    """

    strip_default_database = True

    def _name_from_sql(self, parts: Sequence[Optional[str]]) -> str:
        if self.strip_default_database:
            return dremio_dataset_name_from_sql(parts)
        return dremio_dataset_name(parts)

    def _construct_table_name(self, table: _TableName) -> str:
        # Up to 3 parts, database/db_schema/table carry the same name. They are also
        # the only reliable fields when a CTE was swapped for a table (dbt
        # ephemeral models): sqlglot then reports just the table in `parts`.
        if table.parts and len(table.parts) > 3:
            return self._name_from_sql(table.parts)
        return self._name_from_sql([table.database, table.db_schema, table.table])

    def get_urn_for_table(
        self, table: _TableName, lower: bool = False, mixed: bool = False
    ) -> str:
        table_name = self._construct_table_name(table)
        platform_instance = self.platform_instance

        if lower:
            if not mixed:
                platform_instance = (
                    platform_instance.lower() if platform_instance else None
                )

        urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=table_name,
            env=self.env,
            platform_instance=platform_instance,
        )
        return urn


class DbtDremioSchemaResolver(DremioSchemaResolver):
    """DremioSchemaResolver for dbt's compiled SQL, which never omits the space
    or source, so a leading ``dremio`` is kept."""

    strip_default_database = False
