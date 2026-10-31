"""Dremio dataset naming, kept free of Dremio client dependencies so the dbt
source can match the Dremio source's URNs."""

from typing import Iterable, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.schema_resolver import SchemaResolver

DREMIO_PLATFORM = "dremio"

# Dremio uses 'dremio' as the default database name in all SQL contexts
DREMIO_DATABASE_NAME = "dremio"


def dremio_dataset_name(parts: Iterable[Optional[str]]) -> str:
    """The name the Dremio source gives the dataset at this path: ``dremio.<path>``.

    A leading ``dremio`` is the default database, not part of the path, so it is
    dropped rather than doubled.
    """
    path = [part for part in parts if part]
    if path and path[0].lower() == DREMIO_DATABASE_NAME:
        path = path[1:]
    return ".".join([DREMIO_DATABASE_NAME, *path])


class DremioSchemaResolver(SchemaResolver):
    """Custom schema resolver for Dremio multi-part table names.

    Dremio uses 'dremio' as the database in all URNs. For multi-part tables (>3 parts),
    uses table.parts to preserve the full hierarchy: dremio.part1.part2.part3.table

    Note: Folder names containing dots (e.g., "folder.with.dots") create ambiguous URNs
    in SQL parsing since dots are path delimiters. However, catalog-based ingestion
    correctly handles these via the Dremio API's incremental path resolution. See
    dremio_api.DremioAPIOperations.get_dataset_id() for implementation details.
    """

    def _construct_table_name(self, table: _TableName) -> str:
        # Up to 3 parts, database/db_schema/table carry the same name. They are also
        # the only reliable fields when a CTE was swapped for a table (dbt
        # ephemeral models): sqlglot then reports just the table in `parts`.
        if table.parts and len(table.parts) > 3:
            return dremio_dataset_name(table.parts)
        return dremio_dataset_name([table.database, table.db_schema, table.table])

    def get_urn_for_table(
        self, table: _TableName, lower: bool = False, mixed: bool = False
    ) -> str:
        table_name = self._construct_table_name(table)
        platform_instance = self.platform_instance

        if lower:
            table_name = table_name.lower()
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
