from contextlib import closing
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.external_dq.contract import LogicalType
from datahub.ingestion.source.external_dq.extractor import SelectColumn
from datahub.ingestion.source.external_dq.validate import PhysicalColumn
from datahub.ingestion.source.unity.identifier_helper import (
    quote_databricks_identifier,
    split_databricks_identifier,
)
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import TableReference


def _parts(table: str) -> Tuple[str, str, str]:
    parts = split_databricks_identifier(table)
    if not parts or len(parts) != 3:
        raise ValueError(f"expected catalog.schema.table, got {table!r}")
    return parts[0], parts[1], parts[2]


def _fqn(table: str) -> str:
    return ".".join(quote_databricks_identifier(p) for p in _parts(table))


def _select_list(columns: Sequence[SelectColumn]) -> str:
    items = []
    for name, logical_type in columns:
        column, alias = (
            quote_databricks_identifier(name),
            quote_databricks_identifier(name.lower()),
        )
        if logical_type is LogicalType.TIMESTAMP:
            # Epoch millis sidesteps the SQL warehouse's session time zone.
            items.append(f"unix_millis({column}) AS {alias}")
        else:
            items.append(f"{column} AS {alias}")
    return ", ".join(items)


class UnityExternalDQReader:
    def __init__(self, proxy: UnityCatalogApiProxy) -> None:
        self.proxy = proxy

    def describe(self, table: str) -> List[PhysicalColumn]:
        catalog, schema, name = _parts(table)
        return [
            PhysicalColumn(name=column, data_type=data_type, position=position)
            for column, data_type, position in self.proxy.describe_table_columns(
                catalog, schema, name
            )
        ]

    def read_rules(
        self, table: str, columns: Sequence[SelectColumn]
    ) -> Iterable[Mapping[str, Any]]:
        query = f"SELECT {_select_list(columns)} FROM {_fqn(table)}"
        for row in self.proxy.iter_sql_rows(query):
            yield row.asDict()

    def read_results(
        self, table: str, columns: Sequence[SelectColumn], since_millis: int
    ) -> Iterable[Mapping[str, Any]]:
        # Filtering on the raw column (not unix_millis(col)) keeps Delta data skipping.
        query = (
            f"SELECT {_select_list(columns)} FROM {_fqn(table)} "
            "WHERE `executed_at` >= timestamp_millis(%s) "
            "ORDER BY `executed_at`, `run_id`"
        )
        for row in self.proxy.iter_sql_rows(query, [since_millis]):
            yield row.asDict()

    def count_results_before(self, table: str, before_millis: int) -> int:
        query = (
            f"SELECT count(*) AS n FROM {_fqn(table)} "
            "WHERE `executed_at` < timestamp_millis(%s)"
        )
        with closing(self.proxy.iter_sql_rows(query, [before_millis])) as rows:
            row = next(rows, None)
        # COUNT(*) always returns one row; raise rather than report a false 0.
        if row is None:
            raise ValueError(f"count query returned no rows for {table}")
        return int(row.asDict()["n"])


class UnityDatasetLocator:
    """Resolves contract paths to datasets this connector run actually ingested,
    using the connector's own URN builder (metastore prefix, platform instance, env)."""

    def __init__(
        self,
        table_refs: Iterable[TableReference],
        urn_builder: Callable[[TableReference], str],
        field_names: Optional[Callable[[str], Optional[Iterable[str]]]] = None,
    ) -> None:
        self._refs: Dict[Tuple[str, str, str], TableReference] = {
            (r.catalog.lower(), r.schema.lower(), r.table.lower()): r
            for r in table_refs
        }
        self._urn_builder = urn_builder
        self._field_names = field_names

    def dataset_urn(self, dataset_path: Sequence[str]) -> Optional[str]:
        if len(dataset_path) != 3:
            return None
        key = (
            dataset_path[0].lower(),
            dataset_path[1].lower(),
            dataset_path[2].lower(),
        )
        ref = self._refs.get(key)
        return self._urn_builder(ref) if ref else None

    def field_urn(self, dataset_urn: str, column_path: str) -> str:
        # schemaField URNs are case-sensitive on the field path, and Unity emits
        # top-level columns with their original casing. Nested (struct) fields use
        # v2 field paths and are passed through unchanged.
        names = self._field_names(dataset_urn) if self._field_names else None
        if names:
            by_lower = {name.lower(): name for name in names}
            column_path = by_lower.get(column_path.lower(), column_path)
        return make_schema_field_urn(dataset_urn, column_path)
