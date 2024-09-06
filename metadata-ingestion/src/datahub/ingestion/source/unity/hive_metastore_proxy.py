import logging
from datetime import datetime
from functools import lru_cache
from typing import Iterable, List, Optional

from databricks.sdk.service.catalog import ColumnTypeName, DataSourceFormat
from databricks.sql.types import Row
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Column,
    ColumnProfile,
    CustomCatalogType,
    HiveTableType,
    Metastore,
    Schema,
    Table,
    TableProfile,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport

logger = logging.getLogger(__name__)
HIVE_METASTORE = "hive_metastore"

type_map = {
    "boolean": ColumnTypeName.BOOLEAN,
    "tinyint": ColumnTypeName.INT,
    "smallint": ColumnTypeName.INT,
    "int": ColumnTypeName.INT,
    "bigint": ColumnTypeName.LONG,
    "float": ColumnTypeName.FLOAT,
    "double": ColumnTypeName.DOUBLE,
    "decimal": ColumnTypeName.DECIMAL,
    "string": ColumnTypeName.STRING,
    "varchar": ColumnTypeName.STRING,
    "timestamp": ColumnTypeName.TIMESTAMP,
    "date": ColumnTypeName.DATE,
    "binary": ColumnTypeName.BINARY,
}

NUM_NULLS = "num_nulls"
DISTINCT_COUNT = "distinct_count"
MIN = "min"
MAX = "max"
AVG_COL_LEN = "avg_col_len"
MAX_COL_LEN = "max_col_len"
VERSION = "version"

ROWS = "rows"
BYTES = "bytes"
TABLE_STAT_LIST = {ROWS, BYTES}


class HiveMetastoreProxy(Closeable):
    # Why not use hive ingestion source directly here ?
    # 1. hive ingestion source assumes 2-level namespace heirarchy and currently
    #    there is no other intermediate interface except sqlalchemy inspector
    #    that can be used to fetch hive metadata.
    # 2. hive recipe for databricks (databricks+pyhive dialect) does not
    #    readily support SQL warehouse. Also this dialect is not actively maintained.
    """
    Proxy to read metadata from hive_metastore databricks catalog. This is required
    as unity catalog apis do not return details about this legacy metastore.
    """

    def __init__(
        self, sqlalchemy_url: str, options: dict, report: UnityCatalogReport
    ) -> None:
        try:
            self.inspector = HiveMetastoreProxy.get_inspector(sqlalchemy_url, options)
            self.report = report
        except Exception:
            # This means that there is no `hive_metastore` catalog in databricks workspace
            # Not tested but seems like the logical conclusion.
            raise

    @staticmethod
    def get_inspector(sqlalchemy_url: str, options: dict) -> Inspector:
        engine = create_engine(sqlalchemy_url, **options)
        return inspect(engine.connect())

    def hive_metastore_catalog(self, metastore: Optional[Metastore]) -> Catalog:
        return Catalog(
            id=f"{metastore.id}.{HIVE_METASTORE}" if metastore else HIVE_METASTORE,
            name=HIVE_METASTORE,
            comment=None,
            metastore=metastore,
            owner=None,
            type=CustomCatalogType.HIVE_METASTORE_CATALOG,
        )

    def hive_metastore_schemas(self, catalog: Catalog) -> Iterable[Schema]:
        for schema_name in self.inspector.get_schema_names():
            yield Schema(
                name=schema_name,
                id=f"{catalog.id}.{schema_name}",
                catalog=catalog,
                comment=None,
                owner=None,
            )

    def hive_metastore_tables(self, schema: Schema) -> Iterable[Table]:
        # NOTE: Ideally, we use `inspector.get_view_names` and `inspector.get_table_names` here instead of
        # making show queries in this class however Databricks dialect for databricks-sql-connector<3.0.0 does not
        # back-quote schemas with special char such as hyphen.
        # Currently, databricks-sql-connector is pinned to <3.0.0 due to requirement of SQLAlchemy > 2.0.21 for
        # later versions.
        views = self.get_view_names(schema.name)
        for table_name in views:
            yield self._get_table(schema, table_name, True)

        for table_name in self.get_table_names(schema.name):
            if table_name in views:
                continue
            yield self._get_table(schema, table_name, False)

    def get_table_names(self, schema_name: str) -> List[str]:
        try:
            rows = self._execute_sql(f"SHOW TABLES FROM `{schema_name}`")
            # 3 columns - database, tableName, isTemporary
            return [row.tableName for row in rows]
        except Exception as e:
            self.report.report_warning(
                "Failed to get tables for schema", f"{HIVE_METASTORE}.{schema_name}"
            )
            logger.warning(
                f"Failed to get tables {schema_name} due to {e}", exc_info=True
            )
        return []

    def get_view_names(self, schema_name: str) -> List[str]:
        try:
            rows = self._execute_sql(f"SHOW VIEWS FROM `{schema_name}`")
            # 4 columns - namespace, viewName, isTemporary, isMaterialized
            return [row.viewName for row in rows]
        except Exception as e:
            self.report.report_warning("Failed to get views for schema", schema_name)
            logger.warning(
                f"Failed to get views {schema_name} due to {e}", exc_info=True
            )
        return []

    def _get_table(
        self,
        schema: Schema,
        table_name: str,
        is_view: bool = False,
    ) -> Table:
        columns = self._get_columns(schema.name, table_name)
        detailed_info = self._get_table_info(schema.name, table_name)

        comment = detailed_info.pop("Comment", None)
        storage_location = detailed_info.pop("Location", None)
        datasource_format = self._get_datasource_format(
            detailed_info.pop("Provider", None)
        )

        created_at = self._get_created_at(detailed_info.pop("Created Time", None))

        return Table(
            name=table_name,
            id=f"{schema.id}.{table_name}",
            table_type=self._get_table_type(detailed_info.pop("Type", None)),
            schema=schema,
            columns=columns,
            storage_location=storage_location,
            data_source_format=datasource_format,
            view_definition=(
                self._get_view_definition(schema.name, table_name) if is_view else None
            ),
            properties=detailed_info,
            owner=None,
            generation=None,
            created_at=created_at,
            created_by=None,
            updated_at=None,
            updated_by=None,
            table_id=f"{schema.id}.{table_name}",
            comment=comment,
        )

    def get_table_profile(
        self, ref: TableReference, include_column_stats: bool = False
    ) -> Optional[TableProfile]:
        columns = self._get_columns(
            ref.schema,
            ref.table,
        )
        detailed_info = self._get_table_info(ref.schema, ref.table)

        if not columns and not detailed_info:
            return None

        table_stats = (
            self._get_cached_table_statistics(detailed_info["Statistics"])
            if detailed_info.get("Statistics")
            else {}
        )

        column_profiles: List[ColumnProfile] = []
        if include_column_stats:
            for column in columns:
                column_profile = self._get_column_profile(column.name, ref)
                if column_profile:
                    column_profiles.append(column_profile)

        return TableProfile(
            num_rows=(
                int(table_stats[ROWS]) if table_stats.get(ROWS) is not None else None
            ),
            total_size=(
                int(table_stats[BYTES]) if table_stats.get(BYTES) is not None else None
            ),
            num_columns=len(columns),
            column_profiles=column_profiles,
        )

    def _get_column_profile(
        self, column: str, ref: TableReference
    ) -> Optional[ColumnProfile]:
        try:
            props = self._column_describe_extended(ref.schema, ref.table, column)
            col_stats = {}
            for prop in props:
                col_stats[prop[0]] = prop[1]
            return ColumnProfile(
                name=column,
                null_count=(
                    int(col_stats[NUM_NULLS])
                    if col_stats.get(NUM_NULLS) is not None
                    else None
                ),
                distinct_count=(
                    int(col_stats[DISTINCT_COUNT])
                    if col_stats.get(DISTINCT_COUNT) is not None
                    else None
                ),
                min=col_stats.get(MIN),
                max=col_stats.get(MAX),
                avg_len=col_stats.get(AVG_COL_LEN),
                max_len=col_stats.get(MAX_COL_LEN),
                version=col_stats.get(VERSION),
            )
        except Exception as e:
            logger.debug(f"Failed to get column profile for {ref}.{column} due to {e}")
            return None

    def _get_cached_table_statistics(self, statistics: str) -> dict:
        # statistics is in format "xx bytes" OR "1382 bytes, 2 rows"
        table_stats = dict()
        for prop in statistics.split(","):
            value_key_list = prop.strip().split(" ")  # value_key_list -> [value, key]
            if len(value_key_list) == 2 and value_key_list[1] in TABLE_STAT_LIST:
                table_stats[value_key_list[1]] = value_key_list[0]

        return table_stats

    def _get_created_at(self, created_at: Optional[str]) -> Optional[datetime]:
        return (
            datetime.strptime(created_at, "%a %b %d %H:%M:%S %Z %Y")
            if created_at
            else None
        )

    def _get_datasource_format(
        self, provider: Optional[str]
    ) -> Optional[DataSourceFormat]:
        raw_format = provider
        if raw_format:
            try:
                return DataSourceFormat(raw_format.upper())
            except Exception:
                logger.debug(f"Unknown datasource format : {raw_format}")
                pass
        return None

    def _get_view_definition(self, schema_name: str, table_name: str) -> Optional[str]:
        try:
            rows = self._execute_sql(
                f"SHOW CREATE TABLE `{schema_name}`.`{table_name}`"
            )
            for row in rows:
                return row[0]
        except Exception as e:
            self.report.report_warning(
                "Failed to get view definition for table",
                f"{HIVE_METASTORE}.{schema_name}.{table_name}",
            )
            logger.debug(
                f"Failed to get view definition for {schema_name}.{table_name} due to {e}",
                exc_info=True,
            )
        return None

    def _get_table_type(self, type: Optional[str]) -> HiveTableType:
        if type == "EXTERNAL":
            return HiveTableType.HIVE_EXTERNAL_TABLE
        elif type == "MANAGED":
            return HiveTableType.HIVE_MANAGED_TABLE
        elif type == "VIEW":
            return HiveTableType.HIVE_VIEW
        else:
            return HiveTableType.UNKNOWN

    @lru_cache(maxsize=1)
    def _get_table_info(self, schema_name: str, table_name: str) -> dict:
        # Generate properties dictionary.
        properties = {}

        try:
            rows = self._describe_extended(schema_name, table_name)

            index = rows.index(("# Detailed Table Information", "", ""))
            rows = rows[index + 1 :]
            # Copied from https://github.com/acryldata/PyHive/blob/master/pyhive/sqlalchemy_hive.py#L375

            active_heading = None
            for col_name, data_type, value in rows:
                col_name = col_name.rstrip()
                if col_name.startswith("# "):
                    continue
                elif col_name == "" and data_type is None:
                    active_heading = None
                    continue
                elif col_name != "" and data_type is None:
                    active_heading = col_name
                elif col_name != "" and data_type is not None:
                    properties[col_name] = data_type.strip()
                else:
                    # col_name == "", data_type is not None
                    prop_name = f"{active_heading} {data_type.rstrip()}"
                    properties[prop_name] = value.rstrip()
        except Exception as e:
            self.report.report_warning(
                "Failed to get detailed info for table",
                f"{HIVE_METASTORE}.{schema_name}.{table_name}",
            )
            logger.debug(
                f"Failed to get detailed info for table {schema_name}.{table_name} due to {e}",
                exc_info=True,
            )
        return properties

    @lru_cache(maxsize=1)
    def _get_columns(self, schema_name: str, table_name: str) -> List[Column]:
        columns: List[Column] = []
        try:
            rows = self._describe_extended(schema_name, table_name)
            for i, row in enumerate(rows):
                if i == 0 and row[0].strip() == "col_name":
                    continue  # first row
                if row[0].strip() in (
                    "",
                    "# Partition Information",
                    "# Detailed Table Information",
                ):
                    break
                columns.append(
                    Column(
                        name=row[0].strip(),
                        id=f"{HIVE_METASTORE}.{schema_name}.{table_name}.{row[0].strip()}",
                        type_text=row[1].strip(),
                        type_name=type_map.get(row[1].strip().lower()),
                        type_scale=None,
                        type_precision=None,
                        position=None,
                        nullable=None,
                        comment=row[2],
                    )
                )
        except Exception as e:
            self.report.report_warning(
                "Failed to get columns for table",
                f"{HIVE_METASTORE}.{schema_name}.{table_name}",
            )
            logger.debug(
                f"Failed to get columns for table {schema_name}.{table_name} due to {e}",
                exc_info=True,
            )
        return columns

    @lru_cache(maxsize=1)
    def _describe_extended(self, schema_name: str, table_name: str) -> List[Row]:
        """
        Rows are structured as shown in examples here
        https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html#examples
        """
        return self._execute_sql(f"DESCRIBE EXTENDED `{schema_name}`.`{table_name}`")

    def _column_describe_extended(
        self, schema_name: str, table_name: str, column_name: str
    ) -> List[Row]:
        """
        Rows are structured as shown in examples here
        https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html#examples
        """
        return self._execute_sql(
            f"DESCRIBE EXTENDED `{schema_name}`.`{table_name}` {column_name}"
        )

    def _execute_sql(self, sql: str) -> List[Row]:
        return self.inspector.bind.execute(sql).fetchall()

    def close(self):
        self.inspector.bind.close()  # type:ignore
