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
    CustomCatalogType,
    HiveTableType,
    Metastore,
    Schema,
    Table,
)

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

column_stat_property_map = {
    "num_nulls": "spark.sql.statistics.colStats.{column}.nullCount",
    "distinct_count": "spark.sql.statistics.colStats.{column}.distinctCount",
    "min": "spark.sql.statistics.colStats.{column}.min",
    "max": "spark.sql.statistics.colStats.{column}.max",
    "avg_col_len": "spark.sql.statistics.colStats.{column}.avgLen",
    "max_col_len": "spark.sql.statistics.colStats.{column}.maxLen",
    "version": "spark.sql.statistics.colStats.{column}.version",
}

table_stats_property_map = {
    "rows": "spark.sql.statistics.numRows",
    "bytes": "spark.sql.statistics.totalSize",
}


class HiveMetastoreProxy(Closeable):
    # TODO: Support for view lineage using SQL parsing
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

    def __init__(self, sqlalchemy_url: str, options: dict) -> None:
        try:
            self.inspector = HiveMetastoreProxy.get_inspector(sqlalchemy_url, options)
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
        views = self.inspector.get_view_names(schema.name)
        for table_name in views:
            yield self._get_table(schema, table_name, True)

        for table_name in self.inspector.get_table_names(schema.name):
            if table_name in views:
                continue
            yield self._get_table(schema, table_name, False)

    def _get_table(
        self,
        schema: Schema,
        table_name: str,
        is_view: bool = False,
        include_column_stats: bool = False,
    ) -> Table:
        columns = self._get_columns(schema, table_name)
        detailed_info = self._get_table_info(schema, table_name)

        comment = detailed_info.pop("Comment", None)
        storage_location = detailed_info.pop("Location", None)
        datasource_format = self._get_datasource_format(
            detailed_info.pop("Provider", None)
        )

        if detailed_info.get("Statistics"):
            table_stats = self._get_cached_table_statistics(
                detailed_info.pop("Statistics")
            )
            detailed_info.update(table_stats)

        if include_column_stats:
            column_stats = self._get_cached_column_statistics(
                schema, table_name, columns
            )
            detailed_info.update(column_stats)

        created_at = self._get_created_at(detailed_info.pop("Created Time", None))

        return Table(
            name=table_name,
            id=f"{schema.id}.{table_name}",
            table_type=self._get_table_type(detailed_info.pop("Type", None)),
            schema=schema,
            columns=columns,
            storage_location=storage_location,
            data_source_format=datasource_format,
            view_definition=self._get_view_definition(schema.name, table_name)
            if is_view
            else None,
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

    def _get_cached_table_statistics(self, statistics: str) -> dict:
        # statistics is in format "xx bytes" OR "1382 bytes, 2 rows"
        table_stats = dict()
        for prop in statistics.split(","):
            value_key_list = prop.split(" ")  # value_key_list -> [value, key]
            if (
                len(value_key_list) == 2
                and value_key_list[1] in table_stats_property_map
            ):
                table_stats[
                    table_stats_property_map[value_key_list[1]]
                ] = value_key_list[0]

        return table_stats

    def _get_cached_column_statistics(
        self, schema: Schema, table_name: str, columns: List[Column]
    ) -> dict:
        col_stats = dict()
        for col in columns:
            props = self._column_describe_extended(schema.name, table_name, col.name)
            for prop in props:
                # prop[0]->info_name, prop[1]->info_value
                if prop[0] in column_stat_property_map:
                    col_stats[
                        column_stat_property_map[prop[0]].format(column=col.name)
                    ] = prop[1]

        return col_stats

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
        except Exception:
            logger.debug(
                f"Failed to get view definition for {schema_name}.{table_name}"
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

    def _get_table_info(self, schema: Schema, table_name: str) -> dict:
        rows = self._describe_extended(schema.name, table_name)

        index = rows.index(("# Detailed Table Information", "", ""))
        rows = rows[index + 1 :]
        # Copied from https://github.com/acryldata/PyHive/blob/master/pyhive/sqlalchemy_hive.py#L375
        # Generate properties dictionary.
        properties = {}
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
                prop_name = "{} {}".format(active_heading, data_type.rstrip())
                properties[prop_name] = value.rstrip()

        return properties

    def _get_columns(self, schema: Schema, table_name: str) -> List[Column]:
        rows = self._describe_extended(schema.name, table_name)

        columns: List[Column] = []
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
                    id=f"{schema.id}.{table_name}.{row[0].strip()}",
                    type_text=row[1].strip(),
                    type_name=type_map.get(row[1].strip().lower()),
                    type_scale=None,
                    type_precision=None,
                    position=None,
                    nullable=None,
                    comment=row[2],
                )
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
