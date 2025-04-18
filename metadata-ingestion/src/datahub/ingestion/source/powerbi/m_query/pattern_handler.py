import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Tuple, Type, cast

from lark import Tree

from datahub.configuration.source_common import PlatformDetail
from datahub.emitter import mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    Constant,
    DataBricksPlatformDetail,
    DataPlatformPair,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    PowerBIPlatformDetail,
    SupportedDataPlatform,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
)
from datahub.ingestion.source.powerbi.m_query import native_sql_parser, tree_function
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataAccessFunctionDetail,
    DataPlatformTable,
    FunctionName,
    IdentifierAccessor,
    Lineage,
    ReferencedTable,
)
from datahub.ingestion.source.powerbi.m_query.odbc import (
    extract_dsn,
    extract_platform,
    extract_server,
    normalize_platform_name,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table
from datahub.metadata.schema_classes import SchemaFieldDataTypeClass
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingResult,
)

logger = logging.getLogger(__name__)


def get_next_item(items: List[str], item: str) -> Optional[str]:
    if item in items:
        try:
            index = items.index(item)
            return items[index + 1]
        except IndexError:
            logger.debug(f'item:"{item}", not found in item-list: {items}')
    return None


def urn_to_lowercase(value: str, flag: bool) -> str:
    if flag is True:
        return value.lower()

    return value


def make_urn(
    config: PowerBiDashboardSourceConfig,
    platform_instance_resolver: AbstractDataPlatformInstanceResolver,
    data_platform_pair: DataPlatformPair,
    server: str,
    qualified_table_name: str,
) -> str:
    platform_detail: PlatformDetail = platform_instance_resolver.get_platform_instance(
        PowerBIPlatformDetail(
            data_platform_pair=data_platform_pair,
            data_platform_server=server,
        )
    )

    return builder.make_dataset_urn_with_platform_instance(
        platform=data_platform_pair.datahub_data_platform_name,
        platform_instance=platform_detail.platform_instance,
        env=platform_detail.env,
        name=urn_to_lowercase(
            qualified_table_name, config.convert_lineage_urns_to_lowercase
        ),
    )


class AbstractLineage(ABC):
    """
    Base class to share common functionalities among different dataplatform for M-Query parsing.

    To create qualified table name we need to parse M-Query data-access-functions(https://learn.microsoft.com/en-us/powerquery-m/accessing-data-functions) and
    the data-access-functions has some define pattern to access database-name, schema-name and table-name, for example, see below M-Query.

        let
            Source = Sql.Database("localhost", "library"),
            dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]
        in
            dbo_book_issue

    It is MSSQL M-Query and Sql.Database is the data-access-function to access MSSQL. If this function is available in M-Query then database name is available in the second argument of the first statement and schema-name and table-name is available in the second statement. the second statement can be repeated to access different tables from MSSQL.

    DefaultTwoStepDataAccessSources extends the AbstractDataPlatformTableCreator and provides the common functionalities for data-platform which has above type of M-Query pattern

    data-access-function varies as per data-platform for example for MySQL.Database for MySQL, PostgreSQL.Database for Postgres and Oracle.Database for Oracle and number of statement to
    find out database-name , schema-name and table-name also varies as per dataplatform.

    Value.NativeQuery is one of the functions which is used to execute a native query inside M-Query, for example see below M-Query

        let
            Source = Value.NativeQuery(AmazonRedshift.Database("redshift-url","dev"), "select * from dev.public.category", null, [EnableFolding=true])
        in
            Source

    In this M-Query database-name is available in first argument and rest of the detail i.e database & schema is available in native query.

    NativeQueryDataPlatformTableCreator extends AbstractDataPlatformTableCreator to support Redshift and Snowflake native query parsing.

    """

    ctx: PipelineContext
    table: Table
    config: PowerBiDashboardSourceConfig
    reporter: PowerBiDashboardSourceReport
    platform_instance_resolver: AbstractDataPlatformInstanceResolver

    def __init__(
        self,
        ctx: PipelineContext,
        table: Table,
        config: PowerBiDashboardSourceConfig,
        reporter: PowerBiDashboardSourceReport,
        platform_instance_resolver: AbstractDataPlatformInstanceResolver,
    ) -> None:
        super().__init__()
        self.ctx = ctx
        self.table = table
        self.config = config
        self.reporter = reporter
        self.platform_instance_resolver = platform_instance_resolver

    @abstractmethod
    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        pass

    @abstractmethod
    def get_platform_pair(self) -> DataPlatformPair:
        pass

    @staticmethod
    def get_db_detail_from_argument(
        arg_list: Tree,
    ) -> Tuple[Optional[str], Optional[str]]:
        arguments: List[str] = tree_function.strip_char_from_list(
            values=tree_function.remove_whitespaces_from_list(
                tree_function.token_values(arg_list)
            ),
        )
        logger.debug(f"DB Details: {arguments}")

        if len(arguments) < 2:
            logger.debug(f"Expected minimum 2 arguments, but got {len(arguments)}")
            return None, None

        return arguments[0], arguments[1]

    @staticmethod
    def create_reference_table(
        arg_list: Tree,
        table_detail: Dict[str, str],
    ) -> Optional[ReferencedTable]:
        arguments: List[str] = tree_function.strip_char_from_list(
            values=tree_function.remove_whitespaces_from_list(
                tree_function.token_values(arg_list)
            ),
        )

        logger.debug(f"Processing arguments {arguments}")

        if (
            len(arguments) >= 4  # [0] is warehouse FQDN.
            # [1] is endpoint, we are not using it.
            # [2] is "Catalog" key
            # [3] is catalog's value
        ):
            return ReferencedTable(
                warehouse=arguments[0],
                catalog=arguments[3],
                # As per my observation, database and catalog names are same in M-Query
                database=table_detail["Database"]
                if table_detail.get("Database")
                else arguments[3],
                schema=table_detail["Schema"],
                table=table_detail.get("Table") or table_detail["View"],
            )
        elif len(arguments) == 2:
            return ReferencedTable(
                warehouse=arguments[0],
                database=table_detail["Database"],
                schema=table_detail["Schema"],
                table=table_detail.get("Table") or table_detail["View"],
                catalog=None,
            )

        return None

    def parse_custom_sql(
        self, query: str, server: str, database: Optional[str], schema: Optional[str]
    ) -> Lineage:
        dataplatform_tables: List[DataPlatformTable] = []

        platform_detail: PlatformDetail = (
            self.platform_instance_resolver.get_platform_instance(
                PowerBIPlatformDetail(
                    data_platform_pair=self.get_platform_pair(),
                    data_platform_server=server,
                )
            )
        )

        query = native_sql_parser.remove_drop_statement(
            native_sql_parser.remove_special_characters(query)
        )

        parsed_result: Optional["SqlParsingResult"] = (
            native_sql_parser.parse_custom_sql(
                ctx=self.ctx,
                query=query,
                platform=self.get_platform_pair().datahub_data_platform_name,
                platform_instance=platform_detail.platform_instance,
                env=platform_detail.env,
                database=database,
                schema=schema,
            )
        )

        if parsed_result is None:
            self.reporter.info(
                title=Constant.SQL_PARSING_FAILURE,
                message="Fail to parse native sql present in PowerBI M-Query",
                context=f"table-name={self.table.full_name}, sql={query}",
            )
            return Lineage.empty()

        if parsed_result.debug_info and parsed_result.debug_info.table_error:
            self.reporter.warning(
                title=Constant.SQL_PARSING_FAILURE,
                message="Fail to parse native sql present in PowerBI M-Query",
                context=f"table-name={self.table.full_name}, error={parsed_result.debug_info.table_error},sql={query}",
            )
            return Lineage.empty()

        for urn in parsed_result.in_tables:
            dataplatform_tables.append(
                DataPlatformTable(
                    data_platform_pair=self.get_platform_pair(),
                    urn=urn,
                )
            )

        logger.debug(f"Native Query parsed result={parsed_result}")
        logger.debug(f"Generated dataplatform_tables={dataplatform_tables}")

        return Lineage(
            upstreams=dataplatform_tables,
            column_lineage=(
                parsed_result.column_lineage
                if parsed_result.column_lineage is not None
                else []
            ),
        )

    def create_table_column_lineage(self, urn: str) -> List[ColumnLineageInfo]:
        column_lineage = []

        if self.table.columns is not None:
            for column in self.table.columns:
                downstream = DownstreamColumnRef(
                    table=self.table.name,
                    column=column.name,
                    column_type=SchemaFieldDataTypeClass(type=column.datahubDataType),
                    native_column_type=column.dataType or "UNKNOWN",
                )

                upstreams = [
                    ColumnRef(
                        table=urn,
                        column=column.name.lower(),
                    )
                ]

                column_lineage_info = ColumnLineageInfo(
                    downstream=downstream, upstreams=upstreams
                )

                column_lineage.append(column_lineage_info)

        return column_lineage


class AmazonRedshiftLineage(AbstractLineage):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.AMAZON_REDSHIFT.value

    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        logger.debug(
            f"Processing AmazonRedshift data-access function detail {data_access_func_detail}"
        )

        server, db_name = self.get_db_detail_from_argument(
            data_access_func_detail.arg_list
        )
        if db_name is None or server is None:
            return Lineage.empty()  # Return an empty list

        schema_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Name"]

        table_name: str = cast(
            IdentifierAccessor,
            cast(IdentifierAccessor, data_access_func_detail.identifier_accessor).next,
        ).items["Name"]

        qualified_table_name: str = f"{db_name}.{schema_name}.{table_name}"

        urn = make_urn(
            config=self.config,
            platform_instance_resolver=self.platform_instance_resolver,
            data_platform_pair=self.get_platform_pair(),
            server=server,
            qualified_table_name=qualified_table_name,
        )

        column_lineage = self.create_table_column_lineage(urn)

        return Lineage(
            upstreams=[
                DataPlatformTable(
                    data_platform_pair=self.get_platform_pair(),
                    urn=urn,
                )
            ],
            column_lineage=column_lineage,
        )


class OracleLineage(AbstractLineage):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.ORACLE.value

    @staticmethod
    def _get_server_and_db_name(value: str) -> Tuple[Optional[str], Optional[str]]:
        error_message: str = (
            f"The target argument ({value}) should in the format of <host-name>:<port>/<db-name>["
            ".<domain>]"
        )
        splitter_result: List[str] = value.split("/")
        if len(splitter_result) != 2:
            logger.debug(error_message)
            return None, None

        db_name = splitter_result[1].split(".")[0]

        return tree_function.strip_char_from_list([splitter_result[0]])[0], db_name

    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        logger.debug(
            f"Processing Oracle data-access function detail {data_access_func_detail}"
        )

        arguments: List[str] = tree_function.remove_whitespaces_from_list(
            tree_function.token_values(data_access_func_detail.arg_list)
        )

        server, db_name = self._get_server_and_db_name(arguments[0])

        if db_name is None or server is None:
            return Lineage.empty()

        schema_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Schema"]

        table_name: str = cast(
            IdentifierAccessor,
            cast(IdentifierAccessor, data_access_func_detail.identifier_accessor).next,
        ).items["Name"]

        qualified_table_name: str = f"{db_name}.{schema_name}.{table_name}"

        urn = make_urn(
            config=self.config,
            platform_instance_resolver=self.platform_instance_resolver,
            data_platform_pair=self.get_platform_pair(),
            server=server,
            qualified_table_name=qualified_table_name,
        )

        column_lineage = self.create_table_column_lineage(urn)

        return Lineage(
            upstreams=[
                DataPlatformTable(
                    data_platform_pair=self.get_platform_pair(),
                    urn=urn,
                )
            ],
            column_lineage=column_lineage,
        )


class DatabricksLineage(AbstractLineage):
    def form_qualified_table_name(
        self,
        table_reference: ReferencedTable,
        data_platform_pair: DataPlatformPair,
    ) -> str:
        platform_detail: PlatformDetail = (
            self.platform_instance_resolver.get_platform_instance(
                PowerBIPlatformDetail(
                    data_platform_pair=data_platform_pair,
                    data_platform_server=table_reference.warehouse,
                )
            )
        )

        metastore: Optional[str] = None

        qualified_table_name: str = f"{table_reference.database}.{table_reference.schema}.{table_reference.table}"

        if isinstance(platform_detail, DataBricksPlatformDetail):
            metastore = platform_detail.metastore

        if metastore is not None:
            return f"{metastore}.{qualified_table_name}"

        return qualified_table_name

    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        logger.debug(
            f"Processing Databrick data-access function detail {data_access_func_detail}"
        )
        table_detail: Dict[str, str] = {}
        temp_accessor: Optional[IdentifierAccessor] = (
            data_access_func_detail.identifier_accessor
        )

        while temp_accessor:
            # Condition to handle databricks M-query pattern where table, schema and database all are present in
            # the same invoke statement
            if all(
                element in temp_accessor.items
                for element in ["Item", "Schema", "Catalog"]
            ):
                table_detail["Schema"] = temp_accessor.items["Schema"]
                table_detail["Table"] = temp_accessor.items["Item"]
            else:
                table_detail[temp_accessor.items["Kind"]] = temp_accessor.items["Name"]

            if temp_accessor.next is not None:
                temp_accessor = temp_accessor.next
            else:
                break

        table_reference = self.create_reference_table(
            arg_list=data_access_func_detail.arg_list,
            table_detail=table_detail,
        )

        if table_reference:
            qualified_table_name: str = self.form_qualified_table_name(
                table_reference=table_reference,
                data_platform_pair=self.get_platform_pair(),
            )

            urn = make_urn(
                config=self.config,
                platform_instance_resolver=self.platform_instance_resolver,
                data_platform_pair=self.get_platform_pair(),
                server=table_reference.warehouse,
                qualified_table_name=qualified_table_name,
            )

            column_lineage = self.create_table_column_lineage(urn)

            return Lineage(
                upstreams=[
                    DataPlatformTable(
                        data_platform_pair=self.get_platform_pair(),
                        urn=urn,
                    )
                ],
                column_lineage=column_lineage,
            )

        return Lineage.empty()

    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.DATABRICKS_SQL.value


class TwoStepDataAccessPattern(AbstractLineage, ABC):
    """
    These are the DataSource for which PowerBI Desktop generates default M-Query of the following pattern
        let
            Source = Sql.Database("localhost", "library"),
            dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]
        in
            dbo_book_issue
    """

    def two_level_access_pattern(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        logger.debug(
            f"Processing {self.get_platform_pair().powerbi_data_platform_name} data-access function detail {data_access_func_detail}"
        )

        server, db_name = self.get_db_detail_from_argument(
            data_access_func_detail.arg_list
        )
        if server is None or db_name is None:
            return Lineage.empty()  # Return an empty list

        schema_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Schema"]

        table_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Item"]

        qualified_table_name: str = f"{db_name}.{schema_name}.{table_name}"

        logger.debug(
            f"Platform({self.get_platform_pair().datahub_data_platform_name}) qualified_table_name= {qualified_table_name}"
        )

        urn = make_urn(
            config=self.config,
            platform_instance_resolver=self.platform_instance_resolver,
            data_platform_pair=self.get_platform_pair(),
            server=server,
            qualified_table_name=qualified_table_name,
        )

        column_lineage = self.create_table_column_lineage(urn)

        return Lineage(
            upstreams=[
                DataPlatformTable(
                    data_platform_pair=self.get_platform_pair(),
                    urn=urn,
                )
            ],
            column_lineage=column_lineage,
        )


class MySQLLineage(AbstractLineage):
    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        logger.debug(
            f"Processing {self.get_platform_pair().powerbi_data_platform_name} data-access function detail {data_access_func_detail}"
        )

        server, db_name = self.get_db_detail_from_argument(
            data_access_func_detail.arg_list
        )
        if server is None or db_name is None:
            return Lineage.empty()  # Return an empty list

        schema_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Schema"]

        table_name: str = cast(
            IdentifierAccessor, data_access_func_detail.identifier_accessor
        ).items["Item"]

        qualified_table_name: str = f"{schema_name}.{table_name}"

        logger.debug(
            f"Platform({self.get_platform_pair().datahub_data_platform_name}) qualified_table_name= {qualified_table_name}"
        )

        urn = make_urn(
            config=self.config,
            platform_instance_resolver=self.platform_instance_resolver,
            data_platform_pair=self.get_platform_pair(),
            server=server,
            qualified_table_name=qualified_table_name,
        )

        column_lineage = self.create_table_column_lineage(urn)

        return Lineage(
            upstreams=[
                DataPlatformTable(
                    data_platform_pair=self.get_platform_pair(),
                    urn=urn,
                )
            ],
            column_lineage=column_lineage,
        )

    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.MYSQL.value


class PostgresLineage(TwoStepDataAccessPattern):
    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        return self.two_level_access_pattern(data_access_func_detail)

    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.POSTGRES_SQL.value


class MSSqlLineage(TwoStepDataAccessPattern):
    # https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/ownership-and-user-schema-separation?view=sql-server-ver16
    DEFAULT_SCHEMA = "dbo"  # Default schema name in MS-SQL is dbo

    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.MS_SQL.value

    def create_urn_using_old_parser(
        self, query: str, db_name: str, server: str
    ) -> List[DataPlatformTable]:
        dataplatform_tables: List[DataPlatformTable] = []

        tables: List[str] = native_sql_parser.get_tables(query)

        for parsed_table in tables:
            # components: List[str] = [v.strip("[]") for v in parsed_table.split(".")]
            components = [v.strip("[]") for v in parsed_table.split(".")]
            if len(components) == 3:
                database, schema, table = components
            elif len(components) == 2:
                schema, table = components
                database = db_name
            elif len(components) == 1:
                (table,) = components
                database = db_name
                schema = MSSqlLineage.DEFAULT_SCHEMA
            else:
                self.reporter.warning(
                    title="Invalid table format",
                    message="The advanced SQL lineage feature (enable_advance_lineage_sql_construct) is disabled. Please either enable this feature or ensure the table is referenced as <db-name>.<schema-name>.<table-name> in the SQL.",
                    context=f"table-name={self.table.full_name}",
                )
                continue

            qualified_table_name = f"{database}.{schema}.{table}"
            urn = make_urn(
                config=self.config,
                platform_instance_resolver=self.platform_instance_resolver,
                data_platform_pair=self.get_platform_pair(),
                server=server,
                qualified_table_name=qualified_table_name,
            )
            dataplatform_tables.append(
                DataPlatformTable(
                    data_platform_pair=self.get_platform_pair(),
                    urn=urn,
                )
            )

        logger.debug(f"Generated upstream tables = {dataplatform_tables}")

        return dataplatform_tables

    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        arguments: List[str] = tree_function.strip_char_from_list(
            values=tree_function.remove_whitespaces_from_list(
                tree_function.token_values(data_access_func_detail.arg_list)
            ),
        )

        server, database = self.get_db_detail_from_argument(
            data_access_func_detail.arg_list
        )
        if server is None or database is None:
            return Lineage.empty()  # Return an empty list

        assert server
        assert database  # to silent the lint

        query: Optional[str] = get_next_item(arguments, "Query")
        if query:
            if self.config.enable_advance_lineage_sql_construct is False:
                # Use previous parser to generate URN to keep backward compatibility
                return Lineage(
                    upstreams=self.create_urn_using_old_parser(
                        query=query,
                        db_name=database,
                        server=server,
                    ),
                    column_lineage=[],
                )

            return self.parse_custom_sql(
                query=query,
                database=database,
                server=server,
                schema=MSSqlLineage.DEFAULT_SCHEMA,
            )

        # It is a regular case of MS-SQL
        logger.debug("Handling with regular case")
        return self.two_level_access_pattern(data_access_func_detail)


class ThreeStepDataAccessPattern(AbstractLineage, ABC):
    def get_datasource_server(
        self, arguments: List[str], data_access_func_detail: DataAccessFunctionDetail
    ) -> str:
        return tree_function.strip_char_from_list([arguments[0]])[0]

    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        logger.debug(
            f"Processing {self.get_platform_pair().datahub_data_platform_name} function detail {data_access_func_detail}"
        )

        arguments: List[str] = tree_function.remove_whitespaces_from_list(
            tree_function.token_values(data_access_func_detail.arg_list)
        )
        # First is database name
        db_name: str = data_access_func_detail.identifier_accessor.items["Name"]  # type: ignore
        # Second is schema name
        schema_name: str = cast(
            IdentifierAccessor,
            data_access_func_detail.identifier_accessor.next,  # type: ignore
        ).items["Name"]
        # Third is table name
        table_name: str = cast(
            IdentifierAccessor,
            data_access_func_detail.identifier_accessor.next.next,  # type: ignore
        ).items["Name"]

        qualified_table_name: str = f"{db_name}.{schema_name}.{table_name}"

        logger.debug(
            f"{self.get_platform_pair().datahub_data_platform_name} qualified_table_name {qualified_table_name}"
        )

        server: str = self.get_datasource_server(arguments, data_access_func_detail)

        urn = make_urn(
            config=self.config,
            platform_instance_resolver=self.platform_instance_resolver,
            data_platform_pair=self.get_platform_pair(),
            server=server,
            qualified_table_name=qualified_table_name,
        )

        column_lineage = self.create_table_column_lineage(urn)

        return Lineage(
            upstreams=[
                DataPlatformTable(
                    data_platform_pair=self.get_platform_pair(),
                    urn=urn,
                )
            ],
            column_lineage=column_lineage,
        )


class SnowflakeLineage(ThreeStepDataAccessPattern):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.SNOWFLAKE.value


class GoogleBigQueryLineage(ThreeStepDataAccessPattern):
    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.GOOGLE_BIGQUERY.value

    def get_datasource_server(
        self, arguments: List[str], data_access_func_detail: DataAccessFunctionDetail
    ) -> str:
        # In Google BigQuery server is project-name
        # condition to silent lint, it is not going to be None
        return (
            data_access_func_detail.identifier_accessor.items["Name"]
            if data_access_func_detail.identifier_accessor is not None
            else ""
        )


class NativeQueryLineage(AbstractLineage):
    SUPPORTED_NATIVE_QUERY_DATA_PLATFORM: dict = {
        SupportedDataPlatform.SNOWFLAKE.value.powerbi_data_platform_name: SupportedDataPlatform.SNOWFLAKE,
        SupportedDataPlatform.AMAZON_REDSHIFT.value.powerbi_data_platform_name: SupportedDataPlatform.AMAZON_REDSHIFT,
        SupportedDataPlatform.DatabricksMultiCloud_SQL.value.powerbi_data_platform_name: SupportedDataPlatform.DatabricksMultiCloud_SQL,
    }
    current_data_platform: SupportedDataPlatform = SupportedDataPlatform.SNOWFLAKE

    def get_platform_pair(self) -> DataPlatformPair:
        return self.current_data_platform.value

    @staticmethod
    def is_native_parsing_supported(data_access_function_name: str) -> bool:
        return (
            data_access_function_name
            in NativeQueryLineage.SUPPORTED_NATIVE_QUERY_DATA_PLATFORM
        )

    def create_urn_using_old_parser(self, query: str, server: str) -> Lineage:
        dataplatform_tables: List[DataPlatformTable] = []

        tables: List[str] = native_sql_parser.get_tables(query)

        column_lineage = []
        for qualified_table_name in tables:
            if len(qualified_table_name.split(".")) != 3:
                logger.debug(
                    f"Skipping table {qualified_table_name} as it is not as per qualified_table_name format"
                )
                continue

            urn = make_urn(
                config=self.config,
                platform_instance_resolver=self.platform_instance_resolver,
                data_platform_pair=self.get_platform_pair(),
                server=server,
                qualified_table_name=qualified_table_name,
            )

            dataplatform_tables.append(
                DataPlatformTable(
                    data_platform_pair=self.get_platform_pair(),
                    urn=urn,
                )
            )

            column_lineage = self.create_table_column_lineage(urn)

        logger.debug(f"Generated dataplatform_tables {dataplatform_tables}")

        return Lineage(upstreams=dataplatform_tables, column_lineage=column_lineage)

    def get_db_name(self, data_access_tokens: List[str]) -> Optional[str]:
        if (
            data_access_tokens[0]
            != SupportedDataPlatform.DatabricksMultiCloud_SQL.value.powerbi_data_platform_name
        ):
            return None

        database: Optional[str] = get_next_item(data_access_tokens, "Database")

        if (
            database and database != Constant.M_QUERY_NULL
        ):  # database name is explicitly set
            return database

        return (
            get_next_item(  # database name is set in Name argument
                data_access_tokens, "Name"
            )
            or get_next_item(  # If both above arguments are not available, then try Catalog
                data_access_tokens, "Catalog"
            )
        )

    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        t1: Optional[Tree] = tree_function.first_arg_list_func(
            data_access_func_detail.arg_list
        )
        assert t1 is not None
        flat_argument_list: List[Tree] = tree_function.flat_argument_list(t1)

        if len(flat_argument_list) != 2:
            logger.debug(
                f"Expecting 2 argument, actual argument count is {len(flat_argument_list)}"
            )
            logger.debug(f"Flat argument list = {flat_argument_list}")
            return Lineage.empty()

        data_access_tokens: List[str] = tree_function.remove_whitespaces_from_list(
            tree_function.token_values(flat_argument_list[0])
        )

        if not self.is_native_parsing_supported(data_access_tokens[0]):
            logger.debug(
                f"Unsupported native-query data-platform = {data_access_tokens[0]}"
            )
            logger.debug(
                f"NativeQuery is supported only for {self.SUPPORTED_NATIVE_QUERY_DATA_PLATFORM}"
            )

            return Lineage.empty()

        if len(data_access_tokens[0]) < 3:
            logger.debug(
                f"Server is not available in argument list for data-platform {data_access_tokens[0]}. Returning empty "
                "list"
            )
            return Lineage.empty()

        self.current_data_platform = self.SUPPORTED_NATIVE_QUERY_DATA_PLATFORM[
            data_access_tokens[0]
        ]
        # The First argument is the query
        sql_query: str = tree_function.strip_char_from_list(
            values=tree_function.remove_whitespaces_from_list(
                tree_function.token_values(flat_argument_list[1])
            ),
        )[0]  # Remove any whitespaces and double quotes character

        server = tree_function.strip_char_from_list([data_access_tokens[2]])[0]

        if self.config.enable_advance_lineage_sql_construct is False:
            # Use previous parser to generate URN to keep backward compatibility
            return self.create_urn_using_old_parser(
                query=sql_query,
                server=server,
            )

        database_name: Optional[str] = self.get_db_name(data_access_tokens)

        return self.parse_custom_sql(
            query=sql_query,
            server=server,
            database=database_name,
            schema=None,
        )


class OdbcLineage(AbstractLineage):
    def create_lineage(
        self, data_access_func_detail: DataAccessFunctionDetail
    ) -> Lineage:
        logger.debug(
            f"Processing {self.get_platform_pair().powerbi_data_platform_name} "
            f"data-access function detail {data_access_func_detail}"
        )

        connect_string, _ = self.get_db_detail_from_argument(
            data_access_func_detail.arg_list
        )

        if not connect_string:
            self.reporter.warning(
                title="Can not extract ODBC connect string",
                message="Can not extract ODBC connect string from data access function. Skipping Lineage creation.",
                context=f"table-name={self.table.full_name}, data-access-func-detail={data_access_func_detail}",
            )
            return Lineage.empty()

        logger.debug(f"ODBC connect string: {connect_string}")
        data_platform, powerbi_platform = extract_platform(connect_string)
        server_name = extract_server(connect_string)

        if not data_platform:
            dsn = extract_dsn(connect_string)
            if dsn:
                logger.debug(f"Extracted DSN: {dsn}")
                server_name = dsn
            if dsn and self.config.dsn_to_platform_name:
                logger.debug(f"Attempting to map DSN {dsn} to platform")
                name = self.config.dsn_to_platform_name.get(dsn)
                if name:
                    logger.debug(f"Found DSN {dsn} mapped to platform {name}")
                    data_platform, powerbi_platform = normalize_platform_name(name)

        if not data_platform or not powerbi_platform:
            self.reporter.warning(
                title="Can not determine ODBC platform",
                message="Can not determine platform from ODBC connect string. Skipping Lineage creation.",
                context=f"table-name={self.table.full_name}, connect-string={connect_string}",
            )
            return Lineage.empty()

        platform_pair: DataPlatformPair = self.create_platform_pair(
            data_platform, powerbi_platform
        )

        if not server_name and self.config.server_to_platform_instance:
            self.reporter.warning(
                title="Can not determine ODBC server name",
                message="Can not determine server name with server_to_platform_instance mapping. Skipping Lineage creation.",
                context=f"table-name={self.table.full_name}",
            )
            return Lineage.empty()
        elif not server_name:
            server_name = "unknown"

        database_name = None
        schema_name = None
        table_name = None
        qualified_table_name = None

        temp_accessor: Optional[IdentifierAccessor] = (
            data_access_func_detail.identifier_accessor
        )

        while temp_accessor:
            logger.debug(
                f"identifier = {temp_accessor.identifier} items = {temp_accessor.items}"
            )
            if temp_accessor.items.get("Kind") == "Database":
                database_name = temp_accessor.items["Name"]

            if temp_accessor.items.get("Kind") == "Schema":
                schema_name = temp_accessor.items["Name"]

            if temp_accessor.items.get("Kind") == "Table":
                table_name = temp_accessor.items["Name"]

            if temp_accessor.next is not None:
                temp_accessor = temp_accessor.next
            else:
                break

        if (
            database_name is not None
            and schema_name is not None
            and table_name is not None
        ):
            qualified_table_name = f"{database_name}.{schema_name}.{table_name}"
        elif database_name is not None and table_name is not None:
            qualified_table_name = f"{database_name}.{table_name}"

        if not qualified_table_name:
            self.reporter.warning(
                title="Can not determine qualified table name",
                message="Can not determine qualified table name for ODBC data source. Skipping Lineage creation.",
                context=f"table-name={self.table.full_name}, data-platform={data_platform}",
            )
            logger.warning(
                f"Can not determine qualified table name for ODBC data source {data_platform} "
                f"table {self.table.full_name}."
            )
            return Lineage.empty()

        logger.debug(
            f"ODBC Platform {data_platform} found qualified table name {qualified_table_name}"
        )

        urn = make_urn(
            config=self.config,
            platform_instance_resolver=self.platform_instance_resolver,
            data_platform_pair=platform_pair,
            server=server_name,
            qualified_table_name=qualified_table_name,
        )

        column_lineage = self.create_table_column_lineage(urn)

        return Lineage(
            upstreams=[
                DataPlatformTable(
                    data_platform_pair=platform_pair,
                    urn=urn,
                )
            ],
            column_lineage=column_lineage,
        )

    @staticmethod
    def create_platform_pair(
        data_platform: str, powerbi_platform: str
    ) -> DataPlatformPair:
        return DataPlatformPair(data_platform, powerbi_platform)

    def get_platform_pair(self) -> DataPlatformPair:
        return SupportedDataPlatform.ODBC.value


class SupportedPattern(Enum):
    DATABRICKS_QUERY = (
        DatabricksLineage,
        FunctionName.DATABRICK_DATA_ACCESS,
    )

    DATABRICKS_MULTI_CLOUD = (
        DatabricksLineage,
        FunctionName.DATABRICK_MULTI_CLOUD_DATA_ACCESS,
    )

    POSTGRES_SQL = (
        PostgresLineage,
        FunctionName.POSTGRESQL_DATA_ACCESS,
    )

    ORACLE = (
        OracleLineage,
        FunctionName.ORACLE_DATA_ACCESS,
    )

    SNOWFLAKE = (
        SnowflakeLineage,
        FunctionName.SNOWFLAKE_DATA_ACCESS,
    )

    MS_SQL = (
        MSSqlLineage,
        FunctionName.MSSQL_DATA_ACCESS,
    )

    GOOGLE_BIG_QUERY = (
        GoogleBigQueryLineage,
        FunctionName.GOOGLE_BIGQUERY_DATA_ACCESS,
    )

    AMAZON_REDSHIFT = (
        AmazonRedshiftLineage,
        FunctionName.AMAZON_REDSHIFT_DATA_ACCESS,
    )

    MYSQL = (
        MySQLLineage,
        FunctionName.MYSQL_DATA_ACCESS,
    )

    NATIVE_QUERY = (
        NativeQueryLineage,
        FunctionName.NATIVE_QUERY,
    )

    ODBC = (
        OdbcLineage,
        FunctionName.ODBC_DATA_ACCESS,
    )

    def handler(self) -> Type[AbstractLineage]:
        return self.value[0]

    def function_name(self) -> str:
        return self.value[1].value

    @staticmethod
    def get_function_names() -> List[str]:
        functions: List[str] = []
        for supported_resolver in SupportedPattern:
            functions.append(supported_resolver.function_name())

        return functions

    @staticmethod
    def get_pattern_handler(function_name: str) -> Optional["SupportedPattern"]:
        logger.debug(f"Looking for pattern-handler for {function_name}")
        for supported_resolver in SupportedPattern:
            if function_name == supported_resolver.function_name():
                return supported_resolver
        logger.debug(f"pattern-handler not found for function_name {function_name}")
        return None
