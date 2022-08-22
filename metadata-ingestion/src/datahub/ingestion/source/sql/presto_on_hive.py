import base64
import json
import logging
from collections import namedtuple
from enum import Enum
from itertools import groupby
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pydantic.fields import Field

# This import verifies that the dependencies are available.
from pyhive import hive  # noqa: F401
from sqlalchemy import create_engine, text
from sqlalchemy.engine.reflection import Inspector

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import PlatformKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemyConfig,
    SQLAlchemySource,
    SqlContainerSubTypes,
    SqlWorkUnit,
    get_schema_metadata,
    make_sqlalchemy_uri,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column

logger: logging.Logger = logging.getLogger(__name__)

TableKey = namedtuple("TableKey", ["schema", "table"])


class PrestoOnHiveConfigMode(str, Enum):
    hive: str = "hive"  # noqa: F811
    presto: str = "presto"
    presto_on_hive: str = "presto-on-hive"
    trino: str = "trino"


class PrestoOnHiveConfig(BasicSQLAlchemyConfig):
    views_where_clause_suffix: str = Field(
        default="",
        description="Where clause to specify what Presto views should be ingested.",
    )
    tables_where_clause_suffix: str = Field(
        default="",
        description="Where clause to specify what Hive tables should be ingested.",
    )
    schemas_where_clause_suffix: str = Field(
        default="",
        description="Where clause to specify what Hive schemas should be ingested.",
    )
    host_port: str = Field(
        default="localhost:3306",
        description="Host URL and port to connect to. Example: localhost:3306",
    )
    scheme: str = Field(default="mysql+pymysql", description="", exclude=True)
    metastore_db_name: Optional[str] = Field(
        default=None,
        description="""Name of the Hive metastore's database (usually: metastore).
        For backwardcompatibility, if this field is not provided, the 'database' field will be used.
        If both the 'database' and 'metastore_db_name' fields are set then the 'database'
        field will be used to filter the hive/presto/trino database""",
    )
    mode: PrestoOnHiveConfigMode = Field(
        default=PrestoOnHiveConfigMode.presto_on_hive,
        description=f"""The ingested data will be stored under this platform.
        Valid options: {[e.value for e in PrestoOnHiveConfigMode]}""",
    )

    def get_sql_alchemy_url(self, uri_opts: Optional[Dict[str, Any]] = None) -> str:
        if not ((self.host_port and self.scheme) or self.sqlalchemy_uri):
            raise ValueError("host_port and schema or connect_uri required.")

        return self.sqlalchemy_uri or make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password.get_secret_value() if self.password is not None else None,
            self.host_port,
            self.metastore_db_name if self.metastore_db_name else self.database,
            uri_opts=uri_opts,
        )


@platform_name("Presto on Hive")
@config_class(PrestoOnHiveConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class PrestoOnHiveSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for Presto views and Hive tables (external / managed)
    - Column types associated with each table / view
    - Detailed table / view property info
    """

    _TABLES_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, p.PKEY_NAME as col_name, p.INTEGER_IDX as col_sort_order,
           p.PKEY_TYPE as col_type, 1 as is_partition_col, s.LOCATION as table_location
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN PARTITION_KEYS p ON t.TBL_ID = p.TBL_ID
    WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    UNION
    SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
           c.TYPE_NAME as col_type, 0 as is_partition_col, s.LOCATION as table_location
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
    WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _TABLES_POSTGRES_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
            to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, p."PKEY_NAME" as col_name,
            p."INTEGER_IDX" as col_sort_order, p."PKEY_TYPE" as col_type, 1 as is_partition_col, s."LOCATION" as table_location
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "PARTITION_KEYS" p ON t."TBL_ID" = p."TBL_ID"
    WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    UNION
    SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
           to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
           c."INTEGER_IDX" as col_sort_order, c."TYPE_NAME" as col_type, 0 as is_partition_col, s."LOCATION" as table_location
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
    WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _VIEWS_SQL_STATEMENT = """
    SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, t.VIEW_ORIGINAL_TEXT as view_original_text
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    WHERE t.VIEW_EXPANDED_TEXT = '/* Presto View */'
    {where_clause_suffix}
    ORDER BY t.TBL_ID desc;
    """

    _VIEWS_POSTGRES_SQL_STATEMENT = """
    SELECT t."TBL_ID", d."NAME" as "schema", t."TBL_NAME" "name", t."TBL_TYPE", t."VIEW_ORIGINAL_TEXT" as "view_original_text"
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    WHERE t."VIEW_EXPANDED_TEXT" = '/* Presto View */'
    {where_clause_suffix}
    ORDER BY t."TBL_ID" desc;
    """

    _PRESTO_VIEW_PREFIX = "/* Presto View: "
    _PRESTO_VIEW_SUFFIX = " */"

    _SCHEMAS_SQL_STATEMENT = """
    SELECT d.NAME as `schema`
    FROM DBS d
    WHERE 1
    {where_clause_suffix}
    ORDER BY d.NAME desc;
    """

    _SCHEMAS_POSTGRES_SQL_STATEMENT = """
    SELECT d."NAME" as "schema"
    FROM "DBS" d
    WHERE 1=1
    {where_clause_suffix}
    ORDER BY d."NAME" desc;
    """

    def __init__(self, config: PrestoOnHiveConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx, config.mode.value)
        self.config: PrestoOnHiveConfig = config
        self._alchemy_client = SQLAlchemyClient(config)

    def get_db_name(self, inspector: Inspector) -> str:
        if self.config.database_alias:
            return f"{self.config.database_alias}"
        if self.config.database:
            return f"{self.config.database}"
        else:
            return super().get_db_name(inspector)

    @classmethod
    def create(cls, config_dict, ctx):
        config = PrestoOnHiveConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def gen_schema_containers(
        self, schema: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:

        assert isinstance(self.config, PrestoOnHiveConfig)
        where_clause_suffix = f"{self.config.schemas_where_clause_suffix} {self._get_db_filter_where_clause()}"

        statement: str = (
            PrestoOnHiveSource._SCHEMAS_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else PrestoOnHiveSource._SCHEMAS_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )

        iter_res = self._alchemy_client.execute_query(statement)
        for row in iter_res:
            schema = row["schema"]
            schema_container_key: PlatformKey = self.gen_schema_key(db_name, schema)
            logger.debug("schema_container_key = {} ".format(schema_container_key))
            database_container_key = self.gen_database_key(database=db_name)
            container_workunits: Iterable[MetadataWorkUnit] = gen_containers(
                schema_container_key,
                schema,
                [SqlContainerSubTypes.SCHEMA],
                database_container_key,
            )

            for wu in container_workunits:
                self.report.report_workunit(wu)
                yield wu

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:

        assert isinstance(sql_config, PrestoOnHiveConfig)
        where_clause_suffix = f"{sql_config.tables_where_clause_suffix} {self._get_db_filter_where_clause()}"
        statement: str = (
            PrestoOnHiveSource._TABLES_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in sql_config.scheme
            else PrestoOnHiveSource._TABLES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )

        iter_res = self._alchemy_client.execute_query(statement)

        for key, group in groupby(iter_res, self._get_table_key):
            dataset_name = self.get_identifier(
                schema=key.schema, entity=key.table, inspector=inspector
            )
            self.report.report_entity_scanned(dataset_name, ent_type="table")

            if not sql_config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            columns = list(group)
            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            dataset_urn: str = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )

            # add table schema fields
            schema_fields = self.get_schema_fields(dataset_name, columns)

            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                None,
                None,
                schema_fields,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            # add table properties
            properties: Dict[str, str] = {
                "create_date": columns[-1]["create_date"],
                "table_type": columns[-1]["table_type"],
                "table_location": ""
                if columns[-1]["table_location"] is None
                else columns[-1]["table_location"],
            }

            par_columns: str = ", ".join(
                [c["col_name"] for c in columns if c["is_partition_col"]]
            )
            if par_columns != "":
                properties["partitioned_columns"] = par_columns

            dataset_properties = DatasetPropertiesClass(
                description=None,
                customProperties=properties,
            )
            dataset_snapshot.aspects.append(dataset_properties)

            db_name = self.get_db_name(inspector)
            schema = key.schema
            yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)

            # construct mce
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = SqlWorkUnit(id=dataset_name, mce=mce)
            self.report.report_workunit(wu)
            yield wu

            dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            subtypes_workunit = MetadataWorkUnit(
                id=f"{dataset_name}-subtypes",
                mcp=MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspect=SubTypesClass(typeNames=["table"]),
                ),
            )
            self.report.report_workunit(subtypes_workunit)
            yield subtypes_workunit

            yield from self._get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                entity_type="dataset",
                sql_config=sql_config,
            )

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:

        assert isinstance(sql_config, PrestoOnHiveConfig)
        where_clause_suffix = f"{sql_config.views_where_clause_suffix} {self._get_db_filter_where_clause()}"
        statement: str = (
            PrestoOnHiveSource._VIEWS_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in sql_config.scheme
            else PrestoOnHiveSource._VIEWS_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )
        iter_res = self._alchemy_client.execute_query(statement)
        for row in iter_res:
            dataset_name = self.get_identifier(
                schema=row["schema"], entity=row["name"], inspector=inspector
            )
            self.report.report_entity_scanned(dataset_name, ent_type="view")

            if not sql_config.view_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            columns, view_definition = self._get_column_metadata(
                row["view_original_text"]
            )

            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )

            # add view schema fields
            schema_fields = self.get_schema_fields(dataset_name, columns)

            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                canonical_schema=schema_fields,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            # add view properties
            properties: Dict[str, str] = {
                "is_view": "True",
            }
            dataset_properties = DatasetPropertiesClass(
                description=None,
                customProperties=properties,
            )
            dataset_snapshot.aspects.append(dataset_properties)

            # add view properties
            view_properties = ViewPropertiesClass(
                materialized=False,
                viewLogic=view_definition,
                viewLanguage="SQL",
            )
            dataset_snapshot.aspects.append(view_properties)

            db_name = self.get_db_name(inspector)
            schema = row["schema"]
            yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)

            # construct mce
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = SqlWorkUnit(id=dataset_name, mce=mce)
            self.report.report_workunit(wu)
            yield wu

            dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            yield from self._get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                entity_type="dataset",
                sql_config=sql_config,
            )

    def _get_db_filter_where_clause(self) -> str:
        if self.config.metastore_db_name is None:
            return ""  # read metastore_db_name field discription why
        return f"AND d.\"NAME\" = '{self.config.database}'"

    def _get_table_key(self, row: Dict[str, Any]) -> TableKey:
        return TableKey(schema=row["schema_name"], table=row["table_name"])

    def _get_column_metadata(self, view_original_text: str) -> Tuple[List[Dict], str]:
        """
        Get Column Metadata from VIEW_ORIGINAL_TEXT from TBLS table for Presto Views.
        Columns are sorted the same way as they appear in Presto Create View SQL.
        :param view_original_text:
        :return:
        """
        # remove encoded Presto View data prefix and suffix
        encoded_view_info = view_original_text.split(
            PrestoOnHiveSource._PRESTO_VIEW_PREFIX, 1
        )[-1].rsplit(PrestoOnHiveSource._PRESTO_VIEW_SUFFIX, 1)[0]

        # view_original_text is b64 encoded:
        decoded_view_info = base64.b64decode(encoded_view_info)
        view_definition = json.loads(decoded_view_info).get("originalSql")

        columns = json.loads(decoded_view_info).get("columns")
        for col in columns:
            col["col_name"], col["col_type"] = col["name"], col["type"]

        return list(columns), view_definition

    def close(self) -> None:
        if self._alchemy_client.connection is not None:
            self._alchemy_client.connection.close()

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict[Any, Any],
        pk_constraints: Optional[Dict[Any, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        return get_schema_fields_for_hive_column(
            column["col_name"], column["col_type"], default_nullable=True
        )


class SQLAlchemyClient:
    def __init__(self, config: SQLAlchemyConfig):
        self.config = config
        self.connection = self._get_connection()

    def _get_connection(self) -> Any:
        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)
        conn = engine.connect()
        return conn

    def execute_query(self, query: str) -> Iterable:
        """
        Create an iterator to execute sql.
        """
        results = self.connection.execute(text(query))
        return iter(results)
