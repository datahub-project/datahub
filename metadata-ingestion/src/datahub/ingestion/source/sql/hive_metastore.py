import base64
import json
import logging
from collections import namedtuple
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pydantic.dataclasses import dataclass
from pydantic.fields import Field

# This import verifies that the dependencies are available.
from sqlalchemy import create_engine, text
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    get_schema_metadata,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLCommonConfig,
)
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_database_key,
    gen_schema_container,
    gen_schema_key,
    get_domain_wu,
)
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.state.stateful_ingestion_base import JobId
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    ChangeTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    MapTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    SubTypesClass,
    TimeTypeClass,
    UnionTypeClass,
    ViewPropertiesClass,
)
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column
from datahub.utilities.str_enum import StrEnum

logger: logging.Logger = logging.getLogger(__name__)

TableKey = namedtuple("TableKey", ["schema", "table"])


class HiveMetastoreConfigMode(StrEnum):
    hive = "hive"
    presto = "presto"
    presto_on_hive = "presto-on-hive"
    trino = "trino"


@dataclass
class ViewDataset:
    dataset_name: str
    schema_name: str
    columns: List[dict]
    view_definition: Optional[str] = None


class HiveMetastore(BasicSQLAlchemyConfig):
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
    ingestion_job_id: str = ""
    host_port: str = Field(
        default="localhost:3306",
        description="Host URL and port to connect to. Example: localhost:3306",
    )
    scheme: str = Field(default="mysql+pymysql", description="", hidden_from_docs=True)

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for hive/presto database to filter in ingestion. Specify regex to only match the database name. e.g. to match all tables in database analytics, use the regex 'analytics'",
    )

    metastore_db_name: Optional[str] = Field(
        default=None,
        description="Name of the Hive metastore's database (usually: metastore). For backward compatibility, if this field is not provided, the database field will be used. If both the 'database' and 'metastore_db_name' fields are set then the 'database' field will be used to filter the hive/presto/trino database",
    )
    mode: HiveMetastoreConfigMode = Field(
        default=HiveMetastoreConfigMode.hive,
        description=f"The ingested data will be stored under this platform. Valid options: {[e.value for e in HiveMetastoreConfigMode]}",
    )
    use_catalog_subtype: bool = Field(
        default=True,
        description="Container Subtype name to be 'Database' or 'Catalog' Valid options: ['True', 'False']",
    )
    use_dataset_pascalcase_subtype: bool = Field(
        default=False,
        description="Dataset Subtype name to be 'Table' or 'View' Valid options: ['True', 'False']",
    )

    include_view_lineage: bool = Field(
        default=False, description="", hidden_from_docs=True
    )

    include_catalog_name_in_ids: bool = Field(
        default=False,
        description="Add the Presto catalog name (e.g. hive) to the generated dataset urns. `urn:li:dataset:(urn:li:dataPlatform:hive,hive.user.logging_events,PROD)` versus `urn:li:dataset:(urn:li:dataPlatform:hive,user.logging_events,PROD)`",
    )

    enable_properties_merge: bool = Field(
        default=True,
        description="By default, the connector enables merging of properties with what exists on the server. Set this to False to enable the default connector behavior of overwriting properties on each ingestion.",
    )

    simplify_nested_field_paths: bool = Field(
        default=False,
        description="Simplify v2 field paths to v1 by default. If the schema has Union or Array types, still falls back to v2. Note: This is ignored when use_schema_field_v2 is True.",
    )

    use_schema_field_v2: bool = Field(
        default=False,
        description="Use Schema Field v2 format for all fields. This provides better support for nested structures, unions, and complex types. When enabled, simplify_nested_field_paths is ignored.",
    )

    def get_sql_alchemy_url(
        self, uri_opts: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> str:
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


@platform_name("Hive Metastore")
@config_class(HiveMetastore)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.DATA_PROFILING, "Not Supported", False)
@capability(SourceCapability.CLASSIFICATION, "Not Supported", False)
@capability(
    SourceCapability.LINEAGE_COARSE, "View lineage is not supported", supported=False
)
class HiveMetastoreSource(SQLAlchemySource):
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
           p.PKEY_COMMENT as col_description, p.PKEY_TYPE as col_type, 1 as is_partition_col, s.LOCATION as table_location
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN PARTITION_KEYS p ON t.TBL_ID = p.TBL_ID
    WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    UNION
    SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
            c.COMMENT as col_description, c.TYPE_NAME as col_type, 0 as is_partition_col, s.LOCATION as table_location
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
            to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, p."PKEY_NAME" as col_name, p."INTEGER_IDX" as col_sort_order,
            p."PKEY_COMMENT" as col_description, p."PKEY_TYPE" as col_type, 1 as is_partition_col, s."LOCATION" as table_location
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "PARTITION_KEYS" p ON t."TBL_ID" = p."TBL_ID"
    WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    UNION
    SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
           to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
           c."INTEGER_IDX" as col_sort_order, c."COMMENT" as col_description, c."TYPE_NAME" as col_type, 0 as is_partition_col, s."LOCATION" as table_location
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
    WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _VIEWS_POSTGRES_SQL_STATEMENT = """
    SELECT t."TBL_ID", d."NAME" as "schema", t."TBL_NAME" "name", t."TBL_TYPE", t."VIEW_ORIGINAL_TEXT" as "view_original_text"
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    WHERE t."VIEW_EXPANDED_TEXT" = '/* Presto View */'
    {where_clause_suffix}
    ORDER BY t."TBL_ID" desc;
    """

    _VIEWS_SQL_STATEMENT = """
    SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, t.VIEW_ORIGINAL_TEXT as view_original_text
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    WHERE t.VIEW_EXPANDED_TEXT = '/* Presto View */'
    {where_clause_suffix}
    ORDER BY t.TBL_ID desc;
    """

    _HIVE_VIEWS_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type, t.VIEW_EXPANDED_TEXT as view_expanded_text, tp.PARAM_VALUE as description,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
            c.COMMENT as col_description, c.TYPE_NAME as col_type
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
    LEFT JOIN TABLE_PARAMS tp ON (t.TBL_ID = tp.TBL_ID AND tp.PARAM_KEY='comment')
    WHERE t.TBL_TYPE IN ('VIRTUAL_VIEW')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _HIVE_VIEWS_POSTGRES_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type, t."VIEW_EXPANDED_TEXT" as view_expanded_text, tp."PARAM_VALUE" as description,
           to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
           c."INTEGER_IDX" as col_sort_order, c."TYPE_NAME" as col_type
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
    LEFT JOIN "TABLE_PARAMS" tp ON (t."TBL_ID" = tp."TBL_ID" AND tp."PARAM_KEY"='comment')
    WHERE t."TBL_TYPE" IN ('VIRTUAL_VIEW')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _HIVE_PROPERTIES_SQL_STATEMENT = """
    SELECT d.NAME as schema_name, t.TBL_NAME as table_name, tp.PARAM_KEY, tp.PARAM_VALUE
    FROM TABLE_PARAMS tp
    JOIN TBLS t on t.TBL_ID = tp.TBL_ID
    JOIN DBS d on d.DB_ID = t.DB_ID
    WHERE 1
    {where_clause_suffix}
    ORDER BY tp.TBL_ID desc;
    """

    _HIVE_PROPERTIES_POSTGRES_SQL_STATEMENT = """
    SELECT d."NAME" as schema_name, t."TBL_NAME" as table_name, tp."PARAM_KEY", tp."PARAM_VALUE"
    FROM "TABLE_PARAMS" tp
    JOIN "TBLS" t on t."TBL_ID" = tp."TBL_ID"
    JOIN "DBS" d on d."DB_ID" = t."DB_ID"
    WHERE 1 = 1
    {where_clause_suffix}
    ORDER BY tp."TBL_ID" desc;
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
    WHERE 1 = 1
    {where_clause_suffix}
    ORDER BY d."NAME" desc;
    """

    def __init__(self, config: HiveMetastore, ctx: PipelineContext) -> None:
        super().__init__(config, ctx, config.mode.value)
        self.config: HiveMetastore = config
        self._alchemy_client = SQLAlchemyClient(config)
        self.database_container_subtype = (
            DatasetContainerSubTypes.CATALOG
            if config.use_catalog_subtype
            else DatasetContainerSubTypes.DATABASE
        )
        self.view_subtype = (
            DatasetSubTypes.VIEW.title()
            if config.use_dataset_pascalcase_subtype
            else DatasetSubTypes.VIEW.lower()
        )
        self.table_subtype = (
            DatasetSubTypes.TABLE.title()
            if config.use_dataset_pascalcase_subtype
            else DatasetSubTypes.TABLE.lower()
        )

    def get_db_name(self, inspector: Inspector) -> str:
        if self.config.database:
            return f"{self.config.database}"
        else:
            return super().get_db_name(inspector)

    @classmethod
    def create(cls, config_dict, ctx):
        config = HiveMetastore.parse_obj(config_dict)
        return cls(config, ctx)

    def gen_database_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = gen_database_key(
            database,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_database_container(
            database=database,
            database_container_key=database_container_key,
            sub_types=[self.database_container_subtype],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            extra_properties=extra_properties,
        )

    def gen_schema_containers(
        self,
        schema: str,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        assert isinstance(self.config, HiveMetastore)
        where_clause_suffix: str = ""
        if (
            self.config.schemas_where_clause_suffix
            or self._get_db_filter_where_clause()
        ):
            where_clause_suffix = f"{self.config.schemas_where_clause_suffix} {self._get_db_filter_where_clause()}"

        statement: str = (
            HiveMetastoreSource._SCHEMAS_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else HiveMetastoreSource._SCHEMAS_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )

        iter_res = self._alchemy_client.execute_query(statement)
        for row in iter_res:
            schema = row["schema"]
            if not self.config.database_pattern.allowed(schema):
                continue
            database_container_key = gen_database_key(
                database,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            schema_container_key = gen_schema_key(
                db_name=database,
                schema=schema,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield from gen_schema_container(
                database=database,
                schema=schema,
                sub_types=[DatasetContainerSubTypes.SCHEMA],
                database_container_key=database_container_key,
                schema_container_key=schema_container_key,
                domain_registry=self.domain_registry,
                domain_config=self.config.domain,
                extra_properties=extra_properties,
            )

    def get_default_ingestion_job_id(self) -> JobId:
        """
        Default ingestion job name that sql_common provides.
        Subclasses can override as needed.
        """
        return JobId(self.config.ingestion_job_id)

    def _get_table_properties(
        self, db_name: str, scheme: str, where_clause_suffix: str
    ) -> Dict[str, Dict[str, str]]:
        statement: str = (
            HiveMetastoreSource._HIVE_PROPERTIES_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in scheme
            else HiveMetastoreSource._HIVE_PROPERTIES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )
        iter_res = self._alchemy_client.execute_query(statement)
        table_properties: Dict[str, Dict[str, str]] = {}
        for row in iter_res:
            dataset_name = f"{row['schema_name']}.{row['table_name']}"
            if self.config.include_catalog_name_in_ids:
                dataset_name = f"{db_name}.{dataset_name}"
            if row["PARAM_KEY"] and row["PARAM_VALUE"]:
                table_properties.setdefault(dataset_name, {})[row["PARAM_KEY"]] = row[
                    "PARAM_VALUE"
                ]

        return table_properties

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        # In mysql we get tables for all databases and we should filter out the non metastore one
        if (
            "mysql" in self.config.scheme
            and self.config.metastore_db_name
            and self.config.metastore_db_name != schema
        ):
            return

        assert isinstance(sql_config, HiveMetastore)
        where_clause_suffix = f"{sql_config.tables_where_clause_suffix} {self._get_db_filter_where_clause()}"
        statement: str = (
            HiveMetastoreSource._TABLES_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in sql_config.scheme
            else HiveMetastoreSource._TABLES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )

        db_name = self.get_db_name(inspector)

        properties_cache = self._get_table_properties(
            db_name=db_name,
            scheme=sql_config.scheme,
            where_clause_suffix=where_clause_suffix,
        )

        iter_res = self._alchemy_client.execute_query(statement)

        for key, group in groupby_unsorted(iter_res, self._get_table_key):
            schema_name = (
                f"{db_name}.{key.schema}"
                if self.config.include_catalog_name_in_ids
                else key.schema
            )

            dataset_name = self.get_identifier(
                schema=schema_name, entity=key.table, inspector=inspector
            )

            self.report.report_entity_scanned(dataset_name, ent_type="table")

            if not self.config.database_pattern.allowed(key.schema):
                self.report.report_dropped(f"{dataset_name}")
                continue

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
            schema_fields = self.get_schema_fields(dataset_name, columns, inspector)

            self._set_partition_key(columns, schema_fields)

            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                None,
                None,
                schema_fields,
                self.config.simplify_nested_field_paths,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            # add table properties
            properties: Dict[str, str] = properties_cache.get(dataset_name, {})
            properties["table_type"] = str(columns[-1]["table_type"] or "")
            properties["table_location"] = str(columns[-1]["table_location"] or "")
            properties["create_date"] = str(columns[-1]["create_date"] or "")

            par_columns: str = ", ".join(
                [c["col_name"] for c in columns if c["is_partition_col"]]
            )
            if par_columns != "":
                properties["partitioned_columns"] = par_columns

            table_description = properties.get("comment")
            yield from self.add_hive_dataset_to_container(
                dataset_urn=dataset_urn, inspector=inspector, schema=key.schema
            )

            if self.config.enable_properties_merge:
                from datahub.specific.dataset import DatasetPatchBuilder

                patch_builder: DatasetPatchBuilder = DatasetPatchBuilder(
                    urn=dataset_snapshot.urn
                )
                patch_builder.set_display_name(key.table)

                if table_description:
                    patch_builder.set_description(description=table_description)

                for prop, value in properties.items():
                    patch_builder.add_custom_property(key=prop, value=value)
                yield from [
                    MetadataWorkUnit(
                        id=f"{mcp_raw.entityUrn}-{DatasetPropertiesClass.ASPECT_NAME}",
                        mcp_raw=mcp_raw,
                    )
                    for mcp_raw in patch_builder.build()
                ]
            else:
                # we add to the MCE to keep compatibility with previous output
                # if merging is disabled
                dataset_properties = DatasetPropertiesClass(
                    name=key.table,
                    description=table_description,
                    customProperties=properties,
                )
                dataset_snapshot.aspects.append(dataset_properties)

            # construct mce
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            yield SqlWorkUnit(id=dataset_name, mce=mce)

            dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            yield MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=[self.table_subtype]),
            ).as_workunit()

            if self.config.domain:
                assert self.domain_registry
                yield from get_domain_wu(
                    dataset_name=dataset_name,
                    entity_urn=dataset_urn,
                    domain_config=self.config.domain,
                    domain_registry=self.domain_registry,
                )

    def add_hive_dataset_to_container(
        self, dataset_urn: str, inspector: Inspector, schema: str
    ) -> Iterable[MetadataWorkUnit]:
        db_name = self.get_db_name(inspector)
        schema_container_key = gen_schema_key(
            db_name=db_name,
            schema=schema,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=schema_container_key,
        )

    def get_hive_view_columns(self, inspector: Inspector) -> Iterable[ViewDataset]:
        where_clause_suffix = ""
        if self.config.views_where_clause_suffix or self._get_db_filter_where_clause():
            where_clause_suffix = f"{self.config.views_where_clause_suffix} {self._get_db_filter_where_clause()}"

        statement: str = (
            HiveMetastoreSource._HIVE_VIEWS_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else HiveMetastoreSource._HIVE_VIEWS_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )

        iter_res = self._alchemy_client.execute_query(statement)
        for key, group in groupby_unsorted(iter_res, self._get_table_key):
            db_name = self.get_db_name(inspector)

            schema_name = (
                f"{db_name}.{key.schema}"
                if self.config.include_catalog_name_in_ids
                else key.schema
            )

            dataset_name = self.get_identifier(
                schema=schema_name, entity=key.table, inspector=inspector
            )

            if not self.config.database_pattern.allowed(key.schema):
                self.report.report_dropped(f"{dataset_name}")
                continue

            columns = list(group)

            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            yield ViewDataset(
                dataset_name=dataset_name,
                schema_name=key.schema,
                columns=columns,
                view_definition=columns[-1]["view_expanded_text"],
            )

    def get_presto_view_columns(self, inspector: Inspector) -> Iterable[ViewDataset]:
        where_clause_suffix = ""
        if self.config.views_where_clause_suffix or self._get_db_filter_where_clause():
            where_clause_suffix = f"{self.config.views_where_clause_suffix} {self._get_db_filter_where_clause()}"

        statement: str = (
            HiveMetastoreSource._VIEWS_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else HiveMetastoreSource._VIEWS_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )

        iter_res = self._alchemy_client.execute_query(statement)
        for row in iter_res:
            db_name = self.get_db_name(inspector)
            schema_name = (
                f"{db_name}.{row['schema']}"
                if self.config.include_catalog_name_in_ids
                else row["schema"]
            )
            dataset_name = self.get_identifier(
                schema=schema_name,
                entity=row["name"],
                inspector=inspector,
            )

            columns, view_definition = self._get_presto_view_column_metadata(
                row["view_original_text"]
            )

            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            yield ViewDataset(
                dataset_name=dataset_name,
                schema_name=row["schema"],
                columns=columns,
                view_definition=view_definition,
            )

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        assert isinstance(sql_config, HiveMetastore)

        # In mysql we get tables for all databases and we should filter out the non metastore one
        if (
            "mysql" in self.config.scheme
            and self.config.metastore_db_name
            and self.config.metastore_db_name != schema
        ):
            return

        iter: Iterable[ViewDataset]
        if self.config.mode in [HiveMetastoreConfigMode.hive]:
            iter = self.get_hive_view_columns(inspector=inspector)
        else:
            iter = self.get_presto_view_columns(inspector=inspector)
        for dataset in iter:
            self.report.report_entity_scanned(dataset.dataset_name, ent_type="view")

            if not sql_config.view_pattern.allowed(dataset.dataset_name):
                self.report.report_dropped(dataset.dataset_name)
                continue

            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset.dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )

            # add view schema fields
            schema_fields = self.get_schema_fields(
                dataset.dataset_name,
                dataset.columns,
                inspector,
            )

            schema_metadata = get_schema_metadata(
                self.report,
                dataset.dataset_name,
                self.platform,
                dataset.columns,
                canonical_schema=schema_fields,
                simplify_nested_field_paths=self.config.simplify_nested_field_paths,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            # add view properties
            properties: Dict[str, str] = {
                "is_view": "True",
            }
            dataset_properties = DatasetPropertiesClass(
                name=dataset.dataset_name.split(".")[-1],
                description=None,
                customProperties=properties,
            )
            dataset_snapshot.aspects.append(dataset_properties)

            # add view properties
            view_properties = ViewPropertiesClass(
                materialized=False,
                viewLogic=dataset.view_definition if dataset.view_definition else "",
                viewLanguage="SQL",
            )
            dataset_snapshot.aspects.append(view_properties)

            yield from self.add_hive_dataset_to_container(
                dataset_urn=dataset_urn, inspector=inspector, schema=dataset.schema_name
            )

            # construct mce
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            yield SqlWorkUnit(id=dataset.dataset_name, mce=mce)

            dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            # Add views subtype
            yield MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=[self.view_subtype]),
            ).as_workunit()

            # Add views definition
            view_properties_aspect = ViewPropertiesClass(
                materialized=False,
                viewLanguage="SQL",
                viewLogic=dataset.view_definition if dataset.view_definition else "",
            )
            yield MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="viewProperties",
                aspect=view_properties_aspect,
            ).as_workunit()

            if self.config.domain:
                assert self.domain_registry
                yield from get_domain_wu(
                    dataset_name=dataset.dataset_name,
                    entity_urn=dataset_urn,
                    domain_registry=self.domain_registry,
                    domain_config=self.config.domain,
                )

    def _get_db_filter_where_clause(self) -> str:
        if self.config.metastore_db_name is None:
            return ""  # read metastore_db_name field discription why
        if self.config.database:
            if "postgresql" in self.config.scheme:
                return f"AND d.\"NAME\" = '{self.config.database}'"
            else:
                return f"AND d.NAME = '{self.config.database}'"

        return ""

    def _get_table_key(self, row: Dict[str, Any]) -> TableKey:
        return TableKey(schema=row["schema_name"], table=row["table_name"])

    def _get_presto_view_column_metadata(
        self, view_original_text: str
    ) -> Tuple[List[Dict], str]:
        """
        Get Column Metadata from VIEW_ORIGINAL_TEXT from TBLS table for Presto Views.
        Columns are sorted the same way as they appear in Presto Create View SQL.
        :param view_original_text:
        :return:
        """
        # remove encoded Presto View data prefix and suffix
        encoded_view_info = view_original_text.split(
            HiveMetastoreSource._PRESTO_VIEW_PREFIX, 1
        )[-1].rsplit(HiveMetastoreSource._PRESTO_VIEW_SUFFIX, 1)[0]

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
        super().close()

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict[Any, Any],
        inspector: Inspector,
        pk_constraints: Optional[Dict[Any, Any]] = None,
        partition_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        """
        Generate schema fields with optional v2 support for better nested structure handling
        """

        # Handle SQLAlchemy Row objects and dictionary access
        def safe_get_column_value(
            col_obj: Dict[Any, Any], key: str, default: str = ""
        ) -> str:
            """Safely get column value from SQLAlchemy Row or dict."""
            try:
                if hasattr(col_obj, "_mapping"):
                    # SQLAlchemy Row object
                    return col_obj._mapping.get(key, default)
                elif hasattr(col_obj, "get"):
                    # Dictionary-like object
                    return col_obj.get(key, default)
                else:
                    # Try direct attribute access or indexing
                    try:
                        return col_obj.get(key, default)
                    except (KeyError, TypeError):
                        return getattr(col_obj, key, default)
            except Exception:
                return default

        col_name = safe_get_column_value(column, "col_name")
        col_type = safe_get_column_value(column, "col_type")
        description = safe_get_column_value(column, "col_description", "")

        # Clean column name - remove any array notation that might come from Hive
        if col_name and isinstance(col_name, str):
            col_name = col_name.rstrip("[]")

        # Check if we should use Schema Field v2
        if self.config.use_schema_field_v2:
            try:
                return self._generate_schema_fields_v2(
                    field_name=col_name,
                    field_type=col_type,
                    description=description,
                    dataset_name=dataset_name,
                )
            except Exception as e:
                logger.warning(
                    f"Failed to parse type {col_type} for {dataset_name} with v2, falling back to v1: {e}"
                )
                # Fallback to v1 if v2 parsing completely fails
                return get_schema_fields_for_hive_column(
                    col_name,
                    col_type,
                    description=description,
                    default_nullable=True,
                )
        else:
            # Use original v1 implementation
            return get_schema_fields_for_hive_column(
                col_name,
                col_type,
                description=description,
                default_nullable=True,
            )

    def _generate_schema_fields_v2(
        self,
        field_name: str,
        field_type: str,
        description: str,
        dataset_name: str,
    ) -> List[SchemaField]:
        """
        Generate v2 schema fields following the official DataHub v2 specification
        """
        all_fields = []
        clean_field_name = field_name.rstrip("[]") if field_name else ""

        # Generate fields based on the DataHub v2 specification patterns
        field_type_clean = field_type.strip().lower()

        if field_type_clean.startswith("struct<"):
            all_fields.extend(
                self._generate_struct_fields_v2(
                    clean_field_name, field_type, description
                )
            )
        elif field_type_clean.startswith("array<"):
            all_fields.extend(
                self._generate_array_fields_v2(
                    clean_field_name, field_type, description
                )
            )
        elif field_type_clean.startswith("map<"):
            all_fields.extend(
                self._generate_map_fields_v2(clean_field_name, field_type, description)
            )
        elif field_type_clean.startswith("uniontype<"):
            all_fields.extend(
                self._generate_union_fields_v2(
                    clean_field_name, field_type, description
                )
            )
        else:
            # Simple primitive field
            primitive_type = self._get_v2_type_name(field_type)
            field_path = f"[version=2.0].[type={primitive_type}].{clean_field_name}"
            datahub_type = self._map_hive_type_to_datahub_type(field_type)

            field = SchemaField(
                fieldPath=field_path,
                type=datahub_type,
                nativeDataType=field_type,
                description=description,
                nullable=True,
                recursive=False,
            )
            all_fields.append(field)

        return all_fields

    def _generate_struct_fields_v2(
        self, field_name: str, field_type: str, description: str
    ) -> List[SchemaField]:
        """Generate struct fields according to v2 spec"""
        fields = []

        # Main struct field: [version=2.0].[type=struct].field_name
        struct_path = f"[version=2.0].[type=struct].{field_name}"
        struct_field = SchemaField(
            fieldPath=struct_path,
            type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
            nativeDataType=field_type,
            description=description,
            nullable=True,
            recursive=False,
        )
        fields.append(struct_field)

        # Parse struct contents and generate nested fields
        struct_content = field_type[7:-1]  # Remove 'struct<' and '>'
        struct_fields_parsed = self._parse_struct_fields(struct_content)

        for sub_field_name, sub_field_type in struct_fields_parsed:
            nested_fields = self._generate_nested_struct_fields_v2(
                sub_field_name,
                sub_field_type,
                f"[version=2.0].[type=struct].{field_name}",
            )
            fields.extend(nested_fields)

        return fields

    def _generate_array_fields_v2(
        self, field_name: str, field_type: str, description: str
    ) -> List[SchemaField]:
        """Generate array fields according to v2 spec"""
        fields = []

        # Parse array element type
        element_type = field_type[6:-1]  # Remove 'array<' and '>'
        element_type_name = self._get_v2_type_name(element_type)

        # Main array field: [version=2.0].[type=array].[type=element_type].field_name
        array_path = (
            f"[version=2.0].[type=array].[type={element_type_name}].{field_name}"
        )
        array_field = SchemaField(
            fieldPath=array_path,
            type=SchemaFieldDataTypeClass(type=ArrayTypeClass()),
            nativeDataType=field_type,
            description=description,
            nullable=True,
            recursive=False,
        )
        fields.append(array_field)

        # If element type is complex, generate nested fields
        if element_type.lower().strip().startswith(("struct<", "uniontype<", "map<")):
            nested_fields = self._generate_nested_struct_fields_v2(
                field_name,
                element_type,
                f"[version=2.0].[type=array].[type={element_type_name}]",
            )
            fields.extend(nested_fields)

        return fields

    def _generate_map_fields_v2(
        self, field_name: str, field_type: str, description: str
    ) -> List[SchemaField]:
        """Generate map fields according to v2 spec"""
        fields = []

        # Parse map types
        map_content = field_type[4:-1]  # Remove 'map<' and '>'
        key_type, value_type = self._parse_map_types(map_content)
        value_type_name = self._get_v2_type_name(value_type)

        # Main map field: [version=2.0].[type=map].[type=value_type].field_name
        map_path = f"[version=2.0].[type=map].[type={value_type_name}].{field_name}"
        map_field = SchemaField(
            fieldPath=map_path,
            type=SchemaFieldDataTypeClass(type=MapTypeClass()),
            nativeDataType=field_type,
            description=description,
            nullable=True,
            recursive=False,
        )
        fields.append(map_field)

        # If value type is complex, generate nested fields
        if value_type.lower().strip().startswith(("struct<", "uniontype<", "array<")):
            nested_fields = self._generate_nested_struct_fields_v2(
                field_name,
                value_type,
                f"[version=2.0].[type=map].[type={value_type_name}]",
            )
            fields.extend(nested_fields)

        return fields

    def _generate_union_fields_v2(
        self, field_name: str, field_type: str, description: str
    ) -> List[SchemaField]:
        """Generate union fields according to v2 spec"""
        fields = []

        # Main union field: [version=2.0].[type=union].field_name
        union_path = f"[version=2.0].[type=union].{field_name}"
        union_field = SchemaField(
            fieldPath=union_path,
            type=SchemaFieldDataTypeClass(type=UnionTypeClass()),
            nativeDataType=field_type,
            description=description,
            nullable=True,
            recursive=False,
        )
        fields.append(union_field)

        # Parse union types and generate member fields
        union_content = field_type[10:-1]  # Remove 'uniontype<' and '>'
        union_types = self._parse_union_types(union_content)

        for member_type in union_types:
            member_type_name = self._get_v2_type_name(member_type)
            base_path = f"[version=2.0].[type=union].[type={member_type_name}]"

            if member_type.lower().strip().startswith(("struct<", "array<", "map<")):
                # Complex member type - generate the member field and its nested fields
                if member_type.lower().strip().startswith("struct<"):
                    member_field_path = f"{base_path}.{field_name}"
                    member_field = SchemaField(
                        fieldPath=member_field_path,
                        type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                        nativeDataType=member_type,
                        description="",
                        nullable=True,
                        recursive=False,
                    )
                    fields.append(member_field)

                    # Generate nested fields for struct members
                    struct_content = member_type[7:-1]  # Remove 'struct<' and '>'
                    struct_fields_parsed = self._parse_struct_fields(struct_content)

                    for sub_field_name, sub_field_type in struct_fields_parsed:
                        nested_fields = self._generate_nested_struct_fields_v2(
                            sub_field_name, sub_field_type, member_field_path
                        )
                        fields.extend(nested_fields)

                elif member_type.lower().strip().startswith("array<"):
                    element_type = member_type[6:-1]
                    element_type_name = self._get_v2_type_name(element_type)
                    member_field_path = (
                        f"{base_path}.[type={element_type_name}].{field_name}"
                    )
                    member_field = SchemaField(
                        fieldPath=member_field_path,
                        type=SchemaFieldDataTypeClass(type=ArrayTypeClass()),
                        nativeDataType=member_type,
                        description="",
                        nullable=True,
                        recursive=False,
                    )
                    fields.append(member_field)

                elif member_type.lower().strip().startswith("map<"):
                    map_content = member_type[4:-1]
                    key_type, value_type = self._parse_map_types(map_content)
                    value_type_name = self._get_v2_type_name(value_type)
                    member_field_path = (
                        f"{base_path}.[type={value_type_name}].{field_name}"
                    )
                    member_field = SchemaField(
                        fieldPath=member_field_path,
                        type=SchemaFieldDataTypeClass(type=MapTypeClass()),
                        nativeDataType=member_type,
                        description="",
                        nullable=True,
                        recursive=False,
                    )
                    fields.append(member_field)
            else:
                # Simple member type: [version=2.0].[type=union].[type=member_type].field_name
                member_field_path = f"{base_path}.{field_name}"
                member_datahub_type = self._map_hive_type_to_datahub_type(member_type)
                member_field = SchemaField(
                    fieldPath=member_field_path,
                    type=member_datahub_type,
                    nativeDataType=member_type,
                    description="",
                    nullable=True,
                    recursive=False,
                )
                fields.append(member_field)

        return fields

    def _generate_nested_struct_fields_v2(
        self, field_name: str, field_type: str, parent_path: str
    ) -> List[SchemaField]:
        """Generate nested fields for struct members"""
        fields = []
        field_type_clean = field_type.strip().lower()

        if field_type_clean.startswith("struct<"):
            # Nested struct field
            field_path = f"{parent_path}.[type=struct].{field_name}"
            struct_field = SchemaField(
                fieldPath=field_path,
                type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                nativeDataType=field_type,
                description="",
                nullable=True,
                recursive=False,
            )
            fields.append(struct_field)

            # Parse and generate fields for struct members
            struct_content = field_type[7:-1]
            struct_fields_parsed = self._parse_struct_fields(struct_content)

            for sub_field_name, sub_field_type in struct_fields_parsed:
                nested_fields = self._generate_nested_struct_fields_v2(
                    sub_field_name, sub_field_type, field_path
                )
                fields.extend(nested_fields)

        elif field_type_clean.startswith("array<"):
            # Nested array field
            element_type = field_type[6:-1]
            element_type_name = self._get_v2_type_name(element_type)
            field_path = (
                f"{parent_path}.[type=array].[type={element_type_name}].{field_name}"
            )

            array_field = SchemaField(
                fieldPath=field_path,
                type=SchemaFieldDataTypeClass(type=ArrayTypeClass()),
                nativeDataType=field_type,
                description="",
                nullable=True,
                recursive=False,
            )
            fields.append(array_field)

            # If array element is complex, generate nested fields
            if element_type.lower().strip().startswith(("struct<", "map<")):
                nested_fields = self._generate_nested_struct_fields_v2(
                    field_name,
                    element_type,
                    f"{parent_path}.[type=array].[type={element_type_name}]",
                )
                fields.extend(nested_fields)

        elif field_type_clean.startswith("map<"):
            # Nested map field
            map_content = field_type[4:-1]
            key_type, value_type = self._parse_map_types(map_content)
            value_type_name = self._get_v2_type_name(value_type)
            field_path = (
                f"{parent_path}.[type=map].[type={value_type_name}].{field_name}"
            )

            map_field = SchemaField(
                fieldPath=field_path,
                type=SchemaFieldDataTypeClass(type=MapTypeClass()),
                nativeDataType=field_type,
                description="",
                nullable=True,
                recursive=False,
            )
            fields.append(map_field)

            # If map value is complex, generate nested fields
            if value_type.lower().strip().startswith(("struct<", "array<")):
                nested_fields = self._generate_nested_struct_fields_v2(
                    field_name,
                    value_type,
                    f"{parent_path}.[type=map].[type={value_type_name}]",
                )
                fields.extend(nested_fields)
        else:
            # Simple field
            primitive_type = self._get_v2_type_name(field_type)
            field_path = f"{parent_path}.[type={primitive_type}].{field_name}"
            datahub_type = self._map_hive_type_to_datahub_type(field_type)

            simple_field = SchemaField(
                fieldPath=field_path,
                type=datahub_type,
                nativeDataType=field_type,
                description="",
                nullable=True,
                recursive=False,
            )
            fields.append(simple_field)

        return fields

    def _get_v2_type_name(self, hive_type: str) -> str:
        """Map Hive types to v2 type names used in field paths"""
        hive_type_lower = hive_type.lower().strip()

        # Handle complex types
        if hive_type_lower.startswith("struct<"):
            return "struct"
        elif hive_type_lower.startswith("array<"):
            return "array"
        elif hive_type_lower.startswith("map<"):
            return "map"
        elif hive_type_lower.startswith("uniontype<"):
            return "union"

        # Handle parameterized primitive types
        if hive_type_lower.startswith("varchar") or hive_type_lower.startswith("char"):
            return "string"
        elif hive_type_lower.startswith("decimal"):
            return "double"

        # Handle standard primitive types
        type_mapping = {
            "string": "string",
            "int": "int",
            "integer": "int",
            "bigint": "long",
            "smallint": "int",
            "tinyint": "int",
            "double": "double",
            "float": "float",
            "boolean": "boolean",
            "date": "date",
            "timestamp": "timestamp",
            "binary": "bytes",
        }

        return type_mapping.get(hive_type_lower, "string")

    def _map_hive_type_to_datahub_type(
        self, hive_type: str
    ) -> SchemaFieldDataTypeClass:
        """Map Hive types to DataHub schema field types"""
        hive_type_lower = hive_type.lower()

        if (
            hive_type_lower in ["string", "varchar", "char"]
            or hive_type_lower.startswith("varchar")
            or hive_type_lower.startswith("char")
        ):
            return SchemaFieldDataTypeClass(type=StringTypeClass())
        elif (
            hive_type_lower in ["int", "integer", "smallint", "tinyint"]
            or hive_type_lower == "bigint"
            or hive_type_lower == "double"
            or hive_type_lower.startswith("decimal")
            or hive_type_lower == "float"
        ):
            return SchemaFieldDataTypeClass(type=NumberTypeClass())
        elif hive_type_lower == "boolean":
            return SchemaFieldDataTypeClass(type=BooleanTypeClass())
        elif hive_type_lower == "date":
            return SchemaFieldDataTypeClass(type=DateTypeClass())
        elif hive_type_lower == "timestamp":
            return SchemaFieldDataTypeClass(type=TimeTypeClass())
        elif hive_type_lower == "binary":
            return SchemaFieldDataTypeClass(type=BytesTypeClass())
        else:
            return SchemaFieldDataTypeClass(type=StringTypeClass())

    def _parse_struct_fields(self, struct_content: str) -> List[Tuple[str, str]]:
        """Parse struct field definitions with better handling of nested types"""
        if not struct_content.strip():
            return []

        fields = []
        current_field = ""
        bracket_count = 0
        angle_count = 0

        i = 0
        while i < len(struct_content):
            char = struct_content[i]

            if char == "<":
                angle_count += 1
                current_field += char
            elif char == ">":
                angle_count -= 1
                current_field += char
            elif char in "([":
                bracket_count += 1
                current_field += char
            elif char in ")]":
                bracket_count -= 1
                current_field += char
            elif char == "," and bracket_count == 0 and angle_count == 0:
                # Process the current field
                if current_field.strip():
                    field_parts = current_field.split(":", 1)
                    if len(field_parts) == 2:
                        field_name = field_parts[0].strip()
                        field_type = field_parts[1].strip()
                        fields.append((field_name, field_type))
                    else:
                        logger.warning(f"Could not parse struct field: {current_field}")
                current_field = ""
            else:
                current_field += char

            i += 1

        # Handle the last field
        if current_field.strip():
            field_parts = current_field.split(":", 1)
            if len(field_parts) == 2:
                field_name = field_parts[0].strip()
                field_type = field_parts[1].strip()
                fields.append((field_name, field_type))
            else:
                logger.warning(f"Could not parse struct field: {current_field}")

        return fields

    def _parse_map_types(self, map_content: str) -> Tuple[str, str]:
        """Parse map key and value types with better nested type handling"""
        if not map_content.strip():
            return "string", "string"

        bracket_count = 0
        angle_count = 0
        comma_positions = []

        # Find all comma positions that are at the top level (not inside nested types)
        for i, char in enumerate(map_content):
            if char == "<":
                angle_count += 1
            elif char == ">":
                angle_count -= 1
            elif char in "([":
                bracket_count += 1
            elif char in ")]":
                bracket_count -= 1
            elif char == "," and bracket_count == 0 and angle_count == 0:
                comma_positions.append(i)

        # For maps, we expect exactly one top-level comma separating key and value
        if len(comma_positions) == 1:
            split_pos = comma_positions[0]
            key_type = map_content[:split_pos].strip()
            value_type = map_content[split_pos + 1 :].strip()
            return key_type, value_type
        elif len(comma_positions) == 0:
            # No comma found, might be malformed - try simple split as fallback
            parts = map_content.split(",", 1)
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()

        # Fallback for malformed or complex cases
        logger.warning(
            f"Could not parse map type: map<{map_content}>. Using string defaults."
        )
        return "string", "string"

    def _parse_union_types(self, union_content: str) -> List[str]:
        """Parse union member types with better nested type handling"""
        if not union_content.strip():
            return []

        types = []
        current_type = ""
        bracket_count = 0
        angle_count = 0

        for char in union_content + ",":
            if char == "<":
                angle_count += 1
                current_type += char
            elif char == ">":
                angle_count -= 1
                current_type += char
            elif char in "([":
                bracket_count += 1
                current_type += char
            elif char in ")]":
                bracket_count -= 1
                current_type += char
            elif char == "," and bracket_count == 0 and angle_count == 0:
                if current_type.strip():
                    types.append(current_type.strip())
                current_type = ""
            else:
                current_type += char

        return types

    def _set_partition_key(self, columns, schema_fields):
        if len(columns) > 0:
            partition_key_names = set()
            for column in columns:
                if column["is_partition_col"]:
                    partition_key_names.add(column["col_name"])

            for schema_field in schema_fields:
                name = schema_field.fieldPath.split(".")[-1]
                if name in partition_key_names:
                    schema_field.isPartitioningKey = True


class SQLAlchemyClient:
    def __init__(self, config: SQLCommonConfig):
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
