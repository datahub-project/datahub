import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, Union, cast

from avrogen.dict_wrapper import DictWrapper
from snowflake.connector import SnowflakeConnection

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    Source,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_lineage import (
    SnowflakeLineageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeDatabase,
    SnowflakeDataDictionary,
    SnowflakeFK,
    SnowflakePK,
    SnowflakeQuery,
    SnowflakeSchema,
    SnowflakeTable,
    SnowflakeView,
)
from datahub.ingestion.source.sql.snowflake import SnowflakeSource
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source_report.sql.snowflake import SnowflakeReport
from datahub.metadata.com.linkedin.pegasus2avro.common import Status, SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProfile,
    DatasetProperties,
    UpstreamLineage,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    DateType,
    ForeignKeyConstraint,
    MySqlDDL,
    NullType,
    NumberType,
    RecordType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
    TimeType,
)

logger: logging.Logger = logging.getLogger(__name__)

# https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
SNOWFLAKE_FIELD_TYPE_MAPPINGS = {
    "DATE": DateType,
    "BIGINT": NumberType,
    "BINARY": BytesType,
    # 'BIT': BIT,
    "BOOLEAN": BooleanType,
    "CHAR": NullType,
    "CHARACTER": NullType,
    "DATETIME": TimeType,
    "DEC": NumberType,
    "DECIMAL": NumberType,
    "DOUBLE": NumberType,
    "FIXED": NumberType,
    "FLOAT": NumberType,
    "INT": NumberType,
    "INTEGER": NumberType,
    "NUMBER": NumberType,
    # 'OBJECT': ?
    "REAL": NumberType,
    "BYTEINT": NumberType,
    "SMALLINT": NumberType,
    "STRING": StringType,
    "TEXT": StringType,
    "TIME": TimeType,
    "TIMESTAMP": TimeType,
    "TIMESTAMP_TZ": TimeType,
    "TIMESTAMP_LTZ": TimeType,
    "TIMESTAMP_NTZ": TimeType,
    "TINYINT": NumberType,
    "VARBINARY": BytesType,
    "VARCHAR": StringType,
    "VARIANT": RecordType,
    "OBJECT": NullType,
    "ARRAY": ArrayType,
    "GEOGRAPHY": NullType,
}


@platform_name("Snowflake")
@config_class(SnowflakeV2Config)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration, only table level profiling is supported",
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Coming soon", supported=False)
class SnowflakeV2Source(TestableSource):
    def __init__(self, ctx: PipelineContext, config: SnowflakeV2Config):
        super().__init__(ctx)
        self.config: SnowflakeV2Config = config
        self.report: SnowflakeReport = SnowflakeReport()
        self.platform: str = "snowflake"
        self.sql_common: SQLAlchemySource = SQLAlchemySource(config, ctx, self.platform)

        # For database, schema, tables, views, etc
        self.data_dictionary = SnowflakeDataDictionary()

        # For lineage
        self.lineage_extractor = SnowflakeLineageExtractor(config, self.report)

        # Currently caching using instance variables
        # TODO - rewrite cache for readability or use out of the box solution
        self.db_tables: Dict[str, Optional[Dict[str, List[SnowflakeTable]]]] = {}
        self.db_views: Dict[str, Optional[Dict[str, List[SnowflakeView]]]] = {}

        # For column related queries and constraints, we currently query at schema level
        # In future, we may consider using queries and caching at database level first
        self.schema_columns: Dict[
            Tuple[str, str], Optional[Dict[str, List[SnowflakeColumn]]]
        ] = {}
        self.schema_pk_constraints: Dict[Tuple[str, str], Dict[str, SnowflakePK]] = {}
        self.schema_fk_constraints: Dict[
            Tuple[str, str], Dict[str, List[SnowflakeFK]]
        ] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = SnowflakeV2Config.parse_obj(config_dict)
        return cls(ctx, config)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return SnowflakeSource.test_connection(config_dict)

    def snowflake_identifier(self, identifier: str) -> str:
        # to be in in sync with older connector, convert name to lowercase
        if self.config._convert_urns_to_lowercase:
            return identifier.lower()
        return identifier

    def get_workunits(self) -> Iterable[WorkUnit]:

        conn: SnowflakeConnection = self.config.get_connection()
        self.add_config_to_report()

        self.inspect_session_metadata(conn)
        databases: List[SnowflakeDatabase] = self.data_dictionary.get_databases(conn)
        for snowflake_db in databases:
            if not self.config.database_pattern.allowed(snowflake_db.name):
                self.report.report_dropped(snowflake_db.name)
                continue

            yield from self._process_database(conn, snowflake_db)

    def _process_database(
        self, conn: SnowflakeConnection, snowflake_db: SnowflakeDatabase
    ) -> Iterable[MetadataWorkUnit]:
        db_name = snowflake_db.name

        database_workunits = self.sql_common.gen_database_containers(
            self.snowflake_identifier(db_name)
        )

        for wu in database_workunits:
            self.report.report_workunit(wu)
            yield wu

        # Use database and extract metadata from its information_schema
        # If this query fails, it means, user does not have usage access on database
        try:
            self.data_dictionary.query(conn, SnowflakeQuery.use_database(db_name))
        except Exception as e:
            self.report.report_warning(
                db_name,
                f"unable to get metadata information for database {db_name} due to an error -> {e}",
            )
            self.report.report_dropped(db_name)
            return

        snowflake_db.schemas = self.data_dictionary.get_schemas_for_database(
            conn, db_name
        )

        for snowflake_schema in snowflake_db.schemas:

            if not self.config.schema_pattern.allowed(snowflake_schema.name):
                self.report.report_dropped(f"{snowflake_schema.name}.*")
                continue

            yield from self._process_schema(conn, snowflake_schema, db_name)

    def _process_schema(
        self, conn: SnowflakeConnection, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_name = snowflake_schema.name
        schema_workunits = self.sql_common.gen_schema_containers(
            self.snowflake_identifier(schema_name),
            self.snowflake_identifier(db_name),
        )

        for wu in schema_workunits:
            self.report.report_workunit(wu)
            yield wu

        if self.config.include_tables:
            snowflake_schema.tables = self.get_tables_for_schema(
                conn, schema_name, db_name
            )

            for table in snowflake_schema.tables:
                yield from self._process_table(conn, table, schema_name, db_name)

        if self.config.include_views:
            snowflake_schema.views = self.get_views_for_schema(
                conn, schema_name, db_name
            )

            for view in snowflake_schema.views:
                yield from self._process_view(conn, view, schema_name, db_name)

    def _process_table(
        self,
        conn: SnowflakeConnection,
        table: SnowflakeTable,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = self.get_identifier(table.name, schema_name, db_name)

        self.report.report_entity_scanned(table_identifier)

        if not self.config.table_pattern.allowed(table_identifier):
            self.report.report_dropped(table_identifier)
            return

        table.columns = self.get_columns_for_table(
            conn, table.name, schema_name, db_name
        )
        table.pk = self.get_pk_constraints_for_table(
            conn, table.name, schema_name, db_name
        )
        table.foreign_keys = self.get_fk_constraints_for_table(
            conn, table.name, schema_name, db_name
        )
        dataset_name = self.get_identifier(table.name, schema_name, db_name)

        # TODO: rewrite lineage extractor to honour _convert_urns_to_lowercase=False config
        # Currently it generates backward compatible (lowercase) urns only
        lineage_info = self.lineage_extractor._get_upstream_lineage_info(dataset_name)

        table_workunits = self.gen_dataset_workunits(
            table, schema_name, db_name, lineage_info
        )
        for wu in table_workunits:
            self.report.report_workunit(wu)
            yield wu

    def _process_view(
        self,
        conn: SnowflakeConnection,
        view: SnowflakeView,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = self.get_identifier(view.name, schema_name, db_name)

        self.report.report_entity_scanned(table_identifier, "view")

        if not self.config.view_pattern.allowed(table_identifier):
            self.report.report_dropped(table_identifier)
            return

        view.columns = self.get_columns_for_table(conn, view.name, schema_name, db_name)
        dataset_name = self.get_identifier(view.name, schema_name, db_name)
        lineage_info = self.lineage_extractor._get_upstream_lineage_info(dataset_name)
        view_workunits = self.gen_dataset_workunits(
            view, schema_name, db_name, lineage_info
        )
        for wu in view_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_dataset_workunits(
        self,
        table: Union[SnowflakeTable, SnowflakeView],
        schema_name: str,
        db_name: str,
        lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]],
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = self.get_identifier(table.name, schema_name, db_name)
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        if lineage_info is not None:
            upstream_lineage, upstream_column_props = lineage_info
        else:
            upstream_column_props = {}
            upstream_lineage = None

        status = Status(removed=False)
        yield self.wrap_aspect_as_workunit("dataset", dataset_urn, "status", status)

        schema_metadata = self.get_schema_metadata(table, dataset_name, dataset_urn)
        yield self.wrap_aspect_as_workunit(
            "dataset", dataset_urn, "schemaMetadata", schema_metadata
        )

        dataset_properties = DatasetProperties(
            name=table.name,
            description=table.comment,
            qualifiedName=dataset_name,
            customProperties={**upstream_column_props},
        )
        yield self.wrap_aspect_as_workunit(
            "dataset", dataset_urn, "datasetProperties", dataset_properties
        )

        yield from self.sql_common.add_table_to_schema_container(
            dataset_urn,
            self.snowflake_identifier(db_name),
            self.snowflake_identifier(schema_name),
        )
        dpi_aspect = self.sql_common.get_dataplatform_instance_aspect(
            dataset_urn=dataset_urn
        )
        if dpi_aspect:
            yield dpi_aspect

        subTypes = SubTypes(
            typeNames=["view"] if isinstance(table, SnowflakeView) else ["table"]
        )
        yield self.wrap_aspect_as_workunit("dataset", dataset_urn, "subTypes", subTypes)

        yield from self.sql_common._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=self.config,
        )

        if upstream_lineage is not None:
            # Emit the lineage work unit
            yield self.wrap_aspect_as_workunit(
                "dataset", dataset_urn, "upstreamLineage", upstream_lineage
            )

        if isinstance(table, SnowflakeTable) and self.config.profiling.enabled:
            if self.config.profiling.allow_deny_patterns.allowed(dataset_name):
                # Emit the profile work unit
                dataset_profile = DatasetProfile(
                    timestampMillis=round(datetime.now().timestamp() * 1000),
                    columnCount=len(table.columns),
                    rowCount=table.rows_count,
                )
                self.report.report_entity_profiled(dataset_name)
                yield self.wrap_aspect_as_workunit(
                    "dataset", dataset_urn, "datasetProfile", dataset_profile
                )

            else:
                self.report.report_dropped(f"Profile for {dataset_name}")

        if isinstance(table, SnowflakeView):
            view = cast(SnowflakeView, table)
            view_definition_string = view.view_definition
            view_properties_aspect = ViewProperties(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
            )
            yield self.wrap_aspect_as_workunit(
                "dataset", dataset_urn, "viewProperties", view_properties_aspect
            )

    def get_schema_metadata(
        self,
        table: Union[SnowflakeTable, SnowflakeView],
        dataset_name: str,
        dataset_urn: str,
    ) -> SchemaMetadata:
        foreign_keys: Optional[List[ForeignKeyConstraint]] = None
        if isinstance(table, SnowflakeTable) and len(table.foreign_keys) > 0:
            foreign_keys = []
            for fk in table.foreign_keys:
                foreign_dataset = make_dataset_urn(
                    self.platform,
                    self.get_identifier(
                        fk.referred_table, fk.referred_schema, fk.referred_database
                    ),
                    self.config.env,
                )
                foreign_keys.append(
                    ForeignKeyConstraint(
                        name=fk.name,
                        foreignDataset=foreign_dataset,
                        foreignFields=[
                            make_schema_field_urn(
                                foreign_dataset, self.snowflake_identifier(col)
                            )
                            for col in fk.referred_column_names
                        ],
                        sourceFields=[
                            make_schema_field_urn(
                                dataset_urn, self.snowflake_identifier(col)
                            )
                            for col in fk.column_names
                        ],
                    )
                )

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=[
                SchemaField(
                    fieldPath=self.snowflake_identifier(col.name),
                    type=SchemaFieldDataType(
                        SNOWFLAKE_FIELD_TYPE_MAPPINGS.get(col.data_type, NullType)()
                    ),
                    # NOTE: nativeDataType will not be in sync with older connector
                    nativeDataType=col.data_type,
                    description=col.comment,
                    nullable=col.is_nullable,
                    isPartOfKey=col.name in table.pk.column_names
                    if isinstance(table, SnowflakeTable) and table.pk is not None
                    else None,
                )
                for col in table.columns
            ],
            foreignKeys=foreign_keys,
        )
        return schema_metadata

    def get_identifier(self, table_name: str, schema_name: str, db_name: str) -> str:
        return self.snowflake_identifier(f"{db_name}.{schema_name}.{table_name}")

    def get_report(self) -> SourceReport:
        return self.report

    def get_tables_for_schema(
        self, conn: SnowflakeConnection, schema_name: str, db_name: str
    ) -> List[SnowflakeTable]:

        if db_name not in self.db_tables.keys():
            tables = self.data_dictionary.get_tables_for_database(conn, db_name)
            self.db_tables[db_name] = tables
        else:
            tables = self.db_tables[db_name]

        # get all tables for database failed,
        # falling back to get tables for schema
        if tables is None:
            return self.data_dictionary.get_tables_for_schema(
                conn, schema_name, db_name
            )

        # Some schema may not have any table
        return tables.get(schema_name, [])

    def get_views_for_schema(
        self, conn: SnowflakeConnection, schema_name: str, db_name: str
    ) -> List[SnowflakeView]:

        if db_name not in self.db_views.keys():
            views = self.data_dictionary.get_views_for_database(conn, db_name)
            self.db_views[db_name] = views
        else:
            views = self.db_views[db_name]

        # get all views for database failed,
        # falling back to get views for schema
        if views is None:
            return self.data_dictionary.get_views_for_schema(conn, schema_name, db_name)

        # Some schema may not have any table
        return views.get(schema_name, [])

    def get_columns_for_table(
        self, conn: SnowflakeConnection, table_name: str, schema_name: str, db_name: str
    ) -> List[SnowflakeColumn]:

        if (db_name, schema_name) not in self.schema_columns.keys():
            columns = self.data_dictionary.get_columns_for_schema(
                conn, schema_name, db_name
            )
            self.schema_columns[(db_name, schema_name)] = columns
        else:
            columns = self.schema_columns[(db_name, schema_name)]

        # get all columns for schema failed,
        # falling back to get columns for table
        if columns is None:
            return self.data_dictionary.get_columns_for_table(
                conn, table_name, schema_name, db_name
            )

        # Access to table but none of its columns - is this possible ?
        return columns.get(table_name, [])

    def get_pk_constraints_for_table(
        self, conn: SnowflakeConnection, table_name: str, schema_name: str, db_name: str
    ) -> Optional[SnowflakePK]:

        if (db_name, schema_name) not in self.schema_pk_constraints.keys():
            constraints = self.data_dictionary.get_pk_constraints_for_schema(
                conn, schema_name, db_name
            )
            self.schema_pk_constraints[(db_name, schema_name)] = constraints
        else:
            constraints = self.schema_pk_constraints[(db_name, schema_name)]

        # Access to table but none of its constraints - is this possible ?
        return constraints.get(table_name)

    def get_fk_constraints_for_table(
        self, conn: SnowflakeConnection, table_name: str, schema_name: str, db_name: str
    ) -> List[SnowflakeFK]:

        if (db_name, schema_name) not in self.schema_fk_constraints.keys():
            constraints = self.data_dictionary.get_fk_constraints_for_schema(
                conn, schema_name, db_name
            )
            self.schema_fk_constraints[(db_name, schema_name)] = constraints
        else:
            constraints = self.schema_fk_constraints[(db_name, schema_name)]

        # Access to table but none of its constraints - is this possible ?
        return constraints.get(table_name, [])

    def add_config_to_report(self):
        self.report.cleaned_account_id = self.config.get_account()
        self.report.ignore_start_time_lineage = self.config.ignore_start_time_lineage
        self.report.upstream_lineage_in_report = self.config.upstream_lineage_in_report
        if not self.report.ignore_start_time_lineage:
            self.report.lineage_start_time = self.config.start_time
        self.report.lineage_end_time = self.config.end_time
        self.report.check_role_grants = self.config.check_role_grants

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        log.warning(f"{key} => {reason}")

    def inspect_session_metadata(self, conn: SnowflakeConnection) -> None:
        try:
            logger.info("Checking current version")
            for db_row in self.data_dictionary.query(conn, "select CURRENT_VERSION()"):
                self.report.saas_version = db_row["CURRENT_VERSION()"]
        except Exception as e:
            self.report.report_failure("version", f"Error: {e}")
        try:
            logger.info("Checking current role")
            for db_row in self.data_dictionary.query(conn, "select CURRENT_ROLE()"):
                self.report.role = db_row["CURRENT_ROLE()"]
        except Exception as e:
            self.report.report_failure("version", f"Error: {e}")
        try:
            logger.info("Checking current warehouse")
            for db_row in self.data_dictionary.query(
                conn, "select CURRENT_WAREHOUSE()"
            ):
                self.report.default_warehouse = db_row["CURRENT_WAREHOUSE()"]
        except Exception as e:
            self.report.report_failure("current_warehouse", f"Error: {e}")
        try:
            logger.info("Checking current database")
            for db_row in self.data_dictionary.query(conn, "select CURRENT_DATABASE()"):
                self.report.default_db = db_row["CURRENT_DATABASE()"]
        except Exception as e:
            self.report.report_failure("current_database", f"Error: {e}")
        try:
            logger.info("Checking current schema")
            for db_row in self.data_dictionary.query(conn, "select CURRENT_SCHEMA()"):
                self.report.default_schema = db_row["CURRENT_SCHEMA()"]
        except Exception as e:
            self.report.report_failure("current_schema", f"Error: {e}")

    def wrap_aspect_as_workunit(
        self, entityName: str, entityUrn: str, aspectName: str, aspect: DictWrapper
    ) -> MetadataWorkUnit:
        wu = MetadataWorkUnit(
            id=f"{aspectName}-for-{entityUrn}",
            mcp=MetadataChangeProposalWrapper(
                entityType=entityName,
                entityUrn=entityUrn,
                aspectName=aspectName,
                aspect=aspect,
                changeType=ChangeType.UPSERT,
            ),
        )
        self.report.report_workunit(wu)
        return wu
