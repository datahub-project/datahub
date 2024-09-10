import datetime
import logging

from typing import Optional, Dict, List, Iterable, Union, Tuple, Any, Collection

from pydantic.fields import Field

from concurrent.futures import ThreadPoolExecutor, as_completed

import xml.etree.ElementTree

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

import datahub.sql_parsing.sqlglot_utils
from datahub.configuration.common import AllowDenyPattern
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.utilities.urns.corpuser_urn import CorpuserUrn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    ViewPropertiesClass,
    OtherSchemaClass,
    BooleanTypeClass,
    NumberTypeClass,
    StringTypeClass,
    BytesTypeClass,
    ArrayTypeClass,
    RecordTypeClass,
    DateTypeClass,
    TimeTypeClass,
    SchemaFieldDataTypeClass,
    SubTypesClass,
    TimeStampClass
)
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_data_platform_urn,
    make_user_urn
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    SqlParsingAggregator,
    KnownQueryLineageInfo
)
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    SQLSourceReport
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)

logger = logging.getLogger(__name__)

HANA_TYPES_MAP: Dict[str, Any] = {
    "BOOLEAN": BooleanTypeClass(),
    "TINYINT": NumberTypeClass(),
    "SMALLINT": NumberTypeClass(),
    "INTEGER": NumberTypeClass(),
    "BIGINT": NumberTypeClass(),
    "SMALLDECIMAL": NumberTypeClass(),
    "DECIMAL": NumberTypeClass(),
    "REAL": NumberTypeClass(),
    "DOUBLE": NumberTypeClass(),

    "VARCHAR": StringTypeClass(),
    "NVARCHAR": StringTypeClass(),
    "ALPHANUM": StringTypeClass(),
    "SHORTTEXT": StringTypeClass(),

    "VARBINARY": BytesTypeClass(),

    "BLOB": BytesTypeClass(),
    "CLOB": StringTypeClass(),
    "NCLOB": StringTypeClass(),
    "TEXT": StringTypeClass(),

    "ARRAY": ArrayTypeClass(),

    "ST_GEOMETRY": RecordTypeClass(),
    "ST_POINT": RecordTypeClass(),

    "DATE": DateTypeClass(),
    "TIME": TimeTypeClass(),
    "SECONDDATE": TimeTypeClass(),
    "TIMESTAMP": TimeTypeClass(),
}


class BaseHanaConfig(BasicSQLAlchemyConfig):
    scheme: str = Field(default="hana", hidden_from_docs=True)
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["sys"])
    )
    max_workers: int = Field(
        default=5,
        description=(
            "Maximum concurrent SQL connections to the SAP Hana instance."
        ),
    )


class HanaConfig(BaseHanaConfig):
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for databases to filter in ingestion. "
            "Note: this is not used if `database` or `sqlalchemy_uri` are provided."
        ),
    )
    database: Optional[str] = Field(
        default=None,
        description=(
            "database (catalog). If set to Null, all databases will be considered for ingestion."
        ),
    )


@platform_name("SAP HANA", id="hana")
@config_class(HanaConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class HanaSource(SQLAlchemySource):
    config: HanaConfig
    report: SQLSourceReport = SQLSourceReport()
    aggregator: SqlParsingAggregator

    def __init__(self, config: HanaConfig, ctx: PipelineContext):
        super().__init__(config, ctx, self.get_platform())
        self.config = config
        self.engine = self._create_engine()
        # self.discovered_tables: Optional[Collection[str]] = None
        self.aggregator = SqlParsingAggregator(
            platform=self.get_platform(),
            platform_instance=self.config.platform_instance if self.config.platform_instance else None,
            env=self.config.env,
            graph=ctx.graph,
            generate_lineage=True,
            generate_queries=True,
            generate_usage_statistics=True,
            generate_operations=True,
            format_queries=True,
            usage_config=BaseUsageConfig(),
            # is_allowed_table=self.is_allowed_table,
        )

    def get_platform(self):
        return "hana"

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "HanaSource":
        config = HanaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _create_engine(self):
        url = self.config.get_sql_alchemy_url()
        return create_engine(url)

    def get_table_properties(self, inspector: Inspector, schema: str, table: str) -> Optional[DatasetPropertiesClass]:
        description = inspector.get_table_comment(table, schema).get("text", "")
        return DatasetPropertiesClass(name=table, description=description)

    def get_view_properties(self, schema: str, view: str) -> Tuple[str, str, datetime.datetime, str]:
        query = f"""
                    SELECT DEFINITION,
                    COMMENTS,
                    CREATE_TIME,
                    VIEW_TYPE
                    FROM SYS.VIEWS
                    WHERE LOWER(SCHEMA_NAME) = '{schema}' AND LOWER(VIEW_NAME) = '{view}'
                """
        result = self.engine.execute(query).fetchone()
        return result if result else ("", "", datetime.datetime(year=1970, month=1, day=1), "")

    def get_view_info(self, view: str, definition_and_description: Tuple) -> Optional[DatasetPropertiesClass]:
        return DatasetPropertiesClass(
            name=view,
            description=definition_and_description[1],
            created=TimeStampClass(
                time=int(definition_and_description[2].timestamp()) * 1000
            ),
            customProperties={
                "View Type": definition_and_description[3],
            }
        )

    def get_lineage(self, schema: str, table_or_view: str) -> list:
        query = f"""
            SELECT LOWER(BASE_SCHEMA_NAME) as BASE_SCHEMA_NAME,
            LOWER(BASE_OBJECT_NAME) AS BASE_OBJECT_NAME
            FROM SYS.OBJECT_DEPENDENCIES
            WHERE LOWER(DEPENDENT_SCHEMA_NAME) = '{schema.lower()}'
            AND LOWER(DEPENDENT_OBJECT_NAME) = '{table_or_view.lower()}'
        """
        return self.engine.execute(query).fetchall()

    def construct_lineage(self, upstreams: List[Tuple[str, str]]) -> Optional[UpstreamLineageClass]:

        if not upstreams:
            return None

        upstream = [
            UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    platform=self.get_platform(),
                    name=f"{(self.config.database.lower() + '.') if self.config.database else ''}{row[0].lower()}.{row[1].lower()}",
                    platform_instance=self.config.platform_instance,
                    env=self.config.env
                ),
                type=DatasetLineageTypeClass.VIEW,
            )
            for row in upstreams
        ]
        return UpstreamLineageClass(upstreams=upstream)

    def get_columns(self, inspector: Inspector, schema: str, table_or_view: str) -> List[SchemaFieldClass]:
        columns = inspector.get_columns(table_or_view, schema)
        return [
            SchemaFieldClass(
                fieldPath=f"[version=2.0].[type={col.get('data_type_name')}].{col.get('column_name')}",
                type=SchemaFieldDataTypeClass(
                    type=HANA_TYPES_MAP.get(
                        col.get('data_type_name')
                    )
                ),
                nativeDataType=col.get('data_type_name'),
                description=col.get("comment", ""),
                nullable=bool(col.get("is_nullable")),

            )
            for col in columns
        ]

    def get_calculation_view_columns(self, view: str) -> List[SchemaFieldClass]:
        query = f"""
            SELECT COLUMN_NAME as column_name,
            COMMENTS as comments,
            DATA_TYPE_NAME as data_type_name,
            IS_NULLABLE as is_nullable
            FROM SYS.VIEW_COLUMNS
            WHERE "SCHEMA_NAME" = '_SYS_BIC'
            AND LOWER(VIEW_NAME) = '{view.lower()}'
            ORDER BY POSITION ASC
        """
        query_result = [dict(row) for row in self.engine.execute(query).fetchall()]
        logger.info(f"View: {view.lower()} definition: {query_result}")

        columns = query_result

        return [
            SchemaFieldClass(
                fieldPath=f"[version=2.0].[type={col.get('data_type_name')}].{col.get('column_name')}",
                type=SchemaFieldDataTypeClass(
                    type=HANA_TYPES_MAP.get(
                        col.get('data_type_name')
                    )
                ),
                nativeDataType=col.get('data_type_name'),
                description=col.get('comments', ""),
                nullable=bool(col.get("is_nullable")),
            )
            for col in columns
        ]

    def get_query_logs(self) -> List[Dict]:
        query = """
            SELECT STATEMENT_STRING,
            USER_NAME, LAST_EXECUTION_TIMESTAMP
            FROM SYS.M_SQL_PLAN_CACHE"""
        result = self.engine.execute(query).fetchall()
        return [dict(row) for row in result]

    def get_package_names(self) -> List[dict]:
        query = """
            SELECT
            PACKAGE_ID,
            OBJECT_NAME,
            TO_VARCHAR(CDATA) AS CDATA
            FROM _SYS_REPO.ACTIVE_OBJECT
            WHERE LOWER(OBJECT_SUFFIX)='calculationview'"""
        result = self.engine.execute(query).fetchall()
        packages = [dict(row) for row in result] if result else []
        logging.info(packages)
        return packages

    def get_schema_names(self, inspector):
        return inspector.get_schema_names()

    def add_information_for_schema(self, inspector: Inspector, schema: str) -> None:
        pass

    def get_allowed_schemas(self, inspector: Inspector) -> Iterable[str]:
        # this function returns the schema names which are filtered by schema_pattern.
        for schema in self.get_schema_names(inspector):
            if not self.config.schema_pattern.allowed(schema):
                self.report.report_dropped(f"{schema}.*")
                continue
            else:
                self.add_information_for_schema(inspector, schema)
                yield schema

    def get_workunits_internal(self):
        inspector = inspect(self.engine)
        schemas = self.get_allowed_schemas(
            inspector=inspector,
        )
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = []

            for schema in schemas:
                if schema.lower() == "_sys_bic":
                    views = self.get_package_names()
                    for view in views:
                        futures.append(
                            executor.submit(
                                self._process_calculation_view,
                                dataset_path=view.get("package_id"),
                                dataset_name=view.get("object_name"),
                                dataset_definition=view.get("cdata"),
                                inspector=inspector,
                                schema=schema,
                            )
                        )

                else:
                    tables = inspector.get_table_names(schema)
                    for table in tables:
                        futures.append(
                            executor.submit(
                                self._process_table,
                                dataset_name=table,
                                inspector=inspector,
                                schema=schema,
                            )
                        )

                    views = inspector.get_view_names(schema)
                    for view in views:
                        futures.append(
                            executor.submit(
                                self._process_view,
                                dataset_name=view,
                                inspector=inspector,
                                schema=schema
                            )
                        )

            queries = self.get_query_logs()
            for query in queries:
                self.aggregator.add_observed_query(
                    query=query.get("statement_string"),
                    query_timestamp=query.get("last_execution_timestamp"),
                    user=CorpuserUrn(
                        make_user_urn(
                            query.get("user_name")
                        )
                    ),
                )

            for future in as_completed(futures):
                yield from future.result()

        for mcp in self.aggregator.gen_metadata():
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

    def _process_table(self,
                       dataset_name: str,
                       inspector: Inspector,
                       schema: str,
                       ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        entity = make_dataset_urn_with_platform_instance(
            platform=self.get_platform(),
            name=f"{(self.config.database.lower() + '.') if self.config.database else ''}{schema.lower()}.{dataset_name.lower()}",
            platform_instance=self.config.platform_instance,
            env=self.config.env
        )
        description = self.get_table_properties(inspector=inspector, schema=schema, table=dataset_name)
        columns = self.get_columns(inspector=inspector, schema=schema, table_or_view=dataset_name)
        schema_metadata = SchemaMetadataClass(
            schemaName=f"{schema}.{dataset_name}",
            platform=make_data_platform_urn(self.get_platform()),
            version=0,
            fields=columns,
            hash="",
            platformSchema=OtherSchemaClass(""),
        )

        dataset_snapshot = MetadataChangeProposalWrapper.construct_many(
            entityUrn=entity,
            aspects=[
                description,
                schema_metadata,
            ]
        )

        self.aggregator.register_schema(
            urn=entity,
            schema=schema_metadata,
        )

        for mcp in dataset_snapshot:
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

    def _process_view(self,
                      dataset_name: str,
                      inspector: Inspector,
                      schema: str,
                      ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        entity = make_dataset_urn_with_platform_instance(
            platform=self.get_platform(),
            name=f"{(self.config.database.lower() + '.') if self.config.database else ''}{schema.lower()}.{dataset_name.lower()}",
            platform_instance=self.config.platform_instance,
            env=self.config.env
        )

        view_details = self.get_view_properties(schema=schema, view=dataset_name)
        view_definition = view_details[0]
        properties = self.get_view_info(view=dataset_name, definition_and_description=view_details)

        columns = self.get_columns(inspector=inspector, schema=schema, table_or_view=dataset_name)
        constructed_lineage = self.get_lineage(schema=schema, table_or_view=dataset_name)
        lineage = self.construct_lineage(upstreams=constructed_lineage)
        subtype = SubTypesClass(["View"])

        schema_metadata = SchemaMetadataClass(
            schemaName=f"{schema}.{dataset_name}",
            platform=make_data_platform_urn(self.get_platform()),
            version=0,
            fields=columns,
            hash="",
            platformSchema=OtherSchemaClass(""),
        )

        view_properties = ViewPropertiesClass(
            materialized=False,
            viewLanguage="SQL",
            viewLogic=view_definition,
        )

        aspects = [
            properties,
            schema_metadata,
            view_properties,
            subtype,
        ]

        if lineage:
            aspects.append(lineage)

        dataset_snapshot = MetadataChangeProposalWrapper.construct_many(
            entityUrn=entity,
            aspects=aspects
        )

        self.aggregator.register_schema(
            urn=entity,
            schema=schema_metadata
        )

        self.aggregator.add_view_definition(
            view_urn=entity,
            view_definition=view_definition,
            default_db=self.config.database if self.config.database else "",
            default_schema=schema
        )

        if constructed_lineage:
            self.aggregator.add_known_query_lineage(
                KnownQueryLineageInfo(
                    query_text=view_definition,
                    upstreams=[
                        make_dataset_urn_with_platform_instance(
                            platform=self.get_platform(),
                            name=f"{(self.config.database.lower() + '.') if self.config.database else ''}{row[0].lower()}.{row[1].lower()}",
                            platform_instance=self.config.platform_instance,
                            env=self.config.env
                        ) for row in constructed_lineage
                    ],
                    downstream=entity,
                    query_type=QueryType.SELECT
                ),
                merge_lineage=True,
            )

        for mcp in dataset_snapshot:
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

    def get_calculation_view_lineage(self, root: xml.etree.ElementTree, ns: Dict, dataset_path: str, dataset_name: str):
        upstreams = []
        data_sources = root.find("dataSources", ns)

        try:
            if data_sources:
                for data_source in data_sources.findall("DataSource", ns):
                    data_source_type = data_source.get("type")
                    if data_source_type.upper() == "CALCULATION_VIEW":
                        upstreams.append(
                            f"_sys_bic.{data_source.find('resourceUri', ns).text[1:].replace('/calculationviews/','.')}"
                        )
                    else:
                        column_object = data_source.find("columnObject", ns)
                        upstreams.append(f"{column_object.get('schemaName')}.{column_object.get('columnObjectName')}")

        except Exception as e:
            logging.warning(
                f"No lineage found for Calculation View {dataset_path}/{dataset_path}. Parsing error: {e}"
            )

        if not upstreams:
            return None

        upstream = [
            UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    platform=self.get_platform(),
                    name=row.lower(),
                    platform_instance=self.config.platform_instance,
                    env=self.config.env
                ),
                type=DatasetLineageTypeClass.VIEW,
            )
            for row in upstreams
        ]
        return UpstreamLineageClass(upstreams=upstream)

    def _process_calculation_view(
            self,
            dataset_path: str,
            dataset_name: str,
            dataset_definition: str,
            inspector: Inspector,
            schema: str
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        entity = make_dataset_urn_with_platform_instance(
            platform=self.get_platform(),
            name=f"_sys_bic.{dataset_path.lower()}.{dataset_name.lower()}",
            platform_instance=self.config.platform_instance,
            env=self.config.env
        )

        try:
            root = xml.etree.ElementTree.fromstring(dataset_definition)
        except Exception as e:
            logging.error(e)
            root = None

        ns = {
            "Calculation": "http://www.sap.com/ndb/BiModelCalculation.ecore",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"
        }
        if root:
            lineage = self.get_calculation_view_lineage(
                root=root,
                ns=ns,
                dataset_path=dataset_path,
                dataset_name=dataset_name
            )

            columns = self.get_calculation_view_columns(
                view=f"{dataset_path}/{dataset_name}"
            )

            schema_metadata = SchemaMetadataClass(
                schemaName=f"{schema}.{dataset_name}",
                platform=make_data_platform_urn(self.get_platform()),
                version=0,
                fields=columns,
                hash="",
                platformSchema=OtherSchemaClass(""),
            )

            dataset_details = DatasetPropertiesClass(name=dataset_name)

            view_properties = ViewPropertiesClass(
                materialized=False,
                viewLanguage="XML",
                viewLogic=dataset_definition,
            )

            subtype = SubTypesClass(["View"])

            aspects = [
                schema_metadata,
                subtype,
                dataset_details,
                view_properties,
            ]

            if lineage:
                aspects.append(lineage)

            dataset_snapshot = MetadataChangeProposalWrapper.construct_many(
                entityUrn=entity,
                aspects=aspects
            )

            self.aggregator.register_schema(
                urn=entity,
                schema=schema_metadata
            )

            for mcp in dataset_snapshot:
                self.report.report_workunit(mcp.as_workunit())
                yield mcp.as_workunit()


def _sql_dialect(platform: str) -> str:
    return "tsql"


datahub.sql_parsing.sqlglot_utils._get_dialect_str = _sql_dialect
