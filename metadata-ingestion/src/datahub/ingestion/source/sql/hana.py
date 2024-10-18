import datetime
import logging
import uuid
import re

from typing import Optional, Dict, List, Iterable, Union, Tuple, Any, Set

from pydantic.fields import Field

from concurrent.futures import ThreadPoolExecutor, as_completed

import xml.etree.ElementTree as ET

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
from datahub.ingestion.api.source import SourceReport
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
    TimeStampClass,
    BrowsePathsV2Class,
    BrowsePathEntryClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    ContainerClass,
    FineGrainedLineageClass,
    FineGrainedLineageUpstreamTypeClass,
    FineGrainedLineageDownstreamTypeClass,
)
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_data_platform_urn,
    make_user_urn,
    make_container_urn,
    make_dataplatform_instance_urn,
    make_schema_field_urn,
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    SqlParsingAggregator,
    KnownQueryLineageInfo,
    ObservedQuery,
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


class SAPCalculationViewParser:
    def __init__(self):
        pass

    @staticmethod
    def _parseCalc(view: str, xml: ET) -> Dict:
        sources = {}
        outputs = {}
        nodes = {}

        try:
            for child in xml.iter('DataSource'):
                source = {'type': child.attrib['type']}
                for grandchild in child:
                    if grandchild.tag == 'columnObject':
                        source['name'] = grandchild.attrib['columnObjectName']
                        source['path'] = grandchild.attrib['schemaName']
                    elif grandchild.tag == 'resourceUri':
                        source['name'] = grandchild.text.split(sep="/")[-1]
                        source['path'] = "/".join(grandchild.text.split(sep="/")[:-1])
                sources[child.attrib['id']] = source

            for child in xml.findall(".//logicalModel/attributes/attribute"):
                output = {
                    'source': child.find('keyMapping').attrib['columnName'],
                    'node': child.find('keyMapping').attrib['columnObjectName'],
                    'type': 'attribute',
                }
                outputs[child.attrib['id']] = output

            for child in xml.findall(".//logicalModel/baseMeasures/measure"):
                output = {
                    'source': child.find('measureMapping').attrib['columnName'],
                    'node': child.find('measureMapping').attrib['columnObjectName'],
                    'type': 'measure',
                }
                outputs[child.attrib['id']] = output

            for child in xml.iter('calculationView'):
                node = {
                    'type': child.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'],
                    'sources': {},
                    'id': child.attrib['id'],
                }

                if node['type'] == 'Calculation:UnionView':
                    node['unions'] = []
                    for grandchild in child.iter('input'):
                        union_source = grandchild.attrib['node'].split('#')[-1]
                        node['unions'].append(union_source)
                        node['sources'][union_source] = {}
                        for mapping in grandchild.iter('mapping'):
                            if 'source' in mapping.attrib:
                                node['sources'][union_source][mapping.attrib['target']] = {
                                    'type': 'column',
                                    'source': mapping.attrib['source']
                                }
                else:
                    for grandchild in child.iter('input'):
                        input_source = grandchild.attrib['node'].split('#')[-1]
                        node['sources'][input_source] = {}
                        for mapping in grandchild.iter('mapping'):
                            if 'source' in mapping.attrib:
                                node['sources'][input_source][mapping.attrib['target']] = {
                                    'type': 'column',
                                    'source': mapping.attrib['source']
                                }

                for grandchild in child.iter('calculatedViewAttribute'):
                    formula = grandchild.find('formula')
                    if formula is not None:
                        node['sources'][grandchild.attrib['id']] = {
                            'type': 'formula',
                            'source': formula.text
                        }

                nodes[node['id']] = node
        except Exception as e:
            logger.error(e)

        return {
            'viewName': view,
            'sources': sources,
            'outputs': outputs,
            'nodes': nodes
        }

    @staticmethod
    def _extract_columns_from_formula(formula: str) -> List[str]:
        return re.findall(r'\"([^\"]*)\"', formula)

    def _find_all_sources(
            self,
            calc_view: Dict,
            column: str,
            node: str,
            visited: Set[str]
    ) -> List[Dict]:
        sources = []

        try:
            if node in visited:
                return []
            visited.add(node)

            node_info = calc_view['nodes'].get(node)

            if not node_info:
                source = calc_view['sources'].get(node)
                if source:
                    return [{
                        'column': column,
                        'source': source['name'],
                        'sourceType': source['type'],
                        'sourcePath': source.get('path', '')
                    }]
                return []

            if node_info['type'] == 'Calculation:UnionView':
                for union_source in node_info['unions']:
                    for col, col_info in node_info['sources'][union_source].items():
                        if col == column:
                            new_sources = self._find_all_sources(
                                calc_view,
                                col_info['source'],
                                union_source,
                                visited.copy()
                            )
                            if not new_sources and union_source in calc_view['sources']:
                                new_sources = [{
                                    'column': col_info['source'],
                                    'source': f"{calc_view['sources'][union_source]['path']}/{calc_view['sources'][union_source]['name']}" if
                                    calc_view['sources'][union_source]['type'] == 'CALCULATION_VIEW' else
                                    calc_view['sources'][union_source]['name'],
                                    'sourceType': calc_view['sources'][union_source]['type'],
                                    'sourcePath': calc_view['sources'][union_source]['path'],
                                }]
                            sources.extend(new_sources)
            else:
                for source, columns in node_info['sources'].items():
                    if column in columns:
                        col_info = columns[column]
                        if col_info['type'] == 'formula':
                            formula_columns = self._extract_columns_from_formula(col_info['source'])
                            for formula_column in formula_columns:
                                sources.extend(self._find_all_sources(calc_view, formula_column, node, visited.copy()))
                        elif source in calc_view['sources']:
                            sources.append({
                                'column': col_info['source'],
                                'source': f"{calc_view['sources'][source]['path']}/{calc_view['sources'][source]['name']}" if
                                calc_view['sources'][source]['type'] == 'CALCULATION_VIEW' else
                                calc_view['sources'][source][
                                    'name'],
                                'sourceType': calc_view['sources'][source]['type'],
                                'sourcePath': calc_view['sources'][source]['path'],
                            })
                        else:
                            sources.extend(
                                self._find_all_sources(calc_view, col_info['source'], source, visited.copy())
                            )
        except Exception as e:
            logger.error(e)

        return sources

    def _allColumnsOrigin(self, view: str, definition: str) -> Dict[str, List[Dict]]:
        calc_view = self._parseCalc(view, definition)
        columns_lineage = {}

        try:
            for output, output_info in calc_view['outputs'].items():
                sources = self._find_all_sources(calc_view, output_info['source'], output_info['node'], set())
                columns_lineage[output] = sources

            # Handle calculated attributes
            for node in calc_view['nodes'].values():
                for column, col_info in node['sources'].items():
                    if isinstance(col_info, dict) and col_info.get('type') == 'formula':
                        formula_columns = self._extract_columns_from_formula(col_info['source'])
                        sources = []
                        for formula_column in formula_columns:
                            sources.extend(self._find_all_sources(calc_view, formula_column, node['id'], set()))
                        columns_lineage[column] = sources
        except Exception as e:
            logger.error(e)

        return columns_lineage

    def format_column_dictionary(
            self,
            view_name: str,
            view_definition: str,
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        column_dicts: List[Dict[str, List[Dict[str, str]]]] = []

        try:
            output_columns = self._allColumnsOrigin(
                view_name,
                view_definition,
            )

            for cols, src in output_columns.items():
                column_dicts.append(
                    {
                        "downstream_column": cols,
                        "upstream": [
                            {
                                "upstream_table":
                                    f"{src_col.get('sourcePath')}.{src_col.get('source')}".lower()
                                    if src_col.get('sourceType') == "DATA_BASE_TABLE" else
                                    f"_sys_bic.{src_col.get('source').replace('::', '/').lower()}"
                                    if src_col.get('sourceType') == "TABLE_FUNCTION" else
                                    f"_sys_bic.{src_col.get('source')[1:].replace('/calculationviews/', '/')}".lower(),
                                "upstream_column": src_col.get('column')
                            }
                            for src_col in src
                        ]
                    }
                )

        except Exception as e:
            logger.error(e)

        return column_dicts


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

    def create_container(self, schema: str) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:

        browse_path = self.get_browse_path(
            schema=schema,
        )

        container_urn = self.get_container_urn(
            schema=schema,
        )

        container_name = ContainerPropertiesClass(
            name=schema,
        )

        conatiner_type = SubTypesClass(
            typeNames=["Schema"],
        )

        platform = self.get_platform_instance()

        container_snapshot = MetadataChangeProposalWrapper.construct_many(
            entityUrn=container_urn,
            aspects=[
                container_name,
                conatiner_type,
                browse_path,
                platform
            ]
        )

        for mcp in container_snapshot:
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

    def get_browse_path(self, schema: str) -> BrowsePathsV2Class:
        container_path: List[BrowsePathEntryClass] = []

        if self.config.database:
            container_path.append(
                BrowsePathEntryClass(
                    id=self.config.database.lower()
                )
            )

        container_path.append(
            BrowsePathEntryClass(
                id=schema,
                urn=self.get_container_urn(
                    schema=schema
                )
            )
        )

        return BrowsePathsV2Class(
            path=container_path
        )

    def get_container_urn(self, schema: str) -> str:
        namespace = uuid.NAMESPACE_DNS

        return make_container_urn(
            guid=str(
                uuid.uuid5(
                    namespace,
                    self.get_platform() +
                    self.config.platform_instance +
                    schema,
                )
            )
        )

    def get_container_class(self, schema: str) -> ContainerClass:
        return ContainerClass(
            container=self.get_container_urn(
                schema=schema,
            )
        )

    def get_platform_instance(self):
        return DataPlatformInstanceClass(
            platform=f"urn:li:dataPlatform:{self.get_platform()}",
            instance=(
                make_dataplatform_instance_urn(
                    self.get_platform(), self.config.platform_instance
                )
                if self.config.platform_instance
                else None
            )
        )

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
        upstream_tables: List[str] = []
        for row_item in upstreams:
            if isinstance(row_item[0], str) and isinstance(row_item[1], str):
                upstream_tables.append(f"{row_item[0].lower()}.{row_item[1].lower()}")

        if not upstream_tables:
            return None

        upstream = [
            UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    platform=self.get_platform(),
                    name=f"{(self.config.database.lower() + '.') if self.config.database else ''}{row}",
                    platform_instance=self.config.platform_instance,
                    env=self.config.env
                ),
                type=DatasetLineageTypeClass.VIEW,
            )
            for row in upstream_tables
        ]
        return UpstreamLineageClass(upstreams=upstream)

    def get_columns(self, schema: str, table_or_view: str) -> List[SchemaFieldClass]:
        query = f"""
                    SELECT COLUMN_NAME as column_name,
                    COMMENTS as comments,
                    DATA_TYPE_NAME as data_type_name,
                    IS_NULLABLE as is_nullable
                    FROM (
                        SELECT POSITION,
                        COLUMN_NAME,
                        COMMENTS,
                        DATA_TYPE_NAME,
                        IS_NULLABLE,
                        SCHEMA_NAME,
                        TABLE_NAME
                        FROM SYS.TABLE_COLUMNS UNION ALL
                        SELECT POSITION,
                        COLUMN_NAME,
                        COMMENTS,
                        DATA_TYPE_NAME,
                        IS_NULLABLE,
                        SCHEMA_NAME,
                        VIEW_NAME AS TABLE_NAME
                        FROM SYS.VIEW_COLUMNS ) AS COLUMNS
                    WHERE LOWER("SCHEMA_NAME") = '{schema.lower()}'
                    AND LOWER(TABLE_NAME) = '{table_or_view.lower()}'
                    ORDER BY POSITION ASC
                """
        query_result = [dict(row) for row in self.engine.execute(query).fetchall()]

        columns = query_result

        return [
            SchemaFieldClass(
                fieldPath=col.get('column_name'),
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

    def get_calculation_view_columns(self, view: str) -> List[SchemaFieldClass]:
        query = f"""
            SELECT COLUMN_NAME as column_name,
            COMMENTS as comments,
            DATA_TYPE_NAME as data_type_name,
            IS_NULLABLE as is_nullable
            FROM SYS.VIEW_COLUMNS
            WHERE LOWER("SCHEMA_NAME") = '_sys_bic'
            AND LOWER(VIEW_NAME) = '{view.lower()}'
            ORDER BY POSITION ASC
        """
        query_result = [dict(row) for row in self.engine.execute(query).fetchall()]

        columns = query_result

        return [
            SchemaFieldClass(
                fieldPath=col.get('column_name'),
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

                yield from self.create_container(
                    schema=schema,
                )

                if schema.lower() == "_sys_bic":
                    views = self.get_package_names()
                    for view in views:
                        futures.append(
                            executor.submit(
                                self._process_calculation_view,
                                dataset_path=view.get("package_id"),
                                dataset_name=view.get("object_name"),
                                dataset_definition=view.get("cdata"),
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
                                schema=schema
                            )
                        )

            for future in as_completed(futures):
                yield from future.result()

            queries = self.get_query_logs()
            for query in queries:
                self.aggregator.add_observed_query(
                    observed=ObservedQuery(
                        query=query.get("statement_string"),
                        timestamp=query.get("last_execution_timestamp"),
                        user=CorpuserUrn(
                            make_user_urn(
                                query.get("user_name")
                            )
                        )
                    ),
                )

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
        columns = self.get_columns(schema=schema, table_or_view=dataset_name)
        schema_metadata = SchemaMetadataClass(
            schemaName=f"{schema}.{dataset_name}",
            platform=make_data_platform_urn(self.get_platform()),
            version=0,
            fields=columns,
            hash="",
            platformSchema=OtherSchemaClass(""),
        )

        platform = self.get_platform_instance()

        browse_path = self.get_browse_path(
            schema=schema,
        )

        container = self.get_container_class(
            schema=schema,
        )

        subtype = SubTypesClass(["Table"])

        dataset_snapshot = MetadataChangeProposalWrapper.construct_many(
            entityUrn=entity,
            aspects=[
                description,
                schema_metadata,
                platform,
                browse_path,
                container,
                subtype,
            ]
        )

        self.aggregator.register_schema(
            urn=entity,
            schema=schema_metadata,
        )

        self.aggregator.is_allowed_table(entity)

        for mcp in dataset_snapshot:
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

    def _process_view(self,
                      dataset_name: str,
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

        columns = self.get_columns(schema=schema, table_or_view=dataset_name)
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

        platform = self.get_platform_instance()

        browse_path = self.get_browse_path(
            schema=schema,
        )

        container = self.get_container_class(
            schema=schema,
        )

        aspects = [
            properties,
            schema_metadata,
            view_properties,
            subtype,
            platform,
            browse_path,
            container
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

        self.aggregator.is_allowed_table(entity)

        self.aggregator.add_view_definition(
            view_urn=entity,
            view_definition=view_definition,
            default_db=self.config.database if self.config.database else "",
            default_schema=schema
        )

        if constructed_lineage:
            upstream_tables: List[str] = []
            for row_item in constructed_lineage:
                if isinstance(row_item[0], str) and isinstance(row_item[1], str):
                    upstream_tables.append(f"{row_item[0].lower()}.{row_item[1].lower()}")

            try:
                self.aggregator.add_known_query_lineage(
                    KnownQueryLineageInfo(
                        query_text=view_definition,
                        upstreams=[
                            make_dataset_urn_with_platform_instance(
                                platform=self.get_platform(),
                                name=f"{(self.config.database.lower() + '.') if self.config.database else ''}{row}",
                                platform_instance=self.config.platform_instance,
                                env=self.config.env
                            ) for row in upstream_tables
                        ],
                        downstream=entity,
                        query_type=QueryType.SELECT
                    ),
                    merge_lineage=True,
                )
            except Exception as e:
                logging.error(e)

        for mcp in dataset_snapshot:
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

    def get_calculation_view_lineage(self, root: ET, ns: Dict, dataset_path: str, dataset_name: str):
        upstreams = []
        data_sources = root.find("dataSources", ns)

        try:
            if data_sources:
                for data_source in data_sources.findall("DataSource", ns):
                    data_source_type = data_source.get("type")
                    if data_source_type.upper() == "CALCULATION_VIEW":
                        upstreams.append(
                            f"_sys_bic.{data_source.find('resourceUri', ns).text[1:].replace('/calculationviews/', '/')}"
                        )
                    else:
                        column_object = data_source.find("columnObject", ns)
                        upstreams.append(f"{column_object.get('schemaName')}.{column_object.get('columnObjectName')}")

        except Exception as e:
            logging.error(e)

        if not upstreams:
            return None

        upstream = [
            UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    platform=self.get_platform(),
                    name=f"{(self.config.database.lower() + '.') if self.config.database else ''}{row.lower()}",
                    platform_instance=self.config.platform_instance,
                    env=self.config.env
                ),
                type=DatasetLineageTypeClass.VIEW,
            )
            for row in upstreams
        ]

        fine_grained_lineage: List[FineGrainedLineageClass] = []

        try:
            for column_lineage in SAPCalculationViewParser().format_column_dictionary(
                    view_name=f"{dataset_path}/{dataset_name}",
                    view_definition=root,
            ):
                if column_lineage.get("upstream"):
                    downstream_column = column_lineage.get("downstream_column")
                    upstream_columns: List[str] = []
                    for column in column_lineage.get("upstream"):
                        upstream_columns.append(
                            make_schema_field_urn(
                                parent_urn=make_dataset_urn_with_platform_instance(
                                    platform=self.get_platform(),
                                    name=f"{(self.config.database.lower() + '.') if self.config.database else ''}{column.get('upstream_table')}",
                                    platform_instance=self.config.platform_instance,
                                    env=self.config.env
                                ),
                                field_path=column.get("upstream_column"),
                            )
                        )

                    fine_grained_lineage.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=upstream_columns,
                            downstreams=[
                                make_schema_field_urn(
                                    parent_urn=make_dataset_urn_with_platform_instance(
                                        platform=self.get_platform(),
                                        name=f"{(self.config.database.lower() + '.') if self.config.database else ''}_sys_bic.{dataset_path.lower()}/{dataset_name.lower()}",
                                        platform_instance=self.config.platform_instance,
                                        env=self.config.env
                                    ),
                                    field_path=downstream_column,
                                )
                            ],
                            confidenceScore=1.0,
                        )
                    )

        except Exception as e:
            logging.error(e)

        return UpstreamLineageClass(
            upstreams=upstream,
            fineGrainedLineages=fine_grained_lineage
        )

    def _process_calculation_view(
            self,
            dataset_path: str,
            dataset_name: str,
            dataset_definition: str,
            schema: str
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        entity = make_dataset_urn_with_platform_instance(
            platform=self.get_platform(),
            name=f"{(self.config.database.lower() + '.') if self.config.database else ''}_sys_bic.{dataset_path.lower()}/{dataset_name.lower()}",
            platform_instance=self.config.platform_instance,
            env=self.config.env
        )

        try:
            root = ET.fromstring(dataset_definition)
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
                schemaName=f"{schema}/{dataset_name}",
                platform=make_data_platform_urn(self.get_platform()),
                version=0,
                fields=columns,
                hash="",
                platformSchema=OtherSchemaClass(""),
            )

            dataset_details = DatasetPropertiesClass(
                name=f"{dataset_path}/{dataset_name}"
            )

            view_properties = ViewPropertiesClass(
                materialized=False,
                viewLanguage="XML",
                viewLogic=dataset_definition,
            )

            platform = self.get_platform_instance()

            browse_path = self.get_browse_path(
                schema=schema,
            )

            container = self.get_container_class(
                schema=schema,
            )

            subtype = SubTypesClass(["View"])

            aspects = [
                schema_metadata,
                subtype,
                dataset_details,
                view_properties,
                platform,
                browse_path,
                container,
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

            self.aggregator.is_allowed_table(entity)

            for mcp in dataset_snapshot:
                self.report.report_workunit(mcp.as_workunit())
                yield mcp.as_workunit()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass


def _sql_dialect(platform: str) -> str:
    return "tsql"


datahub.sql_parsing.sqlglot_utils._get_dialect_str = _sql_dialect
