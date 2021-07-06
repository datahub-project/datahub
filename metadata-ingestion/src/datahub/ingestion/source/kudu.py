# This import verifies that the dependencies are available.
import impala  # noqa: F401
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from typing import Optional
from datahub.ingestion.api.source import Source, SourceReport
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union
from dataclasses import dataclass, field
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
import logging
from sqlalchemy import create_engine, inspect
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger: logging.Logger = logging.getLogger(__name__)

# register_custom_type(HiveDate, DateTypeClass)
# register_custom_type(HiveTimestamp, TimeTypeClass)
# register_custom_type(HiveDecimal, NumberTypeClass)

DEFAULT_ENV = "PROD"

class KuduConfig(ConfigModel):
    # defaults
    scheme: str = "impala"
    database: str = None
    host: str = "localhost:21050"    
    authMechanism: Optional[str] = None
    options: dict = {}
    env: str = DEFAULT_ENV
    
    def get_sql_alchemy_url(self):
        url = f"{self.scheme}://{self.host}/{self.database}"
        return url
class KuduDBSourceReport(SourceReport):
    tables_scanned: int = 0
    views_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table
        """
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)



class KuduSource(Source):
    config: KuduConfig
    report: KuduDBSourceReport
    
    def __init__(self, config, ctx):
        super().__init__(ctx)
        self.config = config
        self.report = KuduDBSourceReport()
        self.platform = 'kudu'
        options = {
            **self.config.options,
        }

    @classmethod
    def create(cls, config_dict, ctx):
        config = KuduConfig.parse_obj(config_dict)
        return cls(config, ctx)
    
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        sql_config = self.config
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            sql_config.options["echo"] = True

        url = sql_config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **sql_config.options)
        inspector = inspect(engine)
        for schema in inspector.get_schema_names():            
            yield from self.loop_tables(inspector, schema, sql_config)

    def loop_tables(
        self,
        inspector: Any,
        schema: str,
        sql_config: KuduConfig,
    ) -> Iterable[MetadataWorkUnit]:
        for table in inspector.get_table_names(schema):
            # schema, table = sql_config.standardize_schema_table_names(schema, table)
            # dataset_name = sql_config.get_identifier(schema, table)
            dataset_name = f"{sql_config.database}.{table}"
            self.report.report_entity_scanned(dataset_name, ent_type="table")            

            columns = inspector.get_columns(table, schema)
            try:
                table_info: dict = inspector.get_table_comment(table, schema)
            except NotImplementedError:
                description: Optional[str] = None
                properties: Dict[str, str] = {}
            else:
                description = table_info["text"]

                # The "properties" field is a non-standard addition to SQLAlchemy's interface.
                properties = table_info.get("properties", {})            

            dataset_snapshot = DatasetSnapshot(
                urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name})",
                aspects=[],
            )
            if description is not None or properties:
                dataset_properties = DatasetPropertiesClass(
                    description=description,
                    customProperties=properties,
                    # uri=dataset_name,
                )
                dataset_snapshot.aspects.append(dataset_properties)
            # schema_metadata = get_schema_metadata(
            #     self.report, dataset_name, self.platform, columns
            # )
            # dataset_snapshot.aspects.append(schema_metadata)
            logger.info(columns)
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = MetadataWorkUnit(id=dataset_name, mce=mce)
            self.report.report_workunit(wu)
            yield wu            

    def get_report(self):
        return self.report

    def close(self):
        pass