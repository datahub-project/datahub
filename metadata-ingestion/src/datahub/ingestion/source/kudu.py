# This import verifies that the dependencies are available.
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Type

from krbcontext.context import krbContext
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import sqltypes as types

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemalessClass,
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


@dataclass
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
        else:
            raise KeyError(f"Unknown entity {ent_type}.")

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)


_field_type_mapping: Dict[Type[types.TypeEngine], Type] = {
    types.Integer: NumberTypeClass,
    types.Numeric: NumberTypeClass,
    types.Boolean: BooleanTypeClass,
    types.Enum: EnumTypeClass,
    types._Binary: BytesTypeClass,
    types.LargeBinary: BytesTypeClass,
    types.PickleType: BytesTypeClass,
    types.ARRAY: ArrayTypeClass,
    types.String: StringTypeClass,
    types.Date: DateTypeClass,
    types.DATE: DateTypeClass,
    types.Time: TimeTypeClass,
    types.DateTime: TimeTypeClass,
    types.DATETIME: TimeTypeClass,
    types.TIMESTAMP: TimeTypeClass,
    # When SQLAlchemy is unable to map a type into its internally hierarchy, it
    # assigns the NullType by default. We want to carry this warning through.
    types.NullType: NullTypeClass,
}


def get_column_type(
    sql_report: KuduDBSourceReport, dataset_name: str, column_type: Any
) -> SchemaFieldDataType:

    TypeClass: Optional[Type] = None
    for sql_type in _field_type_mapping.keys():
        if isinstance(column_type, sql_type):
            TypeClass = _field_type_mapping[sql_type]
            break

    if TypeClass is None:
        sql_report.report_warning(
            dataset_name, f"unable to map type {column_type!r} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_metadata(
    sql_report: KuduDBSourceReport,
    dataset_name: str,
    platform: str,
    columns: List[dict],
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []
    for column in columns:
        field = SchemaField(
            fieldPath=column["name"],
            nativeDataType=repr(column["type"]),
            type=get_column_type(sql_report, dataset_name, column["type"]),
            description=column.get("comment", None),
            nullable=column["nullable"],
            recursive=False,
        )
        canonical_schema.append(field)

    actor = "urn:li:corpuser:etl"
    sys_time = int(time.time() * 1000)
    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=SchemalessClass(),
        created=AuditStamp(time=sys_time, actor=actor),
        lastModified=AuditStamp(time=sys_time, actor=actor),
        fields=canonical_schema,
    )
    return schema_metadata


class KuduConfig(ConfigModel):
    # defaults
    scheme: str = "impala"
    database: str = "default"
    ca_cert: str = "/cert/path/is/missing.crt"
    host: str = "localhost:21050"
    use_ssl: bool = True
    authMechanism: Optional[str] = "GSSAPI"
    service_principal: str = "some service principal"
    keytab_location: str = ""
    options: dict = {}
    env: str = DEFAULT_ENV
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    def get_sql_alchemy_url(self):
        if self.use_ssl:
            url = f"{self.scheme}://{self.host}/{self.database}?use_ssl={str(self.use_ssl)}&auth_mechanism={self.authMechanism}&ca_cert={self.ca_cert}"
        else:
            url = f"{self.scheme}://{self.host}/default"
        return url


class KuduSource(Source):
    config: KuduConfig
    report: KuduDBSourceReport

    def __init__(self, config, ctx):
        super().__init__(ctx)
        self.config = config
        self.report = KuduDBSourceReport()
        self.platform = "kudu"

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

        if sql_config.keytab_location is None:
            engine = create_engine(url)
            inspector = inspect(engine)
            for schema in inspector.get_schema_names():
                if not sql_config.schema_pattern.allowed(schema):
                    self.report.report_dropped(schema)
                    logger.error(f"dropped {schema}")
                    continue
                yield from self.loop_tables(inspector, schema, sql_config, engine)
        else:
            with krbContext(
                using_keytab=True,
                principal=sql_config.service_principal,
                keytab_file=sql_config.keytab_location,
            ):
                engine = create_engine(url, **sql_config.options)
                inspector = inspect(engine)
                for schema in inspector.get_schema_names():
                    if not sql_config.schema_pattern.allowed(schema):
                        self.report.report_dropped(schema)
                        continue
                    yield from self.loop_tables(inspector, schema, sql_config, engine)

    def loop_tables(
        self, inspector: Any, schema: str, sql_config: KuduConfig, engine: Any
    ) -> Iterable[MetadataWorkUnit]:
        for table in inspector.get_table_names(schema):

            dataset_name = f"{schema}.{table}"
            if not sql_config.table_pattern.allowed(dataset_name):

                self.report.report_dropped(dataset_name)
                continue
            self.report.report_entity_scanned(dataset_name, ent_type="table")
            # using Impyla to query to HMS, I can't tell if the table is Hive or Kudu unless i query table stats.
            try:
                table_detail_sample = engine.execute(
                    f"show table stats {schema}.{table}"
                ).fetchone()

                columns = [col for col in table_detail_sample.keys()]
                if "Leader Replica" not in columns:
                    self.report.report_dropped(dataset_name)
                    continue  # is Hive not Kudu
            except Exception:
                logger.error(
                    f"unable to parse table stats for {schema}.{table}, will not be ingested"
                )
                continue
            columns = inspector.get_columns(table, schema)

            try:
                table_info: dict = inspector.get_table_comment(table, schema)
            except NotImplementedError:
                description: Optional[str] = None
                properties: Dict[str, str] = {}
            else:
                description = table_info["text"]
                properties = table_info.get("properties", {})

            dataset_snapshot = DatasetSnapshot(
                urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})",
                aspects=[],
            )
            if description is not None or properties:
                dataset_properties = DatasetPropertiesClass(
                    description=description,
                    customProperties=properties,
                )
                dataset_snapshot.aspects.append(dataset_properties)
            schema_metadata = get_schema_metadata(
                self.report, dataset_name, self.platform, columns
            )
            dataset_snapshot.aspects.append(schema_metadata)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = MetadataWorkUnit(id=dataset_name, mce=mce)

            self.report.report_workunit(wu)

            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
