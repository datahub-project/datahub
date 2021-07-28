# This import verifies that the dependencies are available.
import logging
import jaydebeapi
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type

from krbcontext.context import krbContext
from sqlalchemy.sql import sqltypes as types

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemalessClass,
    SchemaMetadata,
    OwnershipClass,
    OwnershipType,
    StringTypeClass,
    TimeTypeClass,
    DatasetProperties,
    Owner
)
from datahub.metadata.schema_classes import DatasetPropertiesClass, OwnerClass

logger: logging.Logger = logging.getLogger(__name__)

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

mapping: Dict[str, Type] = {
    "int":          NumberTypeClass,
    "bigint" :      NumberTypeClass,
    "tinyint":      NumberTypeClass,
    "smallint":     NumberTypeClass,
    "string":       StringTypeClass,
    "boolean":      BooleanTypeClass,
    "float":        NumberTypeClass,
    "decimal":      NumberTypeClass,
    "timestamp":    TimeTypeClass
}


def get_column_type(
    sql_report: KuduDBSourceReport, dataset_name: str, column_type: str
) -> SchemaFieldDataType:

    TypeClass: Optional[Type] = None
    if column_type.lower().startswith("varchar"):
        TypeClass = StringTypeClass
    else:
        TypeClass = mapping.get(column_type, NullTypeClass)
    
    if TypeClass == NullTypeClass:
        sql_report.report_warning(
            dataset_name, f"unable to map type {column_type} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_metadata(
    sql_report: KuduDBSourceReport,
    dataset_name: str,
    platform: str,
    columns: List[Tuple],
) -> SchemaMetadata:
    #item0 is column name, item1 is type, item2 is column comment if any else "", item3 is pri key, item4 is nullable
    canonical_schema: List[SchemaField] = []
    for column in columns:
        field = SchemaField(
            fieldPath=column[0],
            nativeDataType=repr(column[1]),
            type=get_column_type(sql_report, dataset_name, column[1]),
            description=column[2],
            nullable=True if column[4]=="true" else False,
            recursive=False,
        )
        canonical_schema.append(field)

    actor = "urn:li:corpuser:etl"
    sys_time = 0
    priKey=[]
    for column in columns:
        if column[3]=="true":
            priKey.append(column[0])
    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=SchemalessClass(),
        created=AuditStamp(time=sys_time, actor=actor),
        lastModified=AuditStamp(time=sys_time, actor=actor),
        fields=canonical_schema,
        primaryKeys=priKey
    )
    return schema_metadata


class KuduConfig(ConfigModel):
    # defaults
    scheme: str = "impala"
    host: str = "localhost:21050"
    kerberos: bool = False
    truststore_loc: str = "/cert/path/is/missing.jks"
    kerberos_realm: str = ""
    KrbHostFQDN : str = ""    
    #for krbcontext
    service_principal: str = "some service principal"
    keytab_location: str = ""    
    #for jar_file_location
    #If jar_location is "", then im using the dockerized ingest which will have the jar in a fixed loc
    #If not, im probably running kudu from non-docker instance (ie development instance)
    jar_location: str = "/tmp/ImpalaJDBC42-2.6.23.1028.jar" 
    classpath : str = "com.cloudera.impala.impala.core.ImpalaJDBCDriver"
    env: str = DEFAULT_ENV
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    def get_url(self):
        if self.kerberos:
            url = f"""jdbc:{self.scheme}://{self.host};AuthMech=1;KrbServiceName={self.scheme};KrbRealm={self.kerberos_realm};
                    KrbHostFQDN={self.KrbHostFQDN};SSL=1;SSLTrustStore={self.truststore_loc};"""
        else:
            url = f"""jdbc:{self.scheme}://{self.host};"""
        if self.jar_location=="":
            jar = None
        else:
            jar = self.jar_location
        return (url, jar)


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

        (url, jar_loc) = sql_config.get_url()
        classpath = sql_config.classpath
        #if keytab loc is specified, generate ticket using keytab first
        if sql_config.keytab_location == "":            
            db_connection = jaydebeapi.connect(jclassname=classpath, url=url, jars = jar_loc)
            logger.info("db connected!")
            db_cursor = db_connection.cursor()
            db_cursor.execute("show databases;")
            databases_raw = db_cursor.fetchall()
            databases = [item[0] for item in databases_raw]
            for schema in databases:                            
                if not sql_config.schema_pattern.allowed(schema):
                    self.report.report_dropped(schema)
                    logger.error(f"dropped {schema}")
                    continue
                yield from self.loop_tables(db_cursor, schema, sql_config)
            db_connection.close()
            logger.info("db connection closed!")
        else:
            with krbContext(
                using_keytab=True,
                principal=sql_config.service_principal,
                keytab_file=sql_config.keytab_location,
            ):
                db_connection = jaydebeapi.connect(jclassname=classpath, url=url, jars = jar_loc)
                logger.info("db connected!")
                db_cursor = db_connection.cursor()
                db_cursor.execute("show databases;")
                databases_raw = db_cursor.fetchall()
                databases = [item[0] for item in databases_raw]
                for schema in databases:
                    if not sql_config.schema_pattern.allowed(schema):
                        self.report.report_dropped(schema)
                        continue
                    yield from self.loop_tables(db_cursor, schema, sql_config)
                db_connection.close()
                logger.info("db connection closed!")

    def loop_tables(
        self, db_cursor: Any, schema: str, sql_config: KuduConfig 
    ) -> Iterable[MetadataWorkUnit]:
        db_cursor.execute(f"show tables in {schema}")
        all_tables_raw = db_cursor.fetchall()
        all_tables = [item[0] for item in all_tables_raw]
        for table in all_tables:
            dataset_name = f"{schema}.{table}"
            if not sql_config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue
            self.report.report_entity_scanned(dataset_name, ent_type="table")
            #distinguish between hive and kudu table
            try:
                db_cursor.execute(f"show table stats {schema}.{table}")
                result = db_cursor.description
                columns = [col[0] for col in result]
                if "Leader Replica" not in columns:
                    self.report.report_dropped(dataset_name)
                    logger.info(f"{schema}.{table} is dropped cos not kudu")
                    continue  # is Hive not Kudu
            except Exception:
                self.report.report_dropped(dataset_name)
                logger.error(f"unable to parse table stats for {schema}.{table}, will not be ingested")
                continue
            #map out the schema
            db_cursor.execute(f"describe {schema}.{table}")
            schema_info_raw = db_cursor.fetchall()            
            table_schema = [(item[0], item[1], item[2], item[3], item[4]) for item in schema_info_raw]
            
            db_cursor.execute(f"describe formatted {schema}.{table}")
            table_info_raw = db_cursor.fetchall()
            table_info = table_info_raw[len(table_schema)+3:]            

            properties = {}

            for item in table_info:
                if item[0].strip() == "Location:":
                    properties["table_location"] = item[1]                
                if item[0].strip() == "Table Type:":
                    properties["table_type"] = item[1]
                if item[0].strip() == "Owner:":
                    table_owner = item[1]
                if item[1]:
                    if item[1].strip() == "kudu.master_addresses":
                        properties["kudu_master"] = item[2]
            for item in ["table_location","table_type", "kudu_master"]:
                if item not in properties:
                    properties[item]=""
            
            dataset_snapshot = DatasetSnapshot(
                urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})",
                aspects=[],
            )
            data_owner = f"urn:li:corpuser:{table_owner}"
            owner_properties = OwnershipClass(owners=[Owner(owner=data_owner, type = OwnershipType.DATAOWNER)])
            dataset_snapshot.aspects.append(owner_properties)
            #kudu has no table comments. 
            dataset_properties = DatasetProperties(
                description="",
                customProperties=properties,
            )
            dataset_snapshot.aspects.append(dataset_properties)
            
            
            schema_metadata = get_schema_metadata(
                self.report, dataset_name, self.platform, table_schema
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
