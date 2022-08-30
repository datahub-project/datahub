# This import verifies that the dependencies are available.
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Type

import jaydebeapi
import jpype
import pandas as pd
from krbcontext.context import krbContext
from pandas_profiling import ProfileReport

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kudu import KuduDBSourceReport, PPProfilingConfig
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemalessClass,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    StatusClass,
)

logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_ENV = "PROD"
# note to self: the ingestion container needs to have JVM
# constraint, cannot find primary keys from <describe formatted table>.


@dataclass
class CDH_HiveDBSourceReport(KuduDBSourceReport):
    """
    recycle from kudu
    """

    pass


mapping: Dict[str, Type] = {
    "int": NumberTypeClass,
    "bigint": NumberTypeClass,
    "tinyint": NumberTypeClass,
    "smallint": NumberTypeClass,
    "string": StringTypeClass,
    "boolean": BooleanTypeClass,
    "float": NumberTypeClass,
    "decimal": NumberTypeClass,
    "timestamp": TimeTypeClass,
    "binary": BytesTypeClass,
    "array": ArrayTypeClass,
    "struct": StringTypeClass,
    "maps": StringTypeClass,
}


def get_column_type(
    sql_report: CDH_HiveDBSourceReport, dataset_name: str, column_type: str
) -> SchemaFieldDataType:

    TypeClass: Type = NullTypeClass
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
    sql_report: CDH_HiveDBSourceReport,
    dataset_name: str,
    platform: str,
    columns: List[Any],
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []
    for column in columns:
        field = SchemaField(
            fieldPath=column[0],
            nativeDataType=repr(column[1]),
            type=get_column_type(sql_report, dataset_name, column[1]),
            description=column[2],
        )
        canonical_schema.append(field)

    actor = "urn:li:corpuser:etl"
    sys_time = 0
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


class CDHHiveConfig(ConfigModel):
    # defaults
    scheme: str = "hive2"
    host: str = "localhost:10000"
    kerberos: bool = False
    truststore_loc: str = "/opt/cloudera/security/pki/truststore.jks"
    KrbHostFQDN: str = ""
    service_principal: str = "some service principal"
    kerberos_realm: str = ""
    # for krbcontext
    keytab_principal: str = "some principal to get ticket from in keytab"
    keytab_location: str = ""
    jar_location: str = "/tmp/HiveJDBC42-2.6.15.1018.jar"  # absolute path pls!
    conf_file: str = "absolute path to gss-jaas.conf"
    classpath: str = "com.cloudera.hive.jdbc.HS2Driver"
    env: str = DEFAULT_ENV
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    profile_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    profiling: PPProfilingConfig = PPProfilingConfig()
    platform_instance: Optional[str] = None

    def get_url(self):
        if self.kerberos:
            url = f"""jdbc:hive2://{self.host}/default;AuthMech=1;KrbServiceName=hive;KrbRealm={self.kerberos_realm};
                    principal={self.service_principal};KrbAuthType=1;KrbHostFQDN={self.KrbHostFQDN};SSL=1;SSLTrustStore={self.truststore_loc};"""
            return (url, self.jar_location, self.conf_file)
        else:
            url = f"""jdbc:{self.scheme}://{self.host}/default;"""
            return (url, self.jar_location, None)


class CDH_HiveSource(Source):
    config: CDHHiveConfig

    report: CDH_HiveDBSourceReport

    def __init__(self, config, ctx):
        super().__init__(ctx)
        self.config = config
        self.report = CDH_HiveDBSourceReport()
        self.platform = "hive"

    @classmethod
    def create(cls, config_dict, ctx):
        config = CDHHiveConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[SqlWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            )
            wu = SqlWorkUnit(id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp)
            self.report.report_workunit(wu)
            return wu
        else:
            return None

    def gen_database_key(self, database: str) -> DatabaseKey:
        return DatabaseKey(
            database=database,
            platform=self.platform,
            instance=self.config.platform_instance
            if self.config.platform_instance is not None
            else self.config.env,
        )

    def add_table_to_schema_container(
        self, dataset_urn: str, schema: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_database_key(schema)
        container_workunits = add_dataset_to_container(
            container_key=schema_container_key,
            dataset_urn=dataset_urn,
        )
        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_database_containers(self, database: str) -> Iterable[MetadataWorkUnit]:

        database_container_key = self.gen_database_key(database)
        container_workunits = gen_containers(
            container_key=database_container_key,
            name=database,
            sub_types=["DATABASE"],
        )
        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        sql_config = self.config

        (url, jar_loc, conf_file) = sql_config.get_url()
        classpath = sql_config.classpath
        # if keytab loc is specified, generate ticket using keytab first
        args = f"-Djava.class.path={sql_config.jar_location}"
        if not sql_config.kerberos:
            jvm_path = jpype.getDefaultJVMPath()
            if not jpype.isJVMStarted():
                jpype.startJVM(jvm_path, args, convertStrings=True)
            db_connection = jaydebeapi.connect(
                jclassname=classpath, url=url, jars=jar_loc
            )
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
                yield from self.gen_database_containers(schema)
                yield from self.loop_tables(db_cursor, schema, sql_config)
                if sql_config.profiling.enabled:
                    yield from self.loop_profiler(db_cursor, schema, sql_config)
            db_connection.close()
            logger.info("db connection closed!")
        else:
            with krbContext(
                using_keytab=True,
                principal=sql_config.keytab_principal,
                keytab_file=sql_config.keytab_location,
            ):
                jvm_path = jpype.getDefaultJVMPath()
                if not jpype.isJVMStarted():
                    jpype.startJVM(
                        jvm_path,
                        args,
                        f"-Djava.security.auth.login.config={conf_file}",
                        convertStrings=True,
                    )
                db_connection = jaydebeapi.connect(
                    jclassname=classpath, url=url, jars=jar_loc
                )
                logger.info("db connected!")
                db_cursor = db_connection.cursor()
                db_cursor.execute("show databases;")
                databases_raw = db_cursor.fetchall()
                databases = [item[0] for item in databases_raw]
                for schema in databases:
                    if not sql_config.schema_pattern.allowed(schema):
                        self.report.report_dropped(schema)
                        continue
                    yield from self.gen_database_containers(schema)
                    yield from self.loop_tables(db_cursor, schema, sql_config)
                    if sql_config.profiling.enabled:
                        yield from self.loop_profiler(db_cursor, schema, sql_config)
                db_connection.close()
                logger.info("db connection closed!")

    def loop_profiler(
        self, db_cursor: Any, schema: str, sql_config: CDHHiveConfig
    ) -> Iterable[MetadataWorkUnit]:
        db_cursor.execute(f"show tables in {schema}")
        all_tables_raw = db_cursor.fetchall()
        all_tables = [item[0] for item in all_tables_raw]
        if sql_config.profiling.query_date:
            upper_date_limit = (
                datetime.strptime(sql_config.profiling.query_date, "%Y-%m-%d")
                + timedelta(days=1)
            ).strftime("%Y-%m-%d")
        for table in all_tables:
            dataset_name = f"{schema}.{table}"
            self.report.report_entity_scanned(f"profile of {dataset_name}")

            if not sql_config.profile_pattern.allowed(dataset_name):
                self.report.report_dropped(f"profile of {dataset_name}")
                continue
            logger.info(f"Profiling {dataset_name} (this may take a while)")
            if not sql_config.profiling.query_date and sql_config.profiling.limit:
                db_cursor.execute(
                    f"select * from {dataset_name} limit {sql_config.profiling.limit}"  # noqa
                )
            else:  # flake8: noqa
                if sql_config.profiling.query_date and not sql_config.profiling.limit:
                    db_cursor.execute(
                        f"""select * from {dataset_name} where
                        {sql_config.profiling.query_date_field}>='{sql_config.profiling.query_date}'
                        and {sql_config.profiling.query_date_field} < '{upper_date_limit}'"""  # noqa
                    )
                else:
                    db_cursor.execute(
                        f"""select * from {dataset_name} where
                        {sql_config.profiling.query_date_field}>='{sql_config.profiling.query_date}'
                        and {sql_config.profiling.query_date_field} < '{upper_date_limit}'
                        limit {sql_config.profiling.limit}"""  # noqa
                    )
            columns = [desc[0] for desc in db_cursor.description]
            df = pd.DataFrame(db_cursor.fetchall(), columns=columns)
            profile = ProfileReport(
                df,
                minimal=True,
                samples=None,
                correlations=None,
                missing_diagrams=None,
                duplicates=None,
                interactions=None,
            )
            data_samples = self.getDFSamples(df)
            dataset_profile = self.populate_table_profile(profile, data_samples)
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})",
                changeType=ChangeTypeClass.UPSERT,
                aspectName="datasetProfile",
                aspect=dataset_profile,
            )

            wu = MetadataWorkUnit(id=f"profile-{dataset_name}", mcp=mcp)
            self.report.report_workunit(wu)
            logger.debug(f"Finished profiling {dataset_name}")
            yield wu

    def getDFSamples(self, df: pd.DataFrame) -> Dict:
        """
        random sample in pandas profiling came out only in v2.10. however, finding a valid version for py36
        is quite tricky due to other libraries requirements and it kept failing the build tests
        so i decided to just build my own sample. Anyway its just sample rows and assemble into dict
        """
        return df.sample(3 if len(df) > 3 else len(df)).to_dict(orient="records")

    def populate_table_profile(
        self, pandas_profile: ProfileReport, samples: Dict
    ) -> DatasetProfileClass:
        profile_dict = json.loads(pandas_profile.to_json())
        profile_dict["sample"] = samples
        all_fields = []
        for field_variable in profile_dict["variables"].keys():
            field_data = profile_dict["variables"][field_variable]
            field_profile = DatasetFieldProfileClass(
                fieldPath=field_variable,
                uniqueCount=field_data["n_unique"],
                uniqueProportion=field_data["p_unique"],
                nullCount=field_data["n_missing"],
                nullProportion=field_data["p_missing"],
                min=str(field_data.get("min", "")),
                max=str(field_data.get("max", "")),
                median=str(field_data.get("median", "")),
                mean=str(field_data.get("mean", "")),
                sampleValues=[
                    str(item[field_variable]) for item in profile_dict["sample"]
                ],
            )
            all_fields.append(field_profile)

        profile = DatasetProfileClass(
            timestampMillis=int(time.time() * 1000),
            rowCount=profile_dict["table"]["n"],
            columnCount=profile_dict["table"]["n_var"],
            fieldProfiles=all_fields,
        )
        logger.error(profile)
        return profile

    def loop_tables(
        self, db_cursor: Any, schema: str, sql_config: CDHHiveConfig
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
            # distinguish between hive and kudu table
            # map out the schema
            db_cursor.execute(f"describe {schema}.{table}")
            schema_info_raw = db_cursor.fetchall()
            table_schema = [
                (item[0], item[1], item[2])
                for item in schema_info_raw
                if (item[0].strip() != "")
                and (item[0].strip() != "# Partition Information")
                and (item[0].strip() != "# col_name")
            ]

            db_cursor.execute(f"describe formatted {schema}.{table}")
            table_info_raw = db_cursor.fetchall()
            # for partition_ind, item in enumerate(table_info_raw):
            #     if item[0].strip() == "# Partition Information":
            #         mark
            #         break
            # table_info = table_info_raw[partition_ind + 2 :]
            table_info = table_info_raw

            properties = {}

            for item in table_info:
                if item[0].strip() == "Location:":
                    properties["table_location"] = item[1].strip()
                if item[0].strip() == "Table Type:":
                    properties["table_type"] = item[1].strip()
            for item in ["table_location", "table_type"]:
                if item not in properties:
                    properties[item] = ""

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
            # kudu has no table comments.
            dataset_properties = DatasetPropertiesClass(
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
            dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
            if dpi_aspect:
                yield dpi_aspect
            yield from self.add_table_to_schema_container(dataset_urn, schema)
            self.report.report_workunit(wu)

            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
