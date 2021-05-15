import logging
import re
import requests
from dataclasses import dataclass, field
from pydantic import BaseModel, Extra
from typing import Dict, Iterable, List, Optional

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit


logger = logging.getLogger(__name__)


class KafkaConnectConfig(ConfigModel):
    # See the Connect REST Interface for details
    # https://docs.confluent.io/platform/current/connect/references/restapi.html#
    connect_uri: str = "http://localhost:8083/"
    username: Optional[str] = None
    password: Optional[str] = None
    cluster_name: Optional[str] = "connect-cluster"
    env: str = "PROD"
    connector_patterns: AllowDenyPattern = AllowDenyPattern(allow=[".*"], deny=["^_.*"])


@dataclass
class KafkaConnectSourceReport(SourceReport):
    connectors_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_connector_scanned(self, connector: str) -> None:
        self.connectors_scanned += 1

    def report_dropped(self, connector: str) -> None:
        self.filtered.append(connector)

@dataclass
class KafkaConnectLineage:
    """Class to store Kafka Connect lineage mapping"""
    source_dataset: str
    source_platform: str
    target_dataset: str
    target_platform: str

class DebeziumConfig(BaseModel):
    config: dict

    


@dataclass
class DebeziumLineages:
    connector_config: Dict
    topic_names: Iterable[str]

    def __init__(self, connector_config: Dict, topic_names: Iterable[str]) -> None:
        self.connector_config = DebeziumConfig(connector_config)
        self.topic_names = topic_names
        self.lineages: List[str] = field(default_factory=list)
        self.source_platform: str = None
        self.parser = {
            "io.debezium.connector.mysql.MySqlConnector": {
                "source_platform": "mysql",
            },
            "MySqlConnector": {
                "source_platform": "mysql",
            },
            "io.debezium.connector.mongodb.MongoDbConnector": {
                "source_platform": "mongodb",
            },
            "io.debezium.connector.postgresql.PostgresConnector": {
                "source_platform": "postgres",
            },
            "io.debezium.connector.oracle.OracleConnector": {
                "source_platform": "oracle",
            },
            "io.debezium.connector.sqlserver.SqlServerConnector": {
                "source_platform": "mssql",
            },
            "io.debezium.connector.db2.Db2Connector": {
                "source_platform": "db2",
            },
            "io.debezium.connector.vitess.VitessConnector": {
                "source_platform": "vitess",
        },
        }
        self.connector_class= self.connector_config.get('connector.class')

    # def get_

    def get_lineages(self):
        lineages = list()
        parser = self.parser.get(self.connector_class)
        # print(parser)
        for topic in self.topic_names:
            found = re.search(re.compile(topic_name_pattern), topic)
            if found:
                table = database + re.search(topic_name_pattern, topic).group(2)
                lineage = KafkaConnectLineage(
                    source_dataset=table,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform='kafka',
                )
                lineages.append(lineage)

        return lineages

"""
def get_debezium_lineages(connector_config: Dict, topic_names: Iterable) -> Iterable[KafkaConnectLineage]:
    connector_class = connector_config.get('connector.class')
    database = ''
    lineages = list()
    source_platform = None

    if connector_class in ('io.debezium.connector.mysql.MySqlConnector', 'MySqlConnector'):
        # https://debezium.io/documentation/reference/connectors/mysql.html#mysql-topic-names
        # Kafka topic naming convention: serverName.databaseName.tableName
        source_platform = 'mysql'
        serverName = connector_config.get("database.server.name")

    if connector_class in ("io.debezium.connector.mongodb.MongoDbConnector"):
        # https://debezium.io/documentation/reference/connectors/mongodb.html#mongodb-topic-names
        # Kafka topic naming convention: logicalName.databaseName.collectionName
        source_platform = 'mongodb'
        serverName = connector_config.get("mongodb.name")

    if connector_class in ('io.debezium.connector.postgresql.PostgresConnector'):
        # https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-topic-names
        # Kafka topic naming convention: serverName.schemaName.tableName
        source_platform = 'postgres'
        database = connector_config.get("database.dbname") + "."
        serverName = connector_config.get("database.server.name")

    if connector_class in ('io.debezium.connector.oracle.OracleConnector'):
        # https://debezium.io/documentation/reference/connectors/oracle.html#oracle-topic-names
        # Kafka topic naming convention: <database.server.name>.schemaName.tableName
        source_platform = 'oracle'
        database = connector_config.get("database.dbname") + "."
        serverName = connector_config.get("database.server.name")

    if connector_class in ('io.debezium.connector.sqlserver.SqlServerConnector'):
        # https://debezium.io/documentation/reference/connectors/sqlserver.html#sqlserver-topic-names
        # Kafka topic naming convention: serverName.schemaName.tableName
        # serverName=database.server.name
        source_platform = 'mssql'
        database = connector_config.get("database.dbname") + "."
        serverName = connector_config.get("database.server.name")

    if connector_class in ('io.debezium.connector.db2.Db2Connector'):
        # 
        # Kafka topic naming convention: 
        source_platform = 'db2'
        database = connector_config.get("database.dbname") + "."
        serverName = connector_config.get("database.server.name")

    if connector_class in ('io.debezium.connector.vitess.VitessConnector'):
        # 
        # Kafka topic naming convention: 
        source_platform = 'vitess'
        serverName = connector_config.get("database.server.name")

    topic_name_pattern = f"({serverName})\.(\w+\.\w+)"

    for topic in topic_names:
        found = re.search(re.compile(topic_name_pattern), topic)
        if found:
            table = database + re.search(topic_name_pattern, topic).group(2)
            lineage = KafkaConnectLineage(
                source_dataset=table,
                source_platform=source_platform,
                target_dataset=topic,
                target_platform='kafka',
            )
            lineages.append(lineage)

    return lineages
"""

@dataclass
class KafkaConnectSource(Source):
    """The class for Kafka Connect source.

    Attributes:
        config (KafkaConnectConfig): Kafka Connect cluster REST API configurations.
        report (KafkaConnectSourceReport): Kafka Connect source ingestion report.

    """
    config: KafkaConnectConfig
    report: KafkaConnectSourceReport

    def __init__(self, config: KafkaConnectConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = KafkaConnectSourceReport()

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Test the connection
        test_response = self.session.get(f"{self.config.connect_uri}")
        test_response.raise_for_status()
        logger.info(f"Connection to {self.config.connect_uri} is ok")

    @classmethod
    def create(cls, config_dict, ctx):
        config = KafkaConnectConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_connectors_manifest(self):
        """Get Kafka Connect connectors manifest using REST API.

        Enrich with lineages metadata.
        """
        connectors_manifest = list()

        connector_response = self.session.get(
            f"{self.config.connect_uri}/connectors",
        )

        payload = connector_response.json()

        for c in payload:
            connector_url = f"{self.config.connect_uri}/connectors/{c}"
            connector_response = self.session.get(connector_url)

            manifest = connector_response.json()
            name = manifest.get("name")

            # Initialize connector lineages
            manifest["lineages"] = list()
            manifest["url"] = connector_url

            connector_type = manifest['type']

            # Populate Source Connector metadata
            if connector_type == 'source':
                connector_config = manifest.get('config', {})
                print(connector_config)
                topics_response = self.session.get(
                    f"{self.config.connect_uri}/connectors/{c}/topics",
                )

                topics = topics_response.json()
                topic_names = topics[c]["topics"]

                # Currently we only support Debezium Source Connector lineages
                # debezium_lineages = get_debezium_lineages(connector_config=connector_config, topic_names=topic_names)
                # manifest['lineages'].extend(debezium_lineages)

                debezium_lineages = DebeziumLineages(connector_config=connector_config, topic_names=topic_names)
                manifest['lineages'].extend(debezium_lineages.get_lineages())

            if connector_type == 'sink':
                # TODO: Sink Connector not yet implemented
                self.report.report_dropped(name)
                logger.warn(f"Skipping connector {name}. Sink Connector not yet implemented")
                pass

            connectors_manifest.append(manifest)

        return connectors_manifest

    def construct_flow_workunit(self, connector) -> MetadataWorkUnit:
        connector_name = connector.get('name')
        connector_type = connector.get('type', "")
        connector_class = connector.get('config', {}).get('connector.class')
        connector_url = connector.get('url')
        flow_urn = builder.make_data_flow_urn('kafka-connect', connector_name, self.config.env)
        flow_property_bag = {}
        mce = models.MetadataChangeEventClass(
            proposedSnapshot=models.DataFlowSnapshotClass(
                urn=flow_urn,
                aspects=[
                    models.DataFlowInfoClass(
                        name=connector_name,
                        description=f"{connector_type.capitalize()} connector using `{connector_class}` plugin.",
                        customProperties=flow_property_bag,
                        # externalUrl=connector_url, # NOTE: this will expose connector credential when used
                    ),
                    # ownership,
                    # tags,
                ],
            )
        )

        wu = MetadataWorkUnit(id=connector_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def construct_job_workunits(self, connector) -> Iterable[MetadataWorkUnit]:
        connector_name = connector.get('name')
        flow_urn = builder.make_data_flow_urn('kafka-connect', connector_name, self.config.env)

        job_property_bag = {}

        lineages = connector.get('lineages', [])

        for lineage in lineages:
            source_dataset = lineage.source_dataset
            source_platform = lineage.source_platform
            target_dataset = lineage.target_dataset
            target_platform = lineage.target_platform

            job_urn = builder.make_data_job_urn_with_flow(flow_urn, source_dataset)

            inlets = [builder.make_dataset_urn(source_platform, source_dataset)]
            outlets = [builder.make_dataset_urn(target_platform, target_dataset)]

            mce = models.MetadataChangeEventClass(
                proposedSnapshot=models.DataJobSnapshotClass(
                    urn=job_urn,
                    aspects=[
                        models.DataJobInfoClass(
                            name=f"{connector_name}:{source_dataset}",
                            type="COMMAND",
                            description=None,
                            customProperties=job_property_bag,
                            # externalUrl=job_url,
                        ),
                        models.DataJobInputOutputClass(
                            inputDatasets=inlets or [],
                            outputDatasets=outlets or [],
                        ),
                        # ownership,
                        # tags,
                    ],
                )
            )

            wu = MetadataWorkUnit(id=source_dataset, mce=mce)
            self.report.report_workunit(wu)
            yield wu


    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        connectors_manifest = self.get_connectors_manifest()

        for connector in connectors_manifest:
            name = connector.get('name')
            print(connector.get('config'))
            if self.config.connector_patterns.allowed(name):
                yield from self.construct_flow_workunit(connector)
                yield from self.construct_job_workunits(connector)

                self.report.report_connector_scanned(name)

            else:
                self.report.report_dropped(name)

    def get_report(self) -> KafkaConnectSourceReport:
        return self.report
