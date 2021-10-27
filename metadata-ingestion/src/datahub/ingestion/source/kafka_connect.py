import logging
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

import requests
from pydantic import BaseModel
from sqlalchemy.engine.url import make_url

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

logger = logging.getLogger(__name__)


class KafkaConnectSourceConfig(ConfigModel):
    # See the Connect REST Interface for details
    # https://docs.confluent.io/platform/current/connect/references/restapi.html#
    connect_uri: str = "http://localhost:8083/"
    username: Optional[str] = None
    password: Optional[str] = None
    cluster_name: Optional[str] = "connect-cluster"
    env: str = builder.DEFAULT_ENV
    construct_lineage_workunits: bool = True
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
class DebeziumParser:
    source_platform: str
    server_name: Optional[str]
    database_name: Optional[str]


@dataclass
class JdbcParser:
    db_connection_url: Optional[str]


@dataclass
class KafkaConnectLineage:
    """Class to store Kafka Connect lineage mapping"""

    source_dataset: str
    source_platform: str
    target_dataset: str
    target_platform: str


class ConnectorManifest(BaseModel):
    name: str
    config: Dict = {}
    lineages: Optional[List[KafkaConnectLineage]] = []
    topic_names: Optional[Iterable[str]] = []
    type: str
    url: Optional[str]


def get_jdbc_source_connector_parser(
    connector_manifest: ConnectorManifest,
) -> JdbcParser:
    return JdbcParser(connector_manifest.config.get("connection.url"))


def get_debezium_source_connector_parser(
    connector_manifest: ConnectorManifest,
) -> DebeziumParser:
    connector_class = connector_manifest.config.get("connector.class", "")
    if connector_class == "io.debezium.connector.mysql.MySqlConnector":
        # https://debezium.io/documentation/reference/connectors/mysql.html#mysql-topic-names
        parser = DebeziumParser(
            source_platform="mysql",
            server_name=connector_manifest.config.get("database.server.name"),
            database_name=None,
        )
    elif connector_class == "MySqlConnector":
        parser = DebeziumParser(
            source_platform="mysql",
            server_name=connector_manifest.config.get("database.server.name"),
            database_name=None,
        )
    elif connector_class == "io.debezium.connector.mongodb.MongoDbConnector":
        # https://debezium.io/documentation/reference/connectors/mongodb.html#mongodb-topic-names
        parser = DebeziumParser(
            source_platform="mongodb",
            server_name=connector_manifest.config.get("database.server.name"),
            database_name=None,
        )
    elif connector_class == "io.debezium.connector.postgresql.PostgresConnector":
        # https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-topic-names
        parser = DebeziumParser(
            source_platform="postgres",
            server_name=connector_manifest.config.get("database.server.name"),
            database_name=connector_manifest.config.get("database.dbname"),
        )
    elif connector_class == "io.debezium.connector.oracle.OracleConnector":
        # https://debezium.io/documentation/reference/connectors/oracle.html#oracle-topic-names
        parser = DebeziumParser(
            source_platform="oracle",
            server_name=connector_manifest.config.get("database.server.name"),
            database_name=connector_manifest.config.get("database.dbname"),
        )
    elif connector_class == "io.debezium.connector.sqlserver.SqlServerConnector":
        # https://debezium.io/documentation/reference/connectors/sqlserver.html#sqlserver-topic-names
        parser = DebeziumParser(
            source_platform="mssql",
            server_name=connector_manifest.config.get("database.server.name"),
            database_name=connector_manifest.config.get("database.dbname"),
        )
    elif connector_class == "io.debezium.connector.db2.Db2Connector":
        # https://debezium.io/documentation/reference/connectors/db2.html#db2-topic-names
        parser = DebeziumParser(
            source_platform="db2",
            server_name=connector_manifest.config.get("database.server.name"),
            database_name=connector_manifest.config.get("database.dbname"),
        )
    elif connector_class == "io.debezium.connector.vitess.VitessConnector":
        # https://debezium.io/documentation/reference/connectors/vitess.html#vitess-topic-names
        parser = DebeziumParser(
            source_platform="vitess",
            server_name=connector_manifest.config.get("database.server.name"),
            database_name=connector_manifest.config.get("vitess.keyspace"),
        )
    else:
        raise ValueError(f"Connector class '{connector_class}' is unknown.")

    return parser


def remove_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        index = len(prefix)
        return text[index:]
    return text


@dataclass
class JDBCSourceConnectorLineages:
    connector_manifest: ConnectorManifest

    def get_lineages(self):
        lineages: List[KafkaConnectLineage] = list()
        parser = get_jdbc_source_connector_parser(self.connector_manifest)
        db_connection_url = parser.db_connection_url
        url = remove_prefix(str(db_connection_url), "jdbc:")
        url_instance = make_url(url)
        source_platform = url_instance.drivername
        database_name = url_instance.database

        logging.debug(
            f"Extracting source platform: {source_platform} and database name: {database_name} from connection url "
        )

        topic_prefix = (
            str(self.connector_manifest.config.get("topic.prefix"))
            if self.connector_manifest.config.get("topic.prefix")
            else None
        )

        query = (
            self.connector_manifest.config.get("query")
            if self.connector_manifest.config.get("query")
            else None
        )

        if not self.connector_manifest.topic_names:
            return lineages

        for topic in self.connector_manifest.topic_names:
            # if the connector uses a custom query
            if topic_prefix and not query:
                source_table = remove_prefix(topic, topic_prefix)
            else:
                source_table = topic

            dataset_name = (
                database_name + "." + source_table if database_name else source_table
            )

            lineage = KafkaConnectLineage(
                source_dataset=dataset_name,
                source_platform=source_platform,
                target_dataset=topic,
                target_platform="kafka",
            )
            lineages.append(lineage)

        return lineages


@dataclass
class DebeziumSourceConnectorLineages:
    connector_manifest: ConnectorManifest

    def get_lineages(self) -> List[KafkaConnectLineage]:
        lineages: List[KafkaConnectLineage] = list()
        parser = get_debezium_source_connector_parser(self.connector_manifest)
        source_platform = parser.source_platform
        server_name = parser.server_name
        database_name = parser.database_name
        topic_naming_pattern = r"({0})\.(\w+\.\w+)".format(server_name)

        if not self.connector_manifest.topic_names:
            return lineages

        for topic in self.connector_manifest.topic_names:
            found = re.search(re.compile(topic_naming_pattern), topic)

            if found:
                table_name = (
                    database_name + "." + found.group(2)
                    if database_name
                    else found.group(2)
                )

                lineage = KafkaConnectLineage(
                    source_dataset=table_name,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)

        return lineages


class KafkaConnectSource(Source):
    """The class for Kafka Connect source.

    Attributes:
        config (KafkaConnectSourceConfig): Kafka Connect cluster REST API configurations.
        report (KafkaConnectSourceReport): Kafka Connect source ingestion report.

    """

    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport

    def __init__(self, config: KafkaConnectSourceConfig, ctx: PipelineContext):
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
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = KafkaConnectSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_connectors_manifest(self) -> List[ConnectorManifest]:
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
            connector_manifest = ConnectorManifest(**manifest)
            # Initialize connector lineages
            connector_manifest.lineages = list()
            connector_manifest.url = connector_url

            # Populate Source Connector metadata
            if connector_manifest.type == "source":
                topics_response = self.session.get(
                    f"{self.config.connect_uri}/connectors/{c}/topics",
                )

                topics = topics_response.json()
                connector_manifest.topic_names = topics[c]["topics"]

                # JDBC source connector lineages
                if connector_manifest.config.get("connector.class").__eq__(
                    "io.confluent.connect.jdbc.JdbcSourceConnector"
                ):
                    jdbc_source_lineages = JDBCSourceConnectorLineages(
                        connector_manifest=connector_manifest
                    )
                    connector_manifest.lineages.extend(
                        jdbc_source_lineages.get_lineages()
                    )
                else:
                    # Debezium Source Connector lineages
                    try:
                        debezium_source_lineages = DebeziumSourceConnectorLineages(
                            connector_manifest=connector_manifest
                        )
                        connector_manifest.lineages.extend(
                            debezium_source_lineages.get_lineages()
                        )
                    except ValueError as err:
                        logger.warning(
                            f"Skipping connector {connector_manifest.name} due to error: {err}"
                        )
                        self.report.report_failure(connector_manifest.name, str(err))
                        continue

            if connector_manifest.type == "sink":
                # TODO: Sink Connector not yet implemented
                self.report.report_dropped(connector_manifest.name)
                logger.warning(
                    f"Skipping connector {connector_manifest.name}. Sink Connector not yet implemented"
                )
                pass

            connectors_manifest.append(connector_manifest)

        return connectors_manifest

    def construct_flow_workunit(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:
        connector_name = connector.name
        connector_type = connector.type
        connector_class = connector.config.get("connector.class")
        # connector_url = connector.url  # NOTE: this will expose connector credential when used
        flow_urn = builder.make_data_flow_urn(
            "kafka-connect", connector_name, self.config.env
        )
        flow_property_bag: Optional[Dict[str, str]] = None
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

        for c in [connector_name]:
            wu = MetadataWorkUnit(id=c, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def construct_job_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:

        connector_name = connector.name
        flow_urn = builder.make_data_flow_urn(
            "kafka-connect", connector_name, self.config.env
        )

        job_property_bag: Optional[Dict[str, str]] = None

        lineages = connector.lineages
        if lineages:
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

    def construct_lineage_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:

        lineages = connector.lineages
        if lineages:
            for lineage in lineages:
                source_dataset = lineage.source_dataset
                source_platform = lineage.source_platform
                target_dataset = lineage.target_dataset
                target_platform = lineage.target_platform

                mce = models.MetadataChangeEventClass(
                    proposedSnapshot=models.DatasetSnapshotClass(
                        urn=builder.make_dataset_urn(
                            target_platform, target_dataset, self.config.env
                        ),
                        aspects=[
                            models.UpstreamLineageClass(
                                upstreams=[
                                    models.UpstreamClass(
                                        dataset=builder.make_dataset_urn(
                                            source_platform,
                                            source_dataset,
                                            self.config.env,
                                        ),
                                        type=models.DatasetLineageTypeClass.TRANSFORMED,
                                    )
                                ]
                            )
                        ],
                    )
                )

                wu = MetadataWorkUnit(id=source_dataset, mce=mce)
                self.report.report_workunit(wu)
                yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        connectors_manifest = self.get_connectors_manifest()
        for connector in connectors_manifest:
            name = connector.name
            if self.config.connector_patterns.allowed(name):
                yield from self.construct_flow_workunit(connector)
                yield from self.construct_job_workunits(connector)
                if self.config.construct_lineage_workunits:
                    yield from self.construct_lineage_workunits(connector)

                self.report.report_connector_scanned(name)

            else:
                self.report.report_dropped(name)

    def get_report(self) -> KafkaConnectSourceReport:
        return self.report
