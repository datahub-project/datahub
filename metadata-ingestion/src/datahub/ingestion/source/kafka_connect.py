import logging
import re
import requests
from dataclasses import dataclass, field
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


def get_debezium_lineages(connector_config: Dict, topic_names: Iterable) -> Iterable:
    connector_class = connector_config.get('connector.class')
    database = ''
    lineages = list()
    source_platform = None

    if connector_class in ('io.debezium.connector.mysql.MySqlConnector', 'MySqlConnector'):
        serverName = connector_config.get("database.server.name")
        source_platform = 'mysql'

    if connector_class in ("io.debezium.connector.mongodb.MongoDbConnector"):
        serverName = connector_config.get("mongodb.name")
        source_platform = 'mongodb'

    if connector_class in ('io.debezium.connector.postgresql.PostgresConnector'):
        database = connector_config.get("database.dbname") + "."
        serverName = connector_config.get("database.server.name")
        source_platform = 'postgres'

    if connector_class in ('io.debezium.connector.oracle.OracleConnector'):
        database = connector_config.get("database.dbname") + "."
        serverName = connector_config.get("database.server.name")
        source_platform = 'oracle'

    if connector_class in ('io.debezium.connector.sqlserver.SqlServerConnector'):
        database = connector_config.get("database.dbname") + "."
        serverName = connector_config.get("database.server.name")
        source_platform = 'mssql'

    if connector_class in ('io.debezium.connector.db2.Db2Connector'):
        database = connector_config.get("database.dbname") + "."
        serverName = connector_config.get("database.server.name")
        source_platform = 'db2'

    if connector_class in ('io.debezium.connector.vitess.VitessConnector'):
        serverName = connector_config.get("database.server.name")
        source_platform = 'vitess'

    topic_name_pattern = f"({serverName})\.(\w+\.\w+)"

    for topic in topic_names:
        found = re.search(re.compile(topic_name_pattern), topic)
        if found:
            table = database + re.search(topic_name_pattern, topic).group(2)
            lineages.append({
                "source_dataset": table,
                "source_platform": source_platform,
                "target_dataset": topic,
                "target_platform": 'kafka',
            })

    return lineages


@dataclass
class KafkaConnectSource(Source):
    """The class for Kafka Connect source.

    Attributes:
        config (str): Kafka Connect cluster REST API configurations.
        report (:obj:`int`, optional): .

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

        self.connectors_manifest = list()

        # Test the connection
        test_response = self.session.get(f"{self.config.connect_uri}")
        if test_response.status_code == 200:
            logger.info(f"Connection OK = {self.config.connect_uri}")
            pass
            # TODO(Gabe): how should we message about this error?

    @classmethod
    def create(cls, config_dict, ctx):
        config = KafkaConnectConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_connectors(self):
        """Get Kafka Connect connectors manifest using REST API.

        Enrich with lineages metadata.
        """

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
                topics_response = self.session.get(
                    f"{self.config.connect_uri}/connectors/{c}/topics",
                )

                topics = topics_response.json()
                topic_names = topics[c]["topics"]

                # Currently we only support Debezium Source Connector lineages
                debezium_lineages = get_debezium_lineages(connector_config=connector_config, topic_names=topic_names)
                manifest['lineages'].extend(debezium_lineages)

            if connector_type == 'sink':
                # TODO: Sink Connector not yet implemented
                self.report.report_dropped(name)
                logger.warn(f"Skipping connector {name}. Sink Connector not yet implemented")
                pass

            self.connectors_manifest.append(manifest)

    def construct_flow_mce(self, connector) -> MetadataWorkUnit:
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
        return mce

    def construct_job_mces(self, connector) -> Iterable[MetadataWorkUnit]:
        connector_name = connector.get('name')
        flow_urn = builder.make_data_flow_urn('kafka-connect', connector_name, self.config.env)

        job_mces = list()
        job_property_bag = {}

        lineages = connector.get('lineages', [])

        for lineage in lineages:
            source_dataset = lineage.get('source_dataset')
            source_platform = lineage.get('source_platform')
            target_dataset = lineage.get('target_dataset')
            target_platform = lineage.get('target_platform')

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

            job_mces.append({"id": source_dataset, "mce": mce})

        return job_mces

    def emit_mces(self) -> Iterable[MetadataWorkUnit]:

        mces = list()

        for connector in self.connectors_manifest:
            name = connector.get('name')

            if self.config.connector_patterns.allowed(name):
                flow_mce = self.construct_flow_mce(connector=connector)
                mces.append({"id": name, "mce": flow_mce})

                job_mces = self.construct_job_mces(connector=connector)
                for job_mce in job_mces:
                    mces.append(job_mce)

                self.report.report_connector_scanned(name)

            else:
                self.report.report_dropped(name)

        for mce in mces:
            wu = MetadataWorkUnit(id=mce["id"], mce=mce["mce"])
            self.report.report_workunit(wu)

            yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        self.get_connectors()

        yield from self.emit_mces()

    def get_report(self) -> SourceReport:
        return self.report
