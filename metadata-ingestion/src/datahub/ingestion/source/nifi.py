import json
import logging
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from dateutil import parser
from packaging import version
from pydantic.fields import Field
from requests.adapters import HTTPAdapter

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
)

logger = logging.getLogger(__name__)
NIFI = "nifi"


# Python requests does not support passing password for key file,
# The same can be achieved by mounting ssl context
# as described here - https://github.com/psf/requests/issues/2519
# and here - https://github.com/psf/requests/issues/1573
class SSLAdapter(HTTPAdapter):
    def __init__(self, certfile, keyfile, password=None):
        self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.context.load_cert_chain(
            certfile=certfile, keyfile=keyfile, password=password
        )
        super().__init__()

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self.context
        return super().init_poolmanager(*args, **kwargs)


class NifiAuthType(Enum):
    NO_AUTH = "NO_AUTH"
    SINGLE_USER = "SINGLE_USER"
    CLIENT_CERT = "CLIENT_CERT"


class NifiSourceConfig(EnvBasedSourceConfigBase):
    site_url: str = Field(description="URI to connect")

    auth: NifiAuthType = Field(
        default=NifiAuthType.NO_AUTH,
        description="Nifi authentication. must be one of : NO_AUTH, SINGLE_USER, CLIENT_CERT",
    )

    provenance_days: int = Field(
        default=7,
        description="time window to analyze provenance events for external datasets",
    )  # Fetch provenance events for past 1 week
    process_group_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for filtering process groups",
    )

    # Required for nifi deployments using Remote Process Groups
    site_name: str = Field(
        default="default",
        description="Site name to identify this site with, useful when using input and output ports receiving remote connections",
    )
    site_url_to_site_name: Dict[str, str] = Field(
        default={},
        description="Lookup to find site_name for site_url, required if using remote process groups in nifi flow",
    )

    # Required to be set if auth is of type SINGLE_USER
    username: Optional[str] = Field(
        default=None, description='Nifi username, must be set for auth = "SINGLE_USER"'
    )
    password: Optional[str] = Field(
        default=None, description='Nifi password, must be set for auth = "SINGLE_USER"'
    )

    # Required to be set if auth is of type CLIENT_CERT
    client_cert_file: Optional[str] = Field(
        default=None,
        description='Path to PEM file containing the public certificates for the user/client identity, must be set for auth = "CLIENT_CERT"',
    )
    client_key_file: Optional[str] = Field(
        default=None, description="Path to PEM file containing the client’s secret key"
    )
    client_key_password: Optional[str] = Field(
        default=None, description="The password to decrypt the client_key_file"
    )

    # Required to be set if nifi server certificate is not signed by
    # root CA trusted by client system, e.g. self-signed certificates
    ca_file: Optional[str] = Field(
        default=None,
        description="Path to PEM file containing certs for the root CA(s) for the NiFi",
    )


TOKEN_ENDPOINT = "/nifi-api/access/token"
ABOUT_ENDPOINT = "/nifi-api/flow/about"
CLUSTER_ENDPOINT = "/nifi-api/flow/cluster/summary"
PG_ENDPOINT = "/nifi-api/flow/process-groups/"
PROVENANCE_ENDPOINT = "/nifi-api/provenance/"


class NifiType(Enum):
    PROCESSOR = "PROCESSOR"
    FUNNEL = "FUNNEL"
    INPUT_PORT = "INPUT_PORT"
    OUTPUT_PORT = "OUTPUT_PORT"
    REMOTE_INPUT_PORT = "REMOTE_INPUT_PORT"
    REMOTE_OUTPUT_PORT = "REMOTE_OUTPUT_PORT"


class NifiEventType:
    CREATE = "CREATE"
    FETCH = "FETCH"
    SEND = "SEND"
    RECEIVE = "RECEIVE"


class NifiProcessorType:
    ListS3 = "org.apache.nifi.processors.aws.s3.ListS3"
    FetchS3Object = "org.apache.nifi.processors.aws.s3.FetchS3Object"
    PutS3Object = "org.apache.nifi.processors.aws.s3.PutS3Object"
    ListSFTP = "org.apache.nifi.processors.standard.ListSFTP"
    FetchSFTP = "org.apache.nifi.processors.standard.FetchSFTP"
    GetSFTP = "org.apache.nifi.processors.standard.GetSFTP"
    PutSFTP = "org.apache.nifi.processors.standard.PutSFTP"


# To support new processor type,
# 1. add an entry in KNOWN_INGRESS_EGRESS_PROCESORS
# 2. Implement provenance event analyzer to find external dataset and
# map it in provenance_event_to_lineage_map
class NifiProcessorProvenanceEventAnalyzer:
    env: str

    KNOWN_INGRESS_EGRESS_PROCESORS = {
        NifiProcessorType.ListS3: NifiEventType.CREATE,
        NifiProcessorType.FetchS3Object: NifiEventType.FETCH,
        NifiProcessorType.PutS3Object: NifiEventType.SEND,
        NifiProcessorType.ListSFTP: NifiEventType.CREATE,
        NifiProcessorType.FetchSFTP: NifiEventType.FETCH,
        NifiProcessorType.GetSFTP: NifiEventType.RECEIVE,
        NifiProcessorType.PutSFTP: NifiEventType.SEND,
    }

    def __init__(self) -> None:
        # Map of Nifi processor type to the provenance event analyzer to find lineage
        self.provenance_event_to_lineage_map: Dict[
            str, Callable[[Dict], ExternalDataset]
        ] = {
            NifiProcessorType.ListS3: self.process_s3_provenance_event,
            NifiProcessorType.FetchS3Object: self.process_s3_provenance_event,
            NifiProcessorType.PutS3Object: self.process_s3_provenance_event,
            NifiProcessorType.ListSFTP: self.process_sftp_provenance_event,
            NifiProcessorType.FetchSFTP: self.process_sftp_provenance_event,
            NifiProcessorType.GetSFTP: self.process_sftp_provenance_event,
            NifiProcessorType.PutSFTP: self.process_sftp_provenance_event,
        }

    def process_s3_provenance_event(self, event):
        attributes = event.get("attributes", [])
        s3_bucket = get_attribute_value(attributes, "s3.bucket")
        s3_key = get_attribute_value(attributes, "s3.key")
        if not s3_key:
            s3_key = get_attribute_value(attributes, "filename")

        s3_url = f"s3://{s3_bucket}/{s3_key}"
        s3_url = s3_url[: s3_url.rindex("/")]
        dataset_name = s3_url.replace("s3://", "").replace("/", ".")
        platform = "s3"
        dataset_urn = builder.make_dataset_urn(platform, dataset_name, self.env)
        return ExternalDataset(
            platform,
            dataset_name,
            dict(s3_uri=s3_url),
            dataset_urn,
        )

    def process_sftp_provenance_event(self, event):
        attributes = event.get("attributes", [])
        remote_host = get_attribute_value(attributes, "sftp.remote.host")
        path = get_attribute_value(attributes, "path")
        filename = get_attribute_value(attributes, "filename")
        absolute_path = f"sftp://{remote_host}/{path}/{filename}"
        if remote_host is None or path is None or filename is None:
            absolute_path = event.get("transitUri")

        absolute_path = absolute_path.replace("/./", "/")
        if absolute_path.endswith("/."):
            absolute_path = absolute_path[:-2]
        absolute_path = absolute_path[: absolute_path.rindex("/")]
        dataset_name = absolute_path.replace("sftp://", "").replace("/", ".")
        platform = "file"
        dataset_urn = builder.make_dataset_urn(platform, dataset_name, self.env)
        return ExternalDataset(
            platform,
            dataset_name,
            dict(uri=absolute_path),
            dataset_urn,
        )


@dataclass
class ExternalDataset:
    platform: str
    dataset_name: str
    dataset_properties: Dict[str, str]
    dataset_urn: str


@dataclass
class NifiComponent:
    id: str
    name: str
    type: str
    parent_group_id: str
    nifi_type: NifiType
    comments: Optional[str] = None
    status: Optional[str] = None

    # present only for nifi remote ports and processors
    inlets: Dict[str, ExternalDataset] = field(default_factory=dict)
    outlets: Dict[str, ExternalDataset] = field(default_factory=dict)

    # present only for processors
    config: Optional[Dict] = None

    # present only for nifi remote ports
    target_uris: Optional[str] = None
    parent_rpg_id: Optional[str] = None

    # Last successful event time
    last_event_time: Optional[str] = None


@dataclass
class NifiProcessGroup:
    id: str
    name: str
    parent_group_id: Optional[str]


@dataclass
class NifiRemoteProcessGroup:
    id: str
    name: str
    parent_group_id: str
    remote_ports: Dict[str, NifiComponent]


@dataclass
class NifiFlow:
    version: Optional[str]
    clustered: Optional[bool]
    root_process_group: NifiProcessGroup
    components: Dict[str, NifiComponent] = field(default_factory=dict)
    remotely_accessible_ports: Dict[str, NifiComponent] = field(default_factory=dict)
    connections: List[Tuple[str, str]] = field(default_factory=list)
    processGroups: Dict[str, NifiProcessGroup] = field(default_factory=dict)
    remoteProcessGroups: Dict[str, NifiRemoteProcessGroup] = field(default_factory=dict)
    remote_ports: Dict[str, NifiComponent] = field(default_factory=dict)


def get_attribute_value(attr_lst: List[dict], attr_name: str) -> Optional[str]:
    match = [entry for entry in attr_lst if entry["name"] == attr_name]
    if len(match) > 0:
        return match[0]["value"]
    return None


@dataclass
class NifiSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)


# allowRemoteAccess
@platform_name("NiFi", id="nifi")
@config_class(NifiSourceConfig)
@support_status(SupportStatus.CERTIFIED)
class NifiSource(Source):
    """
    This plugin extracts the following:

    - NiFi flow as `DataFlow` entity
    - Ingress, egress processors, remote input and output ports as `DataJob` entity
    - Input and output ports receiving remote connections as `Dataset` entity
    - Lineage information between external datasets and ingress/egress processors by analyzing provenance events

    Current limitations:

    - Limited ingress/egress processors are supported
      - S3: `ListS3`, `FetchS3Object`, `PutS3Object`
      - SFTP: `ListSFTP`, `FetchSFTP`, `GetSFTP`, `PutSFTP`

    """

    config: NifiSourceConfig
    report: NifiSourceReport

    def __init__(self, config: NifiSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config
        self.report = NifiSourceReport()
        self.session = requests.Session()

        if self.config.ca_file is not None:
            self.session.verify = self.config.ca_file

        if self.config.site_url_to_site_name is None:
            self.config.site_url_to_site_name = {}
        if (
            urljoin(self.config.site_url, "/nifi/")
            not in self.config.site_url_to_site_name
        ):
            self.config.site_url_to_site_name[
                urljoin(self.config.site_url, "/nifi/")
            ] = self.config.site_name

        if self.config.auth is NifiAuthType.CLIENT_CERT:
            logger.debug("Setting client certificates in requests ssl context")
            assert (
                self.config.client_cert_file is not None
            ), "Config client_cert_file is required for CLIENT_CERT auth"
            self.session.mount(
                urljoin(self.config.site_url, "/nifi-api/"),
                SSLAdapter(
                    certfile=self.config.client_cert_file,
                    keyfile=self.config.client_key_file,
                    password=self.config.client_key_password,
                ),
            )
        if self.config.auth is NifiAuthType.SINGLE_USER:
            assert (
                self.config.username is not None
            ), "Config username is required for SINGLE_USER auth"
            assert (
                self.config.password is not None
            ), "Config password is required for SINGLE_USER auth"
            token_response = self.session.post(
                url=urljoin(self.config.site_url, TOKEN_ENDPOINT),
                data={
                    "username": self.config.username,
                    "password": self.config.password,
                },
            )
            if not token_response.ok:
                logger.error("Failed to get token")
                self.report.report_failure(self.config.site_url, "Failed to get token")

            self.session.headers.update(
                {
                    "Authorization": "Bearer " + token_response.text,
                    # "Accept": "application/json",
                    "Content-Type": "application/json",
                }
            )
        else:
            self.session.headers.update(
                {
                    # "Accept": "application/json",
                    "Content-Type": "application/json",
                }
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = NifiSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SourceReport:
        return self.report

    def update_flow(self, pg_flow_dto: Dict) -> None:  # noqa: C901
        breadcrumb_dto = pg_flow_dto.get("breadcrumb", {}).get("breadcrumb", {})
        nifi_pg = NifiProcessGroup(
            breadcrumb_dto.get("id"),
            breadcrumb_dto.get("name"),
            pg_flow_dto.get("parentGroupId"),
        )
        self.nifi_flow.processGroups[nifi_pg.id] = nifi_pg
        if not self.config.process_group_pattern.allowed(nifi_pg.name):
            self.report.report_dropped(f"{nifi_pg.name}.*")
            return

        flow_dto = pg_flow_dto.get("flow", {})

        for processor in flow_dto.get("processors", []):
            component = processor.get("component")
            self.nifi_flow.components[component.get("id")] = NifiComponent(
                component.get("id"),
                component.get("name"),
                component.get("type"),
                component.get("parentGroupId"),
                NifiType.PROCESSOR,
                config=component.get("config"),
                comments=component.get("config", {}).get("comments"),
                status=component.get("status", {}).get("runStatus"),
            )
        for funnel in flow_dto.get("funnels", []):
            component = funnel.get("component")
            self.nifi_flow.components[component.get("id")] = NifiComponent(
                component.get("id"),
                component.get("name"),
                component.get("type"),
                component.get("parentGroupId"),
                NifiType.FUNNEL,
                comments=component.get("comments"),
                status=component.get("status", {}).get("runStatus"),
            )
            logger.debug(f"Adding funnel {component.get('id')}")

        for connection in flow_dto.get("connections", []):
            # Exclude self - recursive relationships
            if connection.get("sourceId") != connection.get("destinationId"):
                self.nifi_flow.connections.append(
                    (connection.get("sourceId"), connection.get("destinationId"))
                )

        for inputPort in flow_dto.get("inputPorts", []):
            component = inputPort.get("component")
            if inputPort.get("allowRemoteAccess"):
                self.nifi_flow.remotely_accessible_ports[
                    component.get("id")
                ] = NifiComponent(
                    component.get("id"),
                    component.get("name"),
                    component.get("type"),
                    component.get("parentGroupId"),
                    NifiType.INPUT_PORT,
                    comments=component.get("comments"),
                    status=component.get("status", {}).get("runStatus"),
                )
                logger.debug(f"Adding remotely accessible port {component.get('id')}")
            else:
                self.nifi_flow.components[component.get("id")] = NifiComponent(
                    component.get("id"),
                    component.get("name"),
                    component.get("type"),
                    component.get("parentGroupId"),
                    NifiType.INPUT_PORT,
                    comments=component.get("comments"),
                    status=component.get("status", {}).get("runStatus"),
                )
                logger.debug(f"Adding port {component.get('id')}")

        for outputPort in flow_dto.get("outputPorts", []):
            component = outputPort.get("component")
            if outputPort.get("allowRemoteAccess"):
                self.nifi_flow.remotely_accessible_ports[
                    component.get("id")
                ] = NifiComponent(
                    component.get("id"),
                    component.get("name"),
                    component.get("type"),
                    component.get("parentGroupId"),
                    NifiType.OUTPUT_PORT,
                    comments=component.get("comments"),
                    status=component.get("status", {}).get("runStatus"),
                )
                logger.debug(f"Adding remotely accessible port {component.get('id')}")
            else:
                self.nifi_flow.components[component.get("id")] = NifiComponent(
                    component.get("id"),
                    component.get("name"),
                    component.get("type"),
                    component.get("parentGroupId"),
                    NifiType.OUTPUT_PORT,
                    comments=component.get("comments"),
                    status=component.get("status", {}).get("runStatus"),
                )
                logger.debug(f"Adding report port {component.get('id')}")

        for rpg in flow_dto.get("remoteProcessGroups", []):
            rpg_component = rpg.get("component", {})
            remote_ports = {}

            contents = rpg_component.get("contents", {})
            for component in contents.get("outputPorts", []):
                if component.get("connected", False):
                    remote_ports[component.get("id")] = NifiComponent(
                        component.get("id"),
                        component.get("name"),
                        component.get("type"),
                        rpg_component.get("parentGroupId"),
                        NifiType.REMOTE_OUTPUT_PORT,
                        target_uris=rpg_component.get("targetUris"),
                        parent_rpg_id=rpg_component.get("id"),
                        comments=component.get("comments"),
                        status=component.get("status", {}).get("runStatus"),
                    )
                    logger.debug(f"Adding remote output port {component.get('id')}")

            for component in contents.get("inputPorts", []):
                if component.get("connected", False):
                    remote_ports[component.get("id")] = NifiComponent(
                        component.get("id"),
                        component.get("name"),
                        component.get("type"),
                        rpg_component.get("parentGroupId"),
                        NifiType.REMOTE_INPUT_PORT,
                        target_uris=rpg_component.get("targetUris"),
                        parent_rpg_id=rpg_component.get("id"),
                        comments=component.get("comments"),
                        status=component.get("status", {}).get("runStatus"),
                    )
                    logger.debug(f"Adding remote input port {component.get('id')}")

            nifi_rpg = NifiRemoteProcessGroup(
                rpg_component.get("id"),
                rpg_component.get("name"),
                component.get("parentGroupId"),
                remote_ports,
            )
            logger.debug(f"Adding remote process group {rpg_component.get('id')}")
            self.nifi_flow.components.update(remote_ports)
            self.nifi_flow.remoteProcessGroups[nifi_rpg.id] = nifi_rpg

        for pg in flow_dto.get("processGroups", []):
            pg_response = self.session.get(
                url=urljoin(self.config.site_url, PG_ENDPOINT) + pg.get("id")
            )

            if not pg_response.ok:
                self.report_warning(
                    self.config.site_url,
                    "Failed to get process group flow " + pg.get("id"),
                )
                continue

            pg_flow_dto = pg_response.json().get("processGroupFlow", {})

            self.update_flow(pg_flow_dto)

    def update_flow_keep_only_ingress_egress(self):
        components_to_del: List[NifiComponent] = []
        for component in self.nifi_flow.components.values():
            if (
                component.nifi_type is NifiType.PROCESSOR
                and component.type
                not in NifiProcessorProvenanceEventAnalyzer.KNOWN_INGRESS_EGRESS_PROCESORS.keys()
            ) or component.nifi_type not in [
                NifiType.PROCESSOR,
                NifiType.REMOTE_INPUT_PORT,
                NifiType.REMOTE_OUTPUT_PORT,
            ]:
                components_to_del.append(component)
                incoming = list(
                    filter(lambda x: x[1] == component.id, self.nifi_flow.connections)
                )
                outgoing = list(
                    filter(lambda x: x[0] == component.id, self.nifi_flow.connections)
                )
                # Create new connections from incoming to outgoing
                for i in incoming:
                    for j in outgoing:
                        self.nifi_flow.connections.append((i[0], j[1]))

                # Remove older connections, as we already created
                # new connections bypassing component to be deleted

                for i in incoming:
                    self.nifi_flow.connections.remove(i)
                for j in outgoing:
                    self.nifi_flow.connections.remove(j)

        for c in components_to_del:
            if c.nifi_type is NifiType.PROCESSOR and (
                c.name.startswith("Get")
                or c.name.startswith("List")
                or c.name.startswith("Fetch")
                or c.name.startswith("Put")
            ):
                self.report_warning(
                    self.config.site_url,
                    f"Dropping NiFi Processor of type {c.type}, id {c.id}, name {c.name} from lineage view. \
                    This is likely an Ingress or Egress node which may be reading to/writing from external datasets \
                    However not currently supported in datahub",
                )
            else:
                logger.debug(
                    f"Dropping NiFi Component of type {c.type}, id {c.id}, name {c.name} from lineage view."
                )

            del self.nifi_flow.components[c.id]

    def create_nifi_flow(self):
        about_response = self.session.get(
            url=urljoin(self.config.site_url, ABOUT_ENDPOINT)
        )
        nifi_version: Optional[str] = None
        if about_response.ok:
            nifi_version = about_response.json().get("about", {}).get("version")
        else:
            logger.warning("Failed to fetch version for nifi")
        cluster_response = self.session.get(
            url=urljoin(self.config.site_url, CLUSTER_ENDPOINT)
        )
        clustered: Optional[bool] = None
        if cluster_response.ok:
            clustered = (
                cluster_response.json().get("clusterSummary", {}).get("clustered")
            )
        else:
            logger.warning("Failed to fetch cluster summary for flow")
        pg_response = self.session.get(
            url=urljoin(self.config.site_url, PG_ENDPOINT) + "root"
        )

        if not pg_response.ok:
            logger.error("Failed to get root process group flow")
            self.report.report_failure(
                self.config.site_url, "Failed to get of root process group flow"
            )

        pg_flow_dto = pg_response.json().get("processGroupFlow", {})
        breadcrumb_dto = pg_flow_dto.get("breadcrumb", {}).get("breadcrumb", {})
        self.nifi_flow = NifiFlow(
            version=nifi_version,
            clustered=clustered,
            root_process_group=NifiProcessGroup(
                breadcrumb_dto.get("id"),
                breadcrumb_dto.get("name"),
                pg_flow_dto.get("parentGroupId"),
            ),
        )
        self.update_flow(pg_flow_dto)
        self.update_flow_keep_only_ingress_egress()

    def fetch_provenance_events(
        self,
        processor: NifiComponent,
        eventType: str,
        startDate: datetime,
        endDate: Optional[datetime] = None,
    ) -> Iterable[Dict]:
        logger.debug(
            f"Fetching {eventType} provenance events for {processor.id}\
            of processor type {processor.type}, Start date: {startDate}, End date: {endDate}"
        )

        older_version: bool = self.nifi_flow.version is not None and version.parse(
            self.nifi_flow.version
        ) < version.parse("1.13.0")

        if older_version:
            searchTerms = {
                "ProcessorID": processor.id,
                "EventType": eventType,
            }
        else:
            searchTerms = {
                "ProcessorID": {"value": processor.id},  # type: ignore
                "EventType": {"value": eventType},  # type: ignore
            }

        payload = json.dumps(
            {
                "provenance": {
                    "request": {
                        "maxResults": 1000,
                        "summarize": False,
                        "searchTerms": searchTerms,
                        "startDate": startDate.strftime("%m/%d/%Y %H:%M:%S %Z"),
                        "endDate": (
                            endDate.strftime("%m/%d/%Y %H:%M:%S %Z")
                            if endDate
                            else None
                        ),
                    }
                }
            }
        )
        logger.debug(payload)
        provenance_response = self.session.post(
            url=urljoin(self.config.site_url, PROVENANCE_ENDPOINT), data=payload
        )

        if provenance_response.ok:
            provenance = provenance_response.json().get("provenance", {})
            provenance_uri = provenance.get("uri")

            provenance_response = self.session.get(provenance_uri)
            if provenance_response.ok:
                provenance = provenance_response.json().get("provenance", {})

            attempts = 5  # wait for at most 5 attempts 5*1= 5 seconds
            while (not provenance.get("finished", False)) and attempts > 0:
                logger.warning(
                    f"Provenance query not completed, attempts left : {attempts}"
                )
                # wait until the uri returns percentcomplete 100
                time.sleep(1)
                provenance_response = self.session.get(provenance_uri)
                attempts -= 1
                if provenance_response.ok:
                    provenance = provenance_response.json().get("provenance", {})

            events = provenance.get("results", {}).get("provenanceEvents", [])
            last_event_time: Optional[datetime] = None
            oldest_event_time: Optional[datetime] = None

            for event in events:
                event_time = parser.parse(event.get("eventTime"))
                # datetime.strptime(
                #    event.get("eventTime"), "%m/%d/%Y %H:%M:%S.%f %Z"
                # )
                if not last_event_time or event_time > last_event_time:
                    last_event_time = event_time

                if not oldest_event_time or event_time < oldest_event_time:
                    oldest_event_time = event_time

                yield event

            processor.last_event_time = str(last_event_time)
            self.delete_provenance(provenance_uri)

            total = provenance.get("results", {}).get("total")
            totalCount = provenance.get("results", {}).get("totalCount")
            if total != str(totalCount):
                yield from self.fetch_provenance_events(
                    processor, eventType, startDate, oldest_event_time
                )
        else:
            self.report_warning(
                self.config.site_url,
                f"provenance events could not be fetched for processor \
                    {processor.id} of type {processor.name}",
            )
            logger.warning(provenance_response.text)
        return

    def report_warning(self, key: str, reason: str) -> None:
        logger.warning(f"{key}: {reason}")
        self.report.report_warning(key, reason)

    def delete_provenance(self, provenance_uri):
        delete_response = self.session.delete(provenance_uri)
        if not delete_response.ok:
            logger.error("failed to delete provenance ", provenance_uri)

    def construct_workunits(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        rootpg = self.nifi_flow.root_process_group
        flow_name = rootpg.name  # self.config.site_name
        flow_urn = builder.make_data_flow_urn(NIFI, rootpg.id, self.config.env)
        flow_properties = {}
        if self.nifi_flow.clustered is not None:
            flow_properties["clustered"] = str(self.nifi_flow.clustered)
        if self.nifi_flow.version is not None:
            flow_properties["version"] = str(self.nifi_flow.version)
        yield from self.construct_flow_workunits(
            flow_urn, flow_name, self.make_external_url(rootpg.id), flow_properties
        )

        for component in self.nifi_flow.components.values():
            job_name = component.name
            job_urn = builder.make_data_job_urn_with_flow(flow_urn, component.id)

            incoming = list(
                filter(lambda x: x[1] == component.id, self.nifi_flow.connections)
            )
            outgoing = list(
                filter(lambda x: x[0] == component.id, self.nifi_flow.connections)
            )
            inputJobs = []
            jobProperties = None

            if component.nifi_type is NifiType.PROCESSOR:
                jobProperties = {
                    k: str(v)
                    for k, v in component.config.items()  # type: ignore
                    if k
                    in [
                        "schedulingPeriod",
                        "schedulingStrategy",
                        "executionNode",
                        "concurrentlySchedulableTaskCount",
                    ]
                }
                jobProperties["properties"] = json.dumps(
                    component.config.get("properties")  # type: ignore
                )
                if component.last_event_time is not None:
                    jobProperties["last_event_time"] = component.last_event_time

                for dataset in component.inlets.values():
                    yield from self.construct_dataset_workunits(
                        dataset.platform,
                        dataset.dataset_name,
                        dataset.dataset_urn,
                        datasetProperties=dataset.dataset_properties,
                    )

                for dataset in component.outlets.values():
                    yield from self.construct_dataset_workunits(
                        dataset.platform,
                        dataset.dataset_name,
                        dataset.dataset_urn,
                        datasetProperties=dataset.dataset_properties,
                    )

            for edge in incoming:
                incoming_from = edge[0]
                if incoming_from in self.nifi_flow.remotely_accessible_ports.keys():
                    dataset_name = f"{self.config.site_name}.{self.nifi_flow.remotely_accessible_ports[incoming_from].name}"
                    dataset_urn = builder.make_dataset_urn(
                        NIFI, dataset_name, self.config.env
                    )
                    component.inlets[dataset_urn] = ExternalDataset(
                        NIFI,
                        dataset_name,
                        dict(nifi_uri=self.config.site_url),
                        dataset_urn,
                    )
                else:
                    inputJobs.append(
                        builder.make_data_job_urn_with_flow(flow_urn, incoming_from)
                    )

            for edge in outgoing:
                outgoing_to = edge[1]
                if outgoing_to in self.nifi_flow.remotely_accessible_ports.keys():
                    dataset_name = f"{self.config.site_name}.{self.nifi_flow.remotely_accessible_ports[outgoing_to].name}"
                    dataset_urn = builder.make_dataset_urn(
                        NIFI, dataset_name, self.config.env
                    )
                    component.outlets[dataset_urn] = ExternalDataset(
                        NIFI,
                        dataset_name,
                        dict(nifi_uri=self.config.site_url),
                        dataset_urn,
                    )

            if component.nifi_type is NifiType.REMOTE_INPUT_PORT:
                # TODO - if target_uris is not set, but http proxy is used in RPG
                site_urls = component.target_uris.split(",")  # type: ignore
                for site_url in site_urls:
                    if site_url not in self.config.site_url_to_site_name:
                        self.report_warning(
                            site_url,
                            f"Site with url {site_url} is being used in flow but\
                            corresponding site name is not configured via site_url_to_site_name.\
                            This may result in broken lineage.",
                        )
                    else:
                        site_name = self.config.site_url_to_site_name[site_url]
                        dataset_name = f"{site_name}.{component.name}"
                        dataset_urn = builder.make_dataset_urn(
                            NIFI, dataset_name, self.config.env
                        )
                        component.outlets[dataset_urn] = ExternalDataset(
                            NIFI, dataset_name, dict(nifi_uri=site_url), dataset_urn
                        )
                        break

            if component.nifi_type is NifiType.REMOTE_OUTPUT_PORT:
                site_urls = component.target_uris.split(",")  # type: ignore
                for site_url in site_urls:
                    if site_url not in self.config.site_url_to_site_name:
                        self.report_warning(
                            self.config.site_url,
                            f"Site with url {site_url} is being used in flow but\
                            corresponding site name is not configured via site_url_to_site_name.\
                            This may result in broken lineage.",
                        )
                    else:
                        site_name = self.config.site_url_to_site_name[site_url]

                        dataset_name = f"{site_name}.{component.name}"
                        dataset_urn = builder.make_dataset_urn(
                            NIFI, dataset_name, self.config.env
                        )
                        component.inlets[dataset_urn] = ExternalDataset(
                            NIFI, dataset_name, dict(nifi_uri=site_url), dataset_urn
                        )
                        break

            yield from self.construct_job_workunits(
                job_urn,
                job_name,
                external_url=self.make_external_url(
                    component.parent_group_id, component.id, component.parent_rpg_id
                ),
                job_type=NIFI.upper() + "_" + component.nifi_type.value,
                description=component.comments,
                job_properties=jobProperties,
                inlets=list(component.inlets.keys()),
                outlets=list(component.outlets.keys()),
                inputJobs=inputJobs,
                status=component.status,
            )

        for port in self.nifi_flow.remotely_accessible_ports.values():
            dataset_name = f"{self.config.site_name}.{port.name}"
            dataset_platform = NIFI
            yield from self.construct_dataset_workunits(
                dataset_platform,
                dataset_name,
                external_url=self.make_external_url(port.parent_group_id, port.id),
            )

    def process_provenance_events(self):
        startDate = datetime.now(timezone.utc) - timedelta(
            days=self.config.provenance_days
        )

        eventAnalyzer = NifiProcessorProvenanceEventAnalyzer()
        eventAnalyzer.env = self.config.env

        for component in self.nifi_flow.components.values():
            if component.nifi_type is NifiType.PROCESSOR:
                eventType = eventAnalyzer.KNOWN_INGRESS_EGRESS_PROCESORS[component.type]
                events = self.fetch_provenance_events(component, eventType, startDate)
                for event in events:
                    dataset = eventAnalyzer.provenance_event_to_lineage_map[
                        component.type
                    ](event)
                    if eventType in [
                        NifiEventType.CREATE,
                        NifiEventType.FETCH,
                        NifiEventType.RECEIVE,
                    ]:
                        component.inlets[dataset.dataset_urn] = dataset
                    else:
                        component.outlets[dataset.dataset_urn] = dataset

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Creates nifi_flow by invoking /flow rest api and saves as self.nifi_flow
        self.create_nifi_flow()

        # Updates inlets and outlets of nifi_flow.components by invoking /provenance rest api
        self.process_provenance_events()

        # Reads and translates entities from self.nifi_flow into mcps
        yield from self.construct_workunits()

    def make_external_url(
        self,
        parent_group_id: str,
        component_id: Optional[str] = "",
        parent_rpg_id: Optional[str] = None,
    ) -> str:
        if parent_rpg_id is not None:
            component_id = parent_rpg_id
        return urljoin(
            self.config.site_url,
            f"/nifi/?processGroupId={parent_group_id}&componentIds={component_id}",
        )

    def construct_flow_workunits(
        self,
        flow_urn: str,
        flow_name: str,
        external_url: str,
        flow_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=flow_name,
                customProperties=flow_properties,
                externalUrl=external_url,
            ),
        )
        for proposal in [mcp]:
            wu = MetadataWorkUnit(
                id=f"{NIFI}.{flow_name}.{proposal.aspectName}", mcp=proposal
            )
            self.report.report_workunit(wu)
            yield wu

    def construct_job_workunits(
        self,
        job_urn: str,
        job_name: str,
        external_url: str,
        job_type: str,
        description: Optional[str],
        job_properties: Optional[Dict[str, str]] = None,
        inlets: List[str] = [],
        outlets: List[str] = [],
        inputJobs: List[str] = [],
        status: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if job_properties:
            job_properties = {k: v for k, v in job_properties.items() if v is not None}

        mcp = MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=job_name,
                type=job_type,
                description=description,
                customProperties=job_properties,
                externalUrl=external_url,
                status=status,
            ),
        )

        wu = MetadataWorkUnit(
            id=f"{NIFI}.{job_name}.{mcp.aspectName}",
            mcp=mcp,
        )
        self.report.report_workunit(wu)
        yield wu

        inlets.sort()
        outlets.sort()
        inputJobs.sort()

        mcp = MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=inlets, outputDatasets=outlets, inputDatajobs=inputJobs
            ),
        )

        wu = MetadataWorkUnit(
            id=f"{NIFI}.{job_name}.{mcp.aspectName}",
            mcp=mcp,
        )
        self.report.report_workunit(wu)
        yield wu

    def construct_dataset_workunits(
        self,
        dataset_platform: str,
        dataset_name: str,
        dataset_urn: Optional[str] = None,
        external_url: Optional[str] = None,
        datasetProperties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if not dataset_urn:
            dataset_urn = builder.make_dataset_urn(
                dataset_platform, dataset_name, self.config.env
            )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(dataset_platform)
            ),
        )
        platform = (
            dataset_platform[dataset_platform.rindex(":") + 1 :]
            if dataset_platform.startswith("urn:")
            else dataset_platform
        )
        wu = MetadataWorkUnit(id=f"{platform}.{dataset_name}.{mcp.aspectName}", mcp=mcp)
        self.report.report_workunit(wu)
        yield wu

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                externalUrl=external_url, customProperties=datasetProperties
            ),
        )

        wu = MetadataWorkUnit(id=f"{platform}.{dataset_name}.{mcp.aspectName}", mcp=mcp)
        self.report.report_workunit(wu)
        yield wu
