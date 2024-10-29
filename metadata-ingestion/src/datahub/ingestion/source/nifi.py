import json
import logging
import ssl
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional, Set, Union
from urllib.parse import urljoin

import requests
from cached_property import cached_property
from dateutil import parser
from packaging import version
from pydantic import root_validator, validator
from pydantic.fields import Field
from requests import Response
from requests.adapters import HTTPAdapter
from requests.models import HTTPBasicAuth
from requests_gssapi import HTTPSPNEGOAuth

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import JobContainerSubTypes
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
)
from datahub.specific.datajob import DataJobPatchBuilder

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
    KERBEROS = "KERBEROS"
    BASIC_AUTH = "BASIC_AUTH"


class ProcessGroupKey(ContainerKey):
    process_group_id: str


class NifiSourceConfig(EnvConfigMixin):
    site_url: str = Field(
        description="URL for Nifi, ending with /nifi/. e.g. https://mynifi.domain/nifi/"
    )

    auth: NifiAuthType = Field(
        default=NifiAuthType.NO_AUTH,
        description="Nifi authentication. must be one of : NO_AUTH, SINGLE_USER, CLIENT_CERT, KERBEROS",
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
        description="Lookup to find site_name for site_url ending with /nifi/, required if using remote process groups in nifi flow",
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
    ca_file: Optional[Union[bool, str]] = Field(
        default=None,
        description="Path to PEM file containing certs for the root CA(s) for the NiFi."
        "Set to False to disable SSL verification.",
    )

    # As of now, container entities retrieval does not respect browsePathsV2 similar to container aspect.
    # Consider enabling this when entities with browsePathsV2 pointing to container also get listed in container entities.
    emit_process_group_as_container: bool = Field(
        default=False,
        description="Whether to emit Nifi process groups as container entities.",
    )

    incremental_lineage: bool = Field(
        default=True,
        description="When enabled, emits incremental/patch lineage for Nifi processors."
        " When disabled, re-states lineage on each run.",
    )

    @root_validator(skip_on_failure=True)
    def validate_auth_params(cla, values):
        if values.get("auth") is NifiAuthType.CLIENT_CERT and not values.get(
            "client_cert_file"
        ):
            raise ValueError(
                "Config `client_cert_file` is required for CLIENT_CERT auth"
            )
        elif values.get("auth") in (
            NifiAuthType.SINGLE_USER,
            NifiAuthType.BASIC_AUTH,
        ) and (not values.get("username") or not values.get("password")):
            raise ValueError(
                f"Config `username` and `password` is required for {values.get('auth').value} auth"
            )
        return values

    @root_validator(skip_on_failure=True)
    def validator_site_url_to_site_name(cls, values):
        site_url_to_site_name = values.get("site_url_to_site_name")
        site_url = values.get("site_url")
        site_name = values.get("site_name")

        if site_url_to_site_name is None:
            site_url_to_site_name = {}
            values["site_url_to_site_name"] = site_url_to_site_name

        if site_url not in site_url_to_site_name:
            site_url_to_site_name[site_url] = site_name

        return values

    @validator("site_url")
    def validator_site_url(cls, site_url: str) -> str:
        assert site_url.startswith(
            ("http://", "https://")
        ), "site_url must start with http:// or https://"

        if not site_url.endswith("/"):
            site_url = site_url + "/"

        if not site_url.endswith("/nifi/"):
            site_url = site_url + "nifi/"

        return site_url


class BidirectionalComponentGraph:
    def __init__(self):
        self._outgoing: Dict[str, Set[str]] = defaultdict(set)
        self._incoming: Dict[str, Set[str]] = defaultdict(set)
        # this will not count duplicates/removal of non-existing connections correctly - it is only there for a quick check
        self._connections_cnt = 0

    def add_connection(self, from_component: str, to_component: str) -> None:
        # this is sanity check
        outgoing_duplicated = to_component in self._outgoing[from_component]
        incoming_duplicated = from_component in self._incoming[to_component]

        self._outgoing[from_component].add(to_component)
        self._incoming[to_component].add(from_component)
        self._connections_cnt += 1

        if outgoing_duplicated or incoming_duplicated:
            logger.warning(
                f"Somehow we attempted to add a connection between 2 components which already existed! Duplicated incoming: {incoming_duplicated}, duplicated outgoing: {outgoing_duplicated}. Connection from component: {from_component} to component: {to_component}"
            )

    def remove_connection(self, from_component: str, to_component: str) -> None:
        self._outgoing[from_component].discard(to_component)
        self._incoming[to_component].discard(from_component)
        self._connections_cnt -= 1

    def get_outgoing_connections(self, component: str) -> Set[str]:
        return self._outgoing[component]

    def get_incoming_connections(self, component: str) -> Set[str]:
        return self._incoming[component]

    def delete_component(self, component: str) -> None:
        logger.debug(f"Deleting component with id: {component}")
        incoming = self._incoming[component]
        logger.debug(
            f"Recognized {len(incoming)} incoming connections to the component"
        )
        outgoing = self._outgoing[component]
        logger.debug(
            f"Recognized {len(outgoing)} outgoing connections from the component"
        )

        for i in incoming:
            for o in outgoing:
                self.add_connection(i, o)

        for i in incoming:
            self._outgoing[i].remove(component)
        for o in outgoing:
            self._incoming[o].remove(component)

        added_connections_cnt = len(incoming) * len(outgoing)
        deleted_connections_cnt = len(incoming) + len(outgoing)
        logger.debug(
            f"Deleted {deleted_connections_cnt} connections and added {added_connections_cnt}"
        )

        del self._outgoing[component]
        del self._incoming[component]

        # for performance reasons we are not using `remove_connection` function when deleting an entire component,
        # therefor we need to adjust the estimated count
        self._connections_cnt -= deleted_connections_cnt

    def __len__(self):
        return self._connections_cnt


TOKEN_ENDPOINT = "access/token"
KERBEROS_TOKEN_ENDPOINT = "access/kerberos"
ABOUT_ENDPOINT = "flow/about"
CLUSTER_ENDPOINT = "flow/cluster/summary"
PG_ENDPOINT = "flow/process-groups/"
PROVENANCE_ENDPOINT = "provenance"


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
        logger.debug(f"Processing s3 provenance event: {event}")
        attributes = event.get("attributes", [])
        s3_bucket = get_attribute_value(attributes, "s3.bucket")
        s3_key = get_attribute_value(attributes, "s3.key")
        if not s3_key:
            logger.debug(
                "s3.key not present in the list of attributes, trying to use filename attribute instead"
            )
            s3_key = get_attribute_value(attributes, "filename")

        s3_url = f"s3://{s3_bucket}/{s3_key}"
        s3_url = s3_url[: s3_url.rindex("/")]
        s3_path = s3_url[len("s3://") :]
        dataset_name = s3_path.replace("/", ".")
        platform = "s3"
        dataset_urn = builder.make_dataset_urn(platform, s3_path, self.env)
        logger.debug(f"Reasoned s3 dataset urn: {dataset_urn}")
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
    connections: BidirectionalComponentGraph = field(
        default_factory=BidirectionalComponentGraph
    )
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
@capability(SourceCapability.LINEAGE_COARSE, "Supported. See docs for limitations")
class NifiSource(Source):

    config: NifiSourceConfig
    report: NifiSourceReport

    def __init__(self, config: NifiSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config
        self.report = NifiSourceReport()
        self.session = requests.Session()

        if self.config.ca_file is not None:
            self.session.verify = self.config.ca_file

        # To keep track of process groups (containers) which have already been ingested
        # Required, as we do not ingest all process groups but only those that have known ingress/egress processors
        self.processed_pgs: List[str] = []

    @cached_property
    def rest_api_base_url(self):
        return self.config.site_url[: -len("nifi/")] + "nifi-api/"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = NifiSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SourceReport:
        return self.report

    def update_flow(
        self, pg_flow_dto: Dict, recursion_level: int = 0
    ) -> None:  # noqa: C901
        """
        Update self.nifi_flow with contents of the input process group `pg_flow_dto`
        """
        logger.debug(
            f"Updating flow with pg_flow_dto {pg_flow_dto.get('breadcrumb', {}).get('breadcrumb', {}).get('id')}, recursion level: {recursion_level}"
        )
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

        logger.debug(f"Processing {len(flow_dto.get('processors', []))} processors")
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
        logger.debug(f"Processing {len(flow_dto.get('funnels', []))} funnels")
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

        logger.debug(f"Processing {len(flow_dto.get('connections', []))} connections")
        for connection in flow_dto.get("connections", []):
            # Exclude self - recursive relationships
            if connection.get("sourceId") != connection.get("destinationId"):
                self.nifi_flow.connections.add_connection(
                    connection.get("sourceId"), connection.get("destinationId")
                )

        logger.debug(f"Processing {len(flow_dto.get('inputPorts', []))} inputPorts")
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

        logger.debug(f"Processing {len(flow_dto.get('outputPorts', []))} outputPorts")
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

        logger.debug(
            f"Processing {len(flow_dto.get('remoteProcessGroups', []))} remoteProcessGroups"
        )
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

        logger.debug(
            f"Processing {len(flow_dto.get('processGroups', []))} processGroups"
        )
        for pg in flow_dto.get("processGroups", []):
            logger.debug(
                f"Retrieving process group: {pg.get('id')} while updating flow for {pg_flow_dto.get('breadcrumb', {}).get('breadcrumb', {}).get('id')}"
            )
            pg_response = self.session.get(
                url=urljoin(self.rest_api_base_url, PG_ENDPOINT) + pg.get("id")
            )

            if not pg_response.ok:
                self.report.warning(
                    "Failed to get process group flow " + pg.get("id"),
                    self.config.site_url,
                )
                continue

            pg_flow_dto = pg_response.json().get("processGroupFlow", {})

            self.update_flow(pg_flow_dto, recursion_level=recursion_level + 1)

    def update_flow_keep_only_ingress_egress(self):
        components_to_del: List[NifiComponent] = []
        components = self.nifi_flow.components.values()
        logger.debug(
            f"Processing {len(components)} components for keep only ingress/egress"
        )
        logger.debug(
            f"All the connections recognized: {len(self.nifi_flow.connections)}"
        )
        for index, component in enumerate(components, start=1):
            logger.debug(
                f"Processing {index}th component for ingress/egress pruning. Component id: {component.id}, name: {component.name}, type: {component.type}"
            )
            logger.debug(
                f"Current amount of connections: {len(self.nifi_flow.connections)}"
            )
            if (
                component.nifi_type is NifiType.PROCESSOR
                and component.type
                not in NifiProcessorProvenanceEventAnalyzer.KNOWN_INGRESS_EGRESS_PROCESORS.keys()
            ) or component.nifi_type not in [
                NifiType.PROCESSOR,
                NifiType.REMOTE_INPUT_PORT,
                NifiType.REMOTE_OUTPUT_PORT,
            ]:
                self.nifi_flow.connections.delete_component(component.id)
                components_to_del.append(component)

        for component in components_to_del:
            if component.nifi_type is NifiType.PROCESSOR and component.name.startswith(
                ("Get", "List", "Fetch", "Put")
            ):
                self.report.warning(
                    f"Dropping NiFi Processor of type {component.type}, id {component.id}, name {component.name} from lineage view. \
                    This is likely an Ingress or Egress node which may be reading to/writing from external datasets \
                    However not currently supported in datahub",
                    self.config.site_url,
                )
            else:
                logger.debug(
                    f"Dropping NiFi Component of type {component.type}, id {component.id}, name {component.name} from lineage view."
                )

            del self.nifi_flow.components[component.id]

    def create_nifi_flow(self):
        logger.debug(f"Retrieving NIFI info from {ABOUT_ENDPOINT}")
        about_response = self.session.get(
            url=urljoin(self.rest_api_base_url, ABOUT_ENDPOINT)
        )
        nifi_version: Optional[str] = None
        if about_response.ok:
            try:
                nifi_version = about_response.json().get("about", {}).get("version")
            except Exception as e:
                logger.error(
                    f"Unable to parse about response from Nifi: {about_response} due to {e}"
                )
        else:
            logger.warning("Failed to fetch version for nifi")
        logger.debug(f"Retrieved nifi version: {nifi_version}")
        logger.debug(f"Retrieving cluster info from {CLUSTER_ENDPOINT}")
        cluster_response = self.session.get(
            url=urljoin(self.rest_api_base_url, CLUSTER_ENDPOINT)
        )
        clustered: Optional[bool] = None
        if cluster_response.ok:
            clustered = (
                cluster_response.json().get("clusterSummary", {}).get("clustered")
            )
            logger.debug(f"Retrieved cluster summary: {clustered}")
        else:
            logger.warning("Failed to fetch cluster summary for flow")
        logger.debug("Retrieving ROOT Process Group")
        pg_response = self.session.get(
            url=urljoin(self.rest_api_base_url, PG_ENDPOINT) + "root"
        )

        pg_response.raise_for_status()

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

        provenance_response = self.submit_provenance_query(
            processor, eventType, startDate, endDate
        )

        if provenance_response.ok:
            provenance = provenance_response.json().get("provenance", {})
            provenance_uri = provenance.get("uri")
            logger.debug(f"Retrieving provenance uri: {provenance_uri}")
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
            logger.debug(f"Retrieved {totalCount} of {total}")
            if total != str(totalCount):
                logger.debug("Trying to retrieve more events for the same processor")
                yield from self.fetch_provenance_events(
                    processor, eventType, startDate, oldest_event_time
                )
        else:
            self.report.warning(
                f"Provenance events could not be fetched for processor \
                    {processor.id} of type {processor.name}",
                self.config.site_url,
            )
            logger.warning(provenance_response.text)
        return

    def submit_provenance_query(self, processor, eventType, startDate, endDate):
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
        self.session.headers.update({})

        self.session.headers.update({"Content-Type": "application/json"})
        provenance_response = self.session.post(
            url=urljoin(self.rest_api_base_url, PROVENANCE_ENDPOINT),
            data=payload,
        )

        # Revert to default content-type if basic-auth
        if self.config.auth is NifiAuthType.BASIC_AUTH:
            self.session.headers.update(
                {
                    "Content-Type": "application/x-www-form-urlencoded",
                }
            )

        return provenance_response

    def delete_provenance(self, provenance_uri):
        logger.debug(f"Deleting provenance with uri: {provenance_uri}")
        delete_response = self.session.delete(provenance_uri)
        if not delete_response.ok:
            logger.error("failed to delete provenance ", provenance_uri)

    def construct_workunits(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        rootpg = self.nifi_flow.root_process_group
        flow_name = rootpg.name  # self.config.site_name
        flow_urn = self.make_flow_urn()
        flow_properties = {}
        if self.nifi_flow.clustered is not None:
            flow_properties["clustered"] = str(self.nifi_flow.clustered)
        if self.nifi_flow.version is not None:
            flow_properties["version"] = str(self.nifi_flow.version)
        yield self.construct_flow_workunits(
            flow_urn, flow_name, self.make_external_url(rootpg.id), flow_properties
        )

        for component in self.nifi_flow.components.values():
            logger.debug(
                f"Beginng construction of workunits for component {component.id} of type {component.type} and name {component.name}"
            )
            logger.debug(f"Inlets of the component: {component.inlets.keys()}")
            logger.debug(f"Outlets of the component: {component.outlets.keys()}")
            job_name = component.name
            job_urn = builder.make_data_job_urn_with_flow(flow_urn, component.id)

            incoming = self.nifi_flow.connections.get_incoming_connections(component.id)
            outgoing = self.nifi_flow.connections.get_outgoing_connections(component.id)
            inputJobs = set()
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
                    logger.debug(
                        f"Yielding dataset workunits for {dataset.dataset_urn} (inlet)"
                    )
                    yield from self.construct_dataset_workunits(
                        dataset.platform,
                        dataset.dataset_name,
                        dataset.dataset_urn,
                        datasetProperties=dataset.dataset_properties,
                    )

                for dataset in component.outlets.values():
                    logger.debug(
                        f"Yielding dataset workunits for {dataset.dataset_urn} (outlet)"
                    )
                    yield from self.construct_dataset_workunits(
                        dataset.platform,
                        dataset.dataset_name,
                        dataset.dataset_urn,
                        datasetProperties=dataset.dataset_properties,
                    )

            for incoming_from in incoming:
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
                    inputJobs.add(
                        builder.make_data_job_urn_with_flow(flow_urn, incoming_from)
                    )

            for outgoing_to in outgoing:
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
                        self.report.warning(
                            f"Site with url {site_url} is being used in flow but\
                            corresponding site name is not configured via site_url_to_site_name.\
                            This may result in broken lineage.",
                            site_url,
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
                        self.report.warning(
                            f"Site with url {site_url} is being used in flow but\
                            corresponding site name is not configured via site_url_to_site_name.\
                            This may result in broken lineage.",
                            self.config.site_url,
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

            if self.config.emit_process_group_as_container:
                # We emit process groups only for all nifi components qualifying as datajobs
                yield from self.construct_process_group_workunits(
                    component.parent_group_id
                )

            yield from self.construct_job_workunits(
                job_urn,
                job_name,
                component.parent_group_id,
                external_url=self.make_external_url(
                    component.parent_group_id, component.id, component.parent_rpg_id
                ),
                job_type=NIFI.upper() + "_" + component.nifi_type.value,
                description=component.comments,
                job_properties=jobProperties,
                inlets=list(component.inlets.keys()),
                outlets=list(component.outlets.keys()),
                inputJobs=list(inputJobs),
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

    def make_flow_urn(self) -> str:
        return builder.make_data_flow_urn(
            NIFI, self.nifi_flow.root_process_group.id, self.config.env
        )

    def process_provenance_events(self):
        logger.debug("Starting processing of provenance events")
        startDate = datetime.now(timezone.utc) - timedelta(
            days=self.config.provenance_days
        )

        eventAnalyzer = NifiProcessorProvenanceEventAnalyzer()
        eventAnalyzer.env = self.config.env
        components = self.nifi_flow.components.values()
        logger.debug(f"Processing {len(components)} components")
        for component in components:
            logger.debug(
                f"Processing provenance events for component id: {component.id} name: {component.name}"
            )
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

    def authenticate(self):
        if self.config.auth is NifiAuthType.NO_AUTH:
            # Token not required
            return

        if self.config.auth is NifiAuthType.BASIC_AUTH:
            assert self.config.username is not None
            assert self.config.password is not None
            self.session.auth = HTTPBasicAuth(
                self.config.username, self.config.password
            )
            self.session.headers.update(
                {
                    "Content-Type": "application/x-www-form-urlencoded",
                }
            )
            return

        if self.config.auth is NifiAuthType.CLIENT_CERT:
            self.session.mount(
                self.rest_api_base_url,
                SSLAdapter(
                    certfile=self.config.client_cert_file,
                    keyfile=self.config.client_key_file,
                    password=self.config.client_key_password,
                ),
            )
            return

        # get token flow
        token_response: Response
        if self.config.auth is NifiAuthType.SINGLE_USER:
            token_response = self.session.post(
                url=urljoin(self.rest_api_base_url, TOKEN_ENDPOINT),
                data={
                    "username": self.config.username,
                    "password": self.config.password,
                },
            )
        elif self.config.auth is NifiAuthType.KERBEROS:
            token_response = self.session.post(
                url=urljoin(self.rest_api_base_url, KERBEROS_TOKEN_ENDPOINT),
                auth=HTTPSPNEGOAuth(),
            )
        else:
            raise Exception(f"Unsupported auth: {self.config.auth}")

        token_response.raise_for_status()
        self.session.headers.update({"Authorization": "Bearer " + token_response.text})

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            self.authenticate()
        except Exception as e:
            self.report.failure("Failed to authenticate", self.config.site_url, exc=e)
            return

        # Creates nifi_flow by invoking /flow rest api and saves as self.nifi_flow
        try:
            self.create_nifi_flow()
        except Exception as e:
            self.report.failure(
                "Failed to get root process group flow", self.config.site_url, exc=e
            )
            return

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
            f"?processGroupId={parent_group_id}&componentIds={component_id}",
        )

    def construct_flow_workunits(
        self,
        flow_urn: str,
        flow_name: str,
        external_url: str,
        flow_properties: Optional[Dict[str, str]] = None,
    ) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=flow_name,
                customProperties=flow_properties,
                externalUrl=external_url,
            ),
        ).as_workunit()

    def construct_job_workunits(
        self,
        job_urn: str,
        job_name: str,
        parent_group_id: str,
        external_url: str,
        job_type: str,
        description: Optional[str],
        job_properties: Optional[Dict[str, str]] = None,
        inlets: List[str] = [],
        outlets: List[str] = [],
        inputJobs: List[str] = [],
        status: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        logger.debug(f"Begining construction of job workunit for {job_urn}")
        if job_properties:
            job_properties = {k: v for k, v in job_properties.items() if v is not None}

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=job_name,
                type=job_type,
                description=description,
                customProperties=job_properties,
                externalUrl=external_url,
                status=status,
            ),
        ).as_workunit()

        # If dataJob had container aspect, we would ideally only emit it
        # and browse path v2 would automatically be generated.
        yield self.gen_browse_path_v2_workunit(job_urn, parent_group_id)

        inlets.sort()
        outlets.sort()
        inputJobs.sort()
        logger.debug(f"Inlets after sorting: {inlets}")
        logger.debug(f"Outlets after sorting: {outlets}")
        logger.debug(f"Input jobs after sorting: {inputJobs}")

        if self.config.incremental_lineage:
            logger.debug("Preparing mcps for incremental lineage")
            patch_builder: DataJobPatchBuilder = DataJobPatchBuilder(job_urn)
            for inlet in inlets:
                patch_builder.add_input_dataset(inlet)
            for outlet in outlets:
                patch_builder.add_output_dataset(outlet)
            for inJob in inputJobs:
                patch_builder.add_input_datajob(inJob)
            for patch_mcp in patch_builder.build():
                logger.debug(f"Preparing Patch MCP: {patch_mcp}")
                yield MetadataWorkUnit(
                    id=f"{job_urn}-{patch_mcp.aspectName}", mcp_raw=patch_mcp
                )
        else:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=inlets,
                    outputDatasets=outlets,
                    inputDatajobs=inputJobs,
                ),
            ).as_workunit()

    def gen_browse_path_v2_workunit(
        self, entity_urn: str, process_group_id: str
    ) -> MetadataWorkUnit:
        flow_urn = self.make_flow_urn()
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(id=flow_urn, urn=flow_urn),
                    *self._get_browse_path_v2_entries(process_group_id),
                ]
            ),
        ).as_workunit()

    def _get_browse_path_v2_entries(
        self, process_group_id: str
    ) -> List[BrowsePathEntryClass]:
        """Browse path entries till current process group"""
        if self._is_root_process_group(process_group_id):
            return []

        current_process_group = self.nifi_flow.processGroups[process_group_id]
        assert (
            current_process_group.parent_group_id
        )  # always present for non-root process group
        parent_browse_path = self._get_browse_path_v2_entries(
            current_process_group.parent_group_id
        )

        if self.config.emit_process_group_as_container:
            container_urn = self.gen_process_group_key(process_group_id).as_urn()
            current_browse_entry = BrowsePathEntryClass(
                id=container_urn, urn=container_urn
            )
        else:
            current_browse_entry = BrowsePathEntryClass(id=current_process_group.name)
        return parent_browse_path + [current_browse_entry]

    def _is_root_process_group(self, process_group_id: str) -> bool:
        return self.nifi_flow.root_process_group.id == process_group_id

    def construct_process_group_workunits(
        self, process_group_id: str
    ) -> Iterable[MetadataWorkUnit]:
        if (
            self._is_root_process_group(process_group_id)
            or process_group_id in self.processed_pgs
        ):
            return
        self.processed_pgs.append(process_group_id)

        pg = self.nifi_flow.processGroups[process_group_id]
        container_key = self.gen_process_group_key(process_group_id)
        yield from gen_containers(
            container_key=container_key,
            name=pg.name,
            sub_types=[JobContainerSubTypes.NIFI_PROCESS_GROUP],
            parent_container_key=(
                self.gen_process_group_key(pg.parent_group_id)
                if pg.parent_group_id
                and not self._is_root_process_group(pg.parent_group_id)
                else None
            ),
        )

        if pg.parent_group_id:  # always true for non-root process group
            yield from self.construct_process_group_workunits(pg.parent_group_id)

            if self._is_root_process_group(pg.parent_group_id):
                yield self.gen_browse_path_v2_workunit(
                    container_key.as_urn(), pg.parent_group_id
                )

    def gen_process_group_key(self, process_group_id: str) -> ProcessGroupKey:
        return ProcessGroupKey(
            process_group_id=process_group_id, platform=NIFI, env=self.config.env
        )

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

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(dataset_platform)
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                externalUrl=external_url, customProperties=datasetProperties
            ),
        ).as_workunit()
