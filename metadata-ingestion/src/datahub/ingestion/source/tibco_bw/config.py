from typing import Dict, List, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.tibco_bw.constants import (
    DEFAULT_CLOUD_BASE_URL,
    EMS_DEFAULT_SERVER_GROUP,
)
from datahub.ingestion.source.tibco_bw.models import TibcoDeployment
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urns.error import InvalidUrnError


class TibcoAppLineage(ConfigModel):
    # Manually declared lineage for a BusinessWorks/TCI application. The runtime
    # APIs expose deployment topology but not the datasets an application reads or
    # writes, so upstream/downstream dataset urns are supplied here by the operator.
    upstreams: List[str] = Field(
        default_factory=list,
        description="Dataset urns the application consumes (its inputs).",
    )
    downstreams: List[str] = Field(
        default_factory=list,
        description="Dataset urns the application produces (its outputs).",
    )

    @field_validator("upstreams", "downstreams")
    @classmethod
    def _validate_dataset_urns(cls, value: List[str]) -> List[str]:
        for urn in value:
            # Fail fast on typos rather than emitting lineage to a malformed urn.
            try:
                DatasetUrn.from_string(urn)
            except InvalidUrnError as e:
                raise ValueError(str(e)) from e
        return value


class TibcoEmsTarget(ConfigModel):
    # Where the EMS destinations a process publishes to live in DataHub. A
    # destination's dataset name is built exactly as the TIBCO EMS source builds
    # it, so both connectors describe the same entity rather than two near-copies.
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance of the TIBCO EMS source that ingests these "
        "destinations, if it is ingested with one.",
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment (fabric) of the EMS destinations. Defaults to the "
        "source's `env` when unset.",
    )
    server_group: str = Field(
        default=EMS_DEFAULT_SERVER_GROUP,
        description="EMS server group the destinations belong to. This leads the "
        "destination's dataset name, so it must match the group the EMS source "
        "reported; on a proxy that predates server groups that is `default`.",
    )


class ApplicationArchivesConfig(ConfigModel):
    # A BusinessWorks message schema is declared at design time in the publishing
    # process, and the archive is the only artefact that carries it. bwagent has
    # no archive download endpoint, so the operator supplies the file - the same
    # arrangement as the dbt source and its manifest.
    paths: List[str] = Field(
        default_factory=list,
        description="Paths or glob patterns of application archives (`.ear`) to "
        "read message schemas from, e.g. `/mnt/bw-archives/*.ear`. Obtain them "
        "with `bwadmin download` or from the Admin UI; the bwagent REST API "
        "cannot serve them.",
    )
    emit_destination_schemas: bool = Field(
        default=True,
        description="Emit the declared message schema onto each EMS destination "
        "dataset the archive publishes to.",
    )
    emit_destination_lineage: bool = Field(
        default=True,
        description="Emit lineage between the publishing application and the EMS "
        "destinations it writes to, and the destinations it reads from, derived "
        "from the archive's JMS activities rather than the `application_lineage` "
        "map.",
    )
    ems_target: TibcoEmsTarget = Field(
        default_factory=TibcoEmsTarget,
        description="How to address the EMS destinations in DataHub.",
    )


class TibcoBwSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    deployment: TibcoDeployment = Field(
        description="Which TIBCO runtime to ingest from: `on_prem` for "
        "ActiveMatrix BusinessWorks (bwagent REST API) or `cloud` for TIBCO "
        "Cloud Integration.",
    )
    base_url: Optional[str] = Field(
        default=None,
        description="Base URL of the API. For `on_prem` this is the bwagent "
        "endpoint, e.g. `http://bw-host.example.com:8079`. For `cloud` it "
        f"defaults to `{DEFAULT_CLOUD_BASE_URL}` and rarely needs overriding.",
    )
    username: Optional[SecretStr] = Field(
        default=None,
        description="Username for bwagent HTTP basic authentication (on_prem).",
    )
    password: Optional[SecretStr] = Field(
        default=None,
        description="Password for bwagent HTTP basic authentication (on_prem).",
    )
    token: Optional[SecretStr] = Field(
        default=None,
        description="OAuth access token for the TIBCO Cloud Integration API (cloud).",
    )
    ca_certificate_path: Optional[str] = Field(
        default=None,
        description="Path to a CA bundle used to verify the server's TLS certificate.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify the server's TLS certificate. Prefer "
        "`ca_certificate_path` for private CAs over disabling verification.",
    )
    timeout: int = Field(default=30, description="Per-request timeout in seconds.")
    domain_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter BusinessWorks domains (on_prem).",
    )
    appspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter BusinessWorks appspaces (on_prem).",
    )
    subscription_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter TIBCO Cloud subscriptions (cloud).",
    )
    application_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter deployed applications by name.",
    )
    include_appnodes: bool = Field(
        default=True,
        description="Attach appnode names and run states to the appspace as "
        "custom properties (on_prem only).",
    )
    application_lineage: Dict[str, TibcoAppLineage] = Field(
        default_factory=dict,
        description="Manually declared lineage per application, mapping it to the "
        "dataset urns it consumes and produces. The TIBCO runtime APIs do not expose "
        "which datasets an application reads or writes, so lineage is supplied here "
        "rather than discovered. Key by `<scope>/<application>` - where scope is "
        "`<domain>/<appspace>` on-prem or the subscription id in the cloud - to target "
        "one deployment; a bare application name is also accepted and applies to every "
        "application with that name, which is only safe when the name is unique across "
        "the estate.",
    )
    emit_column_lineage: bool = Field(
        default=False,
        description="Also emit column-level lineage between an application's declared "
        "upstream and downstream datasets. Fields are matched by name (case-insensitive) "
        "using schemas read from DataHub. Best-effort: emitted only where both a "
        "declared upstream and downstream have a schema and share field names, and it "
        "assumes the application passes fields through unchanged. Requires a DataHub "
        "graph to be available.",
    )
    application_archives: ApplicationArchivesConfig = Field(
        default_factory=ApplicationArchivesConfig,
        description="Read declared JMS message schemas and destination lineage from "
        "supplied BusinessWorks application archives. EMS has no schema registry, so "
        "the publishing process's archive is the only place a message's shape is "
        "actually declared.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion config for stale entity removal.",
    )

    @field_validator("base_url")
    @classmethod
    def _strip_trailing_slash(cls, value: Optional[str]) -> Optional[str]:
        return value.rstrip("/") if value is not None else value

    @model_validator(mode="after")
    def _validate_deployment(self) -> "TibcoBwSourceConfig":
        if self.deployment is TibcoDeployment.ON_PREM:
            if not self.base_url:
                raise ValueError(
                    "base_url is required for on_prem deployment (the bwagent REST API URL)."
                )
            if not (self.username and self.password):
                raise ValueError(
                    "username and password are required for on_prem deployment."
                )
        else:
            if not self.token:
                raise ValueError(
                    "token is required for cloud deployment (TIBCO Cloud OAuth token)."
                )
            if not self.base_url:
                self.base_url = DEFAULT_CLOUD_BASE_URL
        return self
