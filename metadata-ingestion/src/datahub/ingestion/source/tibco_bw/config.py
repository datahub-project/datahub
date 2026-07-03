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
from datahub.ingestion.source.tibco_bw.constants import DEFAULT_CLOUD_BASE_URL
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
        description="Manually declared lineage per application, keyed by application "
        "name. Maps each application to the dataset urns it consumes and produces. "
        "The TIBCO runtime APIs do not expose which datasets an application reads or "
        "writes, so lineage is supplied here rather than discovered.",
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
