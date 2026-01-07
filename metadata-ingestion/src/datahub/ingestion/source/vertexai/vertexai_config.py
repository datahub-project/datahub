import atexit
import logging
import os
import re
from copy import deepcopy
from typing import Any, Dict, List, Optional

from pydantic import Field, PrivateAttr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential

logger = logging.getLogger(__name__)

LABEL_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*(?::[a-z0-9_-]+)?$")


def _check_pattern_filters_all(
    project_ids: List[str], pattern: AllowDenyPattern
) -> None:
    filtered = [pid for pid in project_ids if pattern.allowed(pid)]
    if not filtered:
        raise ValueError(
            f"All {len(project_ids)} configured project_ids were filtered out "
            "by project_id_pattern. Check your allow/deny patterns."
        )


class VertexAIConfig(EnvConfigMixin):
    credential: Optional[GCPCredential] = Field(
        default=None, description="GCP credential information"
    )

    _credentials_path: Optional[str] = PrivateAttr(None)

    project_ids: List[str] = Field(
        default_factory=list,
        description=(
            "List of GCP project IDs to ingest Vertex AI resources from. "
            "If set, takes precedence over project_labels and auto-discovery. "
            "The project_id_pattern is still applied to filter this list."
        ),
    )

    @field_validator("project_ids")
    @classmethod
    def validate_project_ids_field(cls, v: List[str]) -> List[str]:
        if not v:
            return v
        empty_ids = [pid for pid in v if not pid.strip()]
        if empty_ids:
            raise ValueError(
                f"project_ids contains {len(empty_ids)} empty or whitespace-only value(s). "
                "Remove empty strings from the list."
            )
        duplicates = [pid for pid in v if v.count(pid) > 1]
        if duplicates:
            raise ValueError(
                f"project_ids contains duplicates: {list(set(duplicates))}. "
                "Remove duplicate entries from your configuration."
            )
        return v

    project_labels: List[str] = Field(
        default_factory=list,
        description=(
            "Discover projects by GCP labels. Format: 'key:value' or 'key' (for any value). "
            "Example: ['env:prod', 'team:ml']. If project_ids is set, this is ignored. "
            "The project_id_pattern is applied after label filtering."
        ),
    )

    @field_validator("project_labels")
    @classmethod
    def validate_project_labels_field(cls, v: List[str]) -> List[str]:
        if not v:
            return v
        invalid = [label for label in v if not LABEL_PATTERN.match(label)]
        if invalid:
            raise ValueError(
                f"Invalid project_labels format: {invalid}. "
                "Labels must be 'key' or 'key:value' format."
            )
        return v

    project_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter project IDs. Applied after project_ids or "
            "project_labels selection. Use 'allow' for whitelist, 'deny' for blacklist."
        ),
    )

    region: str = Field(
        description="Region of your project in Google Cloud Platform.",
    )

    bucket_uri: Optional[str] = Field(
        default=None,
        description="Bucket URI used in your project",
    )

    vertexai_url: Optional[str] = Field(
        default="https://console.cloud.google.com/vertex-ai",
        description="VertexAI Console base URL",
    )

    def __init__(self, **data: Any):
        super().__init__(**data)

        if self.credential:
            self._credentials_path = self.credential.create_credential_temp_file()
            logger.debug(
                "Creating temporary credential file at %s", self._credentials_path
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path
            atexit.register(self._cleanup_credentials)

    def _cleanup_credentials(self) -> None:
        if self._credentials_path:
            try:
                os.unlink(self._credentials_path)
                logger.debug(
                    "Cleaned up temp credentials file: %s", self._credentials_path
                )
            except FileNotFoundError:
                pass
            except OSError as e:
                logger.warning(
                    "Failed to cleanup credentials %s: %s", self._credentials_path, e
                )
            finally:
                self._credentials_path = None

    @model_validator(mode="before")
    @classmethod
    def project_id_backward_compatibility(cls, values: Dict) -> Dict:
        values = deepcopy(values)
        project_id = values.pop("project_id", None)
        project_ids = values.get("project_ids")

        if not project_ids and project_id:
            values["project_ids"] = [project_id]
            logger.warning(
                "Config field `project_id` is deprecated. "
                "Your config has been auto-converted to `project_ids: [%s]`. "
                "Please update your config file to use `project_ids` directly. "
                "See https://datahubproject.io/docs/generated/ingestion/sources/vertexai for details.",
                project_id,
            )
        elif project_ids and project_id:
            logger.warning(
                "Both `project_id` and `project_ids` specified. "
                "Using `project_ids` and ignoring deprecated `project_id`. "
                "Please remove `project_id` from your config."
            )
        return values

    @model_validator(mode="after")
    def validate_projects_config(self) -> "VertexAIConfig":
        if self.project_ids:
            _check_pattern_filters_all(self.project_ids, self.project_id_pattern)

        is_default_allow = self.project_id_pattern.allow == [".*"]
        no_explicit_projects = not self.project_ids and not self.project_labels

        if not is_default_allow and no_explicit_projects:
            raise ValueError(
                f"Auto-discovery with restrictive allow patterns ({self.project_id_pattern.allow}) "
                "is not supported. Either specify project_ids explicitly, use project_labels, "
                "or remove the allow pattern to discover all accessible projects."
            )

        if self.project_labels and not is_default_allow:
            logger.warning(
                "RISK: project_labels combined with restrictive project_id_pattern (allow=%s). "
                "This filters AFTER expensive GCP API calls. If no projects match both, "
                "ingestion WILL FAIL at runtime. Consider using project_ids explicitly instead.",
                self.project_id_pattern.allow,
            )

        return self

    def has_explicit_project_ids(self) -> bool:
        return bool(self.project_ids)
