import logging
import re
from copy import deepcopy
from typing import Any, Dict, List, Optional

from pydantic import Field, PrivateAttr, field_validator, model_validator
from pydantic.functional_validators import ModelWrapValidatorHandler

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential

logger = logging.getLogger(__name__)

LABEL_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*(?::[a-z0-9_-]+)?$")
# GCP project IDs: 6-30 chars, lowercase letters, digits, hyphens
# Must start with letter, end with letter or digit
GCP_PROJECT_ID_PATTERN = re.compile(r"^[a-z][-a-z0-9]{4,28}[a-z0-9]$")


def _check_pattern_filters_all(
    project_ids: List[str],
    pattern: AllowDenyPattern,
    *,
    field_name: str = "project_ids",
) -> None:
    filtered = [pid for pid in project_ids if pattern.allowed(pid)]
    if not filtered:
        raise ValueError(
            f"All {len(project_ids)} configured {field_name} were filtered out "
            "by project_id_pattern. Check your allow/deny patterns."
        )


class VertexAIConfig(EnvConfigMixin):
    credential: Optional[GCPCredential] = Field(
        default=None, description="GCP credential information"
    )

    _used_deprecated_project_id: bool = PrivateAttr(default=False)

    project_ids: List[str] = Field(
        default_factory=list,
        description=(
            "List of GCP project IDs to ingest Vertex AI resources from. "
            "If set, takes precedence over project_labels and auto-discovery. "
            "The project_id_pattern is still applied to filter this list. "
            "Note: Using explicit project_ids does not require org-level "
            "resourcemanager.projects.list permission."
        ),
    )

    @field_validator("project_ids")
    @classmethod
    def validate_project_ids_field(cls, v: List[str]) -> List[str]:
        if not v:
            return v
        if any(not p.strip() for p in v):
            raise ValueError("project_ids contains empty values")
        if len(v) != len(set(v)):
            raise ValueError("project_ids contains duplicates")
        invalid = [p for p in v if not GCP_PROJECT_ID_PATTERN.match(p)]
        if invalid:
            raise ValueError(
                f"Invalid project_ids format: {invalid}. "
                "Must be 6-30 chars, lowercase letters/numbers/hyphens, "
                "start with letter, end with letter or number."
            )
        return v

    project_labels: List[str] = Field(
        default_factory=list,
        description=(
            "Ingests projects with the specified labels. Format: 'key:value' or 'key' (for any value). "
            "Example: ['env:prod', 'team:ml']. If project_ids is set, this is ignored. "
            "The ingestion process filters projects by label first, then applies project_id_pattern. "
            "If no projects match both criteria, ingestion will fail at runtime."
        ),
    )

    @field_validator("project_labels")
    @classmethod
    def validate_project_labels_field(cls, v: List[str]) -> List[str]:
        if not v:
            return v
        if any(not label.strip() for label in v):
            raise ValueError("project_labels contains empty values")
        if len(v) != len(set(v)):
            raise ValueError("project_labels contains duplicates")
        invalid = [label for label in v if not LABEL_PATTERN.match(label)]
        if invalid:
            raise ValueError(
                f"Invalid project_labels format: {invalid}. "
                "Must be 'key' or 'key:value' format. Example: env:prod"
            )
        return v

    project_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter project IDs. Applied after project_ids or "
            "project_labels selection. Use 'allow' for whitelist, 'deny' for blacklist."
        ),
    )

    @field_validator("project_id_pattern")
    @classmethod
    def validate_project_id_pattern_syntax(
        cls, v: AllowDenyPattern
    ) -> AllowDenyPattern:
        """Validate regex pattern syntax upfront to fail fast on config errors."""
        invalid_patterns = []
        for pattern in v.allow + v.deny:
            try:
                re.compile(pattern)
            except re.error as e:
                invalid_patterns.append(f"'{pattern}': {e}")

        if invalid_patterns:
            raise ValueError(
                f"Invalid regex in project_id_pattern: {', '.join(invalid_patterns)}. "
                "Check your allow/deny patterns for syntax errors."
            )
        return v

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

    def get_credentials_dict(self) -> Optional[Dict[str, Any]]:
        if self.credential:
            return self.credential.to_dict()
        return None

    @model_validator(mode="wrap")
    @classmethod
    def project_id_backward_compatibility_and_validate(
        cls, values: Any, handler: "ModelWrapValidatorHandler[VertexAIConfig]"
    ) -> "VertexAIConfig":
        if isinstance(values, dict):
            values = deepcopy(values)
            project_id = values.pop("project_id", None)
            project_ids = values.get("project_ids")
            used_deprecated = False

            if not project_ids and project_id:
                values["project_ids"] = [project_id]
                used_deprecated = True
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

            model = handler(values)
            model._used_deprecated_project_id = used_deprecated
        else:
            model = handler(values)

        field_name = (
            "project_id (deprecated)"
            if model._used_deprecated_project_id
            else "project_ids"
        )

        if model.project_ids:
            _check_pattern_filters_all(
                model.project_ids, model.project_id_pattern, field_name=field_name
            )

        has_restrictive_pattern = model.project_id_pattern.allow != [".*"]
        relies_on_auto_discovery = not model.project_ids and not model.project_labels

        if has_restrictive_pattern and relies_on_auto_discovery:
            raise ValueError(
                f"Auto-discovery with restrictive allow patterns ({model.project_id_pattern.allow}) "
                "is not supported. Specify project_ids, use project_labels, or remove the pattern."
            )

        return model

    def has_explicit_project_ids(self) -> bool:
        return bool(self.project_ids)
