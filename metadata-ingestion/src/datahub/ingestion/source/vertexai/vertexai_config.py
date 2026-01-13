from __future__ import annotations

import logging
import re
from copy import deepcopy
from typing import Any, Dict, List, Optional

import pydantic
from pydantic import Field, field_validator, model_validator
from typing_extensions import Self

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.common.gcp_project_utils import (
    GCPValidationError,
    validate_project_id_list,
    validate_project_label_list,
)

logger = logging.getLogger(__name__)


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

    project_id: Optional[str] = Field(
        default=None,
        description="[DEPRECATED] Use 'project_ids' instead. Single GCP project ID.",
    )

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
    def validate_project_ids_field(cls, project_ids: List[str]) -> List[str]:
        if project_ids == []:
            raise ValueError(
                "project_ids cannot be an empty list. "
                "Either specify project IDs or omit the field to use auto-discovery."
            )
        try:
            validate_project_id_list(project_ids, allow_empty=True)
        except GCPValidationError as e:
            raise ValueError(str(e)) from e
        return project_ids

    project_labels: List[str] = Field(
        default_factory=list,
        description=(
            "Ingests projects with the specified labels. Format: 'key:value' or 'key' (for any value). "
            "Example: ['env:prod', 'team:ml']. If project_ids is set, this is ignored. "
            "The ingestion process filters projects by label first, then applies project_id_pattern. "
            "If no projects match both criteria, ingestion will fail at runtime. "
            "Note: Label-based discovery requires organization-level 'resourcemanager.projects.list' "
            "permission. If you only have project-level permissions, use explicit project_ids instead."
        ),
    )

    @field_validator("project_labels")
    @classmethod
    def validate_project_labels_field(cls, project_labels: List[str]) -> List[str]:
        try:
            validate_project_label_list(project_labels)
        except GCPValidationError as e:
            raise ValueError(str(e)) from e
        return project_labels

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
    def migrate_deprecated_project_id(
        cls,
        values: Any,
        handler: pydantic.ValidatorFunctionWrapHandler,
        info: pydantic.ValidationInfo,
    ) -> Self:
        """
        Auto-migrate deprecated 'project_id' to 'project_ids'.

        This validator handles backward compatibility and emits deprecation warnings.
        """
        if not isinstance(values, dict):
            return handler(values)

        values = deepcopy(values)
        project_id = values.get("project_id")
        project_ids = values.get("project_ids")

        if not project_id:
            return handler(values)

        if not project_ids:
            logger.warning(
                "Config field 'project_id' is deprecated and will be removed in a future release. "
                "Please update your config to use 'project_ids: [\"%s\"]' instead. "
                "Auto-migrating for now.",
                project_id,
            )
            values["project_ids"] = [project_id]
            values.pop("project_id", None)
            return handler(values)

        if project_id in project_ids:
            logger.warning(
                "Both 'project_id' (deprecated) and 'project_ids' are set with the same project. "
                "Ignoring 'project_id' - please remove it from your config."
            )
        else:
            logger.warning(
                "Both 'project_id' (deprecated) and 'project_ids' are set. "
                "Using 'project_ids' and ignoring 'project_id'. "
                "Please remove 'project_id' from your config."
            )
        values.pop("project_id", None)
        return handler(values)

    @model_validator(mode="after")
    def _validate_pattern_filtering(self) -> VertexAIConfig:
        if not self.project_ids:
            return self

        _check_pattern_filters_all(
            self.project_ids, self.project_id_pattern, field_name="project_ids"
        )

        return self

    @model_validator(mode="after")
    def _validate_auto_discovery_pattern(self) -> VertexAIConfig:
        has_restrictive_pattern = self.project_id_pattern.allow != [".*"]
        relies_on_auto_discovery = not self.project_ids and not self.project_labels

        if has_restrictive_pattern and relies_on_auto_discovery:
            raise ValueError(
                f"Auto-discovery with restrictive allow patterns ({self.project_id_pattern.allow}) "
                "is not supported. Specify project_ids, use project_labels, or remove the pattern."
            )

        return self

    def has_explicit_project_ids(self) -> bool:
        return bool(self.project_ids)
