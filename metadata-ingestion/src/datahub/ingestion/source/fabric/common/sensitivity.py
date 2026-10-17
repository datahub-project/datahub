"""Resolve a Fabric sensitivity label id into a DataHub tag or glossary term."""

import logging
from dataclasses import dataclass
from typing import Any, List, Literal, Optional

import requests
from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.fabric.common.auth import (
    GRAPH_API_SCOPE,
    FabricAuthHelper,
)
from datahub.sdk.entity import Entity
from datahub.sdk.glossary_term import GlossaryTerm
from datahub.sdk.tag import Tag

logger = logging.getLogger(__name__)

GRAPH_SENSITIVITY_LABELS_URL = (
    "https://graph.microsoft.com/v1.0/security/"
    "dataSecurityAndGovernance/sensitivityLabels"
)

SensitivityLabelOutput = Literal["tag", "glossary_term"]

SENSITIVITY_LABEL_CAPABILITY = (
    "Optionally enabled via `extract_sensitivity_label`. The item sensitivity "
    "label id from GET /workspaces/{workspaceId}/items/{itemId} is resolved "
    "to a name and description with Microsoft Graph. "
    "`extract_sensitivity_label.output` chooses a tag or a glossary term, "
    "and a glossary term's description is the label description."
)


class FabricSensitivityLabelConfig(ConfigModel):
    """Ingest a Fabric item's sensitivity label as a tag or glossary term."""

    enabled: bool = Field(
        default=False,
        description=(
            "Read each item's sensitivity label id and resolve its name and "
            "description from Microsoft Graph. The Azure credential must be "
            "able to acquire a token for `https://graph.microsoft.com/.default` "
            "that can read sensitivity labels."
        ),
    )
    output: SensitivityLabelOutput = Field(
        default="tag",
        description=(
            "Where to store the label. `tag` creates a DataHub tag named after "
            "the label. `glossary_term` creates a glossary term whose id is the "
            "label GUID, whose display name is the label name, and whose "
            "description is the label description."
        ),
    )


class FabricSensitivityLabelConfigMixin(ConfigModel):
    extract_sensitivity_label: FabricSensitivityLabelConfig = Field(
        default_factory=FabricSensitivityLabelConfig,
        description=(
            "Ingest each item's sensitivity label as a DataHub tag or glossary term."
        ),
    )


@dataclass(frozen=True)
class SensitivityLabel:
    """Name and description for one sensitivity label GUID."""

    id: str
    name: str
    description: Optional[str]


def sensitivity_label_filter(label_id: str) -> str:
    """OData filter that selects one sensitivity label by id."""
    escaped = label_id.replace("'", "''")
    return f"id in ('{escaped}')"


class SensitivityLabelResolver:
    """Look up a sensitivity label id in Microsoft Graph.

    GET /security/dataSecurityAndGovernance/sensitivityLabels?$filter=id in ('<guid>')
    """

    def __init__(self, auth_helper: FabricAuthHelper, timeout: int):
        self._auth_helper = auth_helper
        self._timeout = timeout
        self._cache: dict[str, Optional[SensitivityLabel]] = {}

    def resolve(self, label_id: str) -> Optional[SensitivityLabel]:
        if label_id in self._cache:
            return self._cache[label_id]
        label = self._fetch(label_id)
        self._cache[label_id] = label
        return label

    def _fetch(self, label_id: str) -> Optional[SensitivityLabel]:
        try:
            response = requests.get(
                GRAPH_SENSITIVITY_LABELS_URL,
                params={"$filter": sensitivity_label_filter(label_id)},
                headers={
                    "Authorization": self._auth_helper.get_authorization_header(
                        scope=GRAPH_API_SCOPE
                    )
                },
                timeout=self._timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as e:
            logger.warning(f"Failed to resolve sensitivity label {label_id}: {e}")
            return None

        values = payload.get("value") if isinstance(payload, dict) else None
        if not isinstance(values, list) or not values:
            logger.warning(f"Sensitivity label {label_id} was not returned by Graph")
            return None
        entry = values[0]
        if not isinstance(entry, dict):
            return None
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            logger.warning(f"Sensitivity label {label_id} has no name")
            return None
        description = entry.get("description")
        if not isinstance(description, str):
            description = None
        resolved_id = entry.get("id")
        if not isinstance(resolved_id, str) or not resolved_id:
            resolved_id = label_id
        return SensitivityLabel(id=resolved_id, name=name, description=description)


class SensitivityLabelEmitter:
    """Emit one tag or glossary term per label, and attach it to an item."""

    def __init__(
        self,
        *,
        enabled: bool,
        output: SensitivityLabelOutput,
        auth_helper: FabricAuthHelper,
        timeout: int,
    ):
        self.enabled = enabled
        self.output = output
        self._resolver = (
            SensitivityLabelResolver(auth_helper, timeout) if enabled else None
        )
        self._emitted: set[str] = set()

    def attach(self, entity: Any, label_id: Optional[str]) -> List[Entity]:
        """Attach the label to ``entity`` and return the tag or term to emit once."""
        if not self.enabled or not label_id or self._resolver is None:
            return []
        label = self._resolver.resolve(label_id)
        if label is None:
            return []

        if self.output == "glossary_term":
            term = GlossaryTerm(
                id=label.id,
                display_name=label.name,
                definition=label.description or "",
            )
            entity.add_term(term.urn)
            created: Entity = term
        else:
            tag = Tag(name=label.name, description=label.description)
            entity.add_tag(tag.urn)
            created = tag

        if label.id in self._emitted:
            return []
        self._emitted.add(label.id)
        return [created]
