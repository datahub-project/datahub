"""Resolve a Power BI sensitivity label id into a DataHub tag or glossary term."""

import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Literal, Optional

import requests
from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import get_sys_time, make_tag_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    TagAssociationClass,
)
from datahub.sdk.entity import Entity
from datahub.sdk.glossary_term import GlossaryTerm
from datahub.sdk.tag import Tag

logger = logging.getLogger(__name__)

SensitivityLabelOutput = Literal["tag", "glossary_term"]

SENSITIVITY_LABEL_CAPABILITY = (
    "Optionally enabled via `extract_sensitivity_label`. The sensitivity label "
    "id on a dataset, report, or dashboard (`sensitivityLabel.labelId` in the "
    "workspace scan) is resolved to a name and description with Microsoft Graph. "
    "`extract_sensitivity_label.output` chooses a tag or a glossary term, and a "
    "glossary term's description is the label description. A dataset label is "
    "applied to each of its tables."
)


class PowerBiSensitivityLabelConfig(ConfigModel):
    """Ingest a Power BI artifact's sensitivity label as a tag or glossary term."""

    enabled: bool = Field(
        default=False,
        description=(
            "Read each dataset, report, and dashboard sensitivity label id from "
            "the workspace scan and resolve its name and description from "
            "Microsoft Graph. The app registration must be able to acquire a "
            "token for Microsoft Graph with application permission "
            "SensitivityLabels.Read.All."
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


class PowerBiSensitivityLabelConfigMixin(ConfigModel):
    extract_sensitivity_label: PowerBiSensitivityLabelConfig = Field(
        default_factory=PowerBiSensitivityLabelConfig,
        description=(
            "Ingest each dataset, report, and dashboard sensitivity label as a "
            "DataHub tag or glossary term."
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

    def __init__(
        self,
        *,
        session: requests.Session,
        authorization_header: Callable[[], Dict[str, str]],
        labels_url: str,
    ):
        self._session = session
        self._authorization_header = authorization_header
        self._labels_url = labels_url
        self._cache: dict[str, Optional[SensitivityLabel]] = {}

    def resolve(self, label_id: str) -> Optional[SensitivityLabel]:
        if label_id in self._cache:
            return self._cache[label_id]
        label = self._fetch(label_id)
        self._cache[label_id] = label
        return label

    def _fetch(self, label_id: str) -> Optional[SensitivityLabel]:
        try:
            response = self._session.get(
                self._labels_url,
                params={"$filter": sensitivity_label_filter(label_id)},
                headers=self._authorization_header(),
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
    """Emit one tag or glossary term per label, and the association on an entity."""

    def __init__(
        self,
        *,
        enabled: bool,
        output: SensitivityLabelOutput,
        resolver: Optional[SensitivityLabelResolver],
    ):
        self.enabled = enabled
        self.output = output
        self._resolver = resolver
        self._emitted: set[str] = set()

    @classmethod
    def disabled(cls) -> "SensitivityLabelEmitter":
        return cls(enabled=False, output="tag", resolver=None)

    def contributions(
        self,
        entity_urn: str,
        label_id: Optional[str],
        endorsement_tags: List[str],
    ) -> List[MetadataChangeProposalWrapper]:
        """Tag or term entity, once, plus the association on ``entity_urn``.

        Endorsement tags share the ``GlobalTags`` aspect with a sensitivity tag.
        A second ``GlobalTags`` aspect would replace the first. Glossary-term
        output leaves endorsements on their own ``GlobalTags`` aspect.
        """
        label = self._resolve(label_id)
        mcps: List[MetadataChangeProposalWrapper] = []
        tag_names = list(endorsement_tags)

        if label is not None and self.output == "glossary_term":
            entity = self._entity(label)
            mcps.extend(self._emit_once(label, entity))
            mcps.append(self._glossary_mcp(entity_urn, str(entity.urn)))
        elif label is not None:
            entity = self._entity(label)
            mcps.extend(self._emit_once(label, entity))
            if label.name not in tag_names:
                tag_names.append(label.name)

        if tag_names:
            mcps.append(self._global_tags_mcp(entity_urn, tag_names))
        return mcps

    def _resolve(self, label_id: Optional[str]) -> Optional[SensitivityLabel]:
        if not self.enabled or not label_id or self._resolver is None:
            return None
        return self._resolver.resolve(label_id)

    def _emit_once(
        self, label: SensitivityLabel, entity: Entity
    ) -> List[MetadataChangeProposalWrapper]:
        if label.id in self._emitted:
            return []
        self._emitted.add(label.id)
        return entity.as_mcps()

    def _entity(self, label: SensitivityLabel) -> Entity:
        if self.output == "glossary_term":
            return GlossaryTerm(
                id=label.id,
                display_name=label.name,
                definition=label.description or "",
            )
        return Tag(name=label.name, description=label.description)

    def _glossary_mcp(
        self, entity_urn: str, term_urn: str
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn=term_urn)],
                auditStamp=AuditStampClass(
                    time=get_sys_time(),
                    actor=make_user_urn("datahub"),
                ),
            ),
        )

    @staticmethod
    def _global_tags_mcp(
        entity_urn: str, tag_names: List[str]
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=GlobalTagsClass(
                tags=[
                    TagAssociationClass(tag=make_tag_urn(tag_name))
                    for tag_name in tag_names
                ]
            ),
        )
