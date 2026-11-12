"""Client-side view of a server's entity & aspect specs, for capability detection."""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, TypedDict, cast

logger = logging.getLogger(__name__)


# Shapes returned by the registry specifications API
# (/openapi/v1/registry/models/entity/specifications). total=False because the
# parser reads every key with .get() and tolerates its absence -- notably
# older servers don't emit schemaVersion. These type the contract at the
# parsing boundary so mypy catches structural mismatches.
class _AspectAnnotation(TypedDict, total=False):
    name: str
    schemaVersion: int


class _AspectSpec(TypedDict, total=False):
    aspectAnnotation: _AspectAnnotation


class _EntityElement(TypedDict, total=False):
    name: str
    keyAspectName: str
    keyAspectSpec: _AspectSpec
    aspectSpecs: List[_AspectSpec]


@dataclass
class EntityAspectSpecs:
    """A parsed, queryable view of a server's entity & aspect specifications."""

    # entityType -> set of supported aspectNames
    entity_aspects: Dict[str, Set[str]] = field(default_factory=dict)
    # aspectName -> schemaVersion. Empty on servers that don't expose it yet.
    aspect_schema_versions: Dict[str, int] = field(default_factory=dict)

    def _all_aspects(self) -> Set[str]:
        # Not aspect_schema_versions.keys(): older servers register aspects
        # without advertising a schemaVersion, so that would be a strict subset.
        return (
            set().union(*self.entity_aspects.values()) if self.entity_aspects else set()
        )

    def supports(self, entity_type: str, aspect_name: str) -> bool:
        """Whether the server registers ``aspect_name`` on ``entity_type``."""
        aspects = self.entity_aspects.get(entity_type)
        if aspects is None:
            raise ValueError(
                f"Entity type {entity_type!r} is not registered on the server"
            )
        return aspect_name in aspects

    def schema_version(self, aspect_name: str) -> Optional[int]:
        """The ``schemaVersion`` the server advertises for ``aspect_name``, if any."""
        if aspect_name not in self._all_aspects():
            raise ValueError(f"Aspect {aspect_name!r} is not registered on the server")
        return self.aspect_schema_versions.get(aspect_name)

    @classmethod
    def from_registry_api_elements(
        cls, elements: Iterable[dict]
    ) -> "EntityAspectSpecs":
        """Build specs from the ``elements`` of the registry specifications API."""
        specs = cls()
        for entity in elements:
            # Narrow raw JSON to the documented contract at this boundary.
            specs._ingest_entity(cast(_EntityElement, entity))
        return specs

    def _ingest_entity(self, entity: _EntityElement) -> None:
        name = entity.get("name")
        if not name:
            return
        aspects = self.entity_aspects.setdefault(name, set())

        key_aspect = entity.get("keyAspectName")
        if key_aspect:
            aspects.add(key_aspect)
        self._ingest_aspect_spec(entity.get("keyAspectSpec"))

        for aspect_spec in entity.get("aspectSpecs", []):
            annotation = (aspect_spec or {}).get("aspectAnnotation") or {}
            aspect_name = annotation.get("name")
            if aspect_name:
                aspects.add(aspect_name)
            self._ingest_aspect_spec(aspect_spec)

    def _ingest_aspect_spec(self, aspect_spec: Optional[_AspectSpec]) -> None:
        if not aspect_spec:
            return
        annotation = aspect_spec.get("aspectAnnotation") or {}
        name = annotation.get("name")
        if not name:
            return
        version = annotation.get("schemaVersion")
        if version is not None:
            self.aspect_schema_versions[name] = int(version)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-friendly dict (sets become sorted lists)."""
        return {
            "entity_aspects": {k: sorted(v) for k, v in self.entity_aspects.items()},
            "aspect_schema_versions": self.aspect_schema_versions,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EntityAspectSpecs":
        """Inverse of :meth:`to_dict`."""
        return cls(
            entity_aspects={k: set(v) for k, v in data["entity_aspects"].items()},
            aspect_schema_versions={
                k: int(v) for k, v in (data.get("aspect_schema_versions") or {}).items()
            },
        )
