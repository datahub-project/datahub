# So that SourceCapabilityModifier can be resolved at runtime
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Type

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import (
    SourceCapability as SourceCapability,
)
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier


def config_class(config_cls: Type) -> Callable[[Type], Type]:
    """Adds a get_config_class method to the decorated class"""

    def default_create(cls: Type, config_dict: Dict, ctx: PipelineContext) -> Type:
        config = config_cls.model_validate(config_dict)
        return cls(config=config, ctx=ctx)

    def wrapper(cls: Type) -> Type:
        # add a get_config_class method
        cls.get_config_class = lambda: config_cls
        if "create" not in cls.__dict__:
            # Add create() for this class using its own config_cls.
            # Uses __dict__ (not hasattr) so that subclasses with their own
            # @config_class get a create() bound to their config, even when
            # the parent already has a decorator-generated create().
            cls.create = classmethod(default_create)

            # TODO: Once we're on Python 3.10, we should call abc.update_abstractmethods here.

        return cls

    return wrapper


def platform_name(
    platform_name: str, id: Optional[str] = None, doc_order: Optional[int] = None
) -> Callable[[Type], Type]:
    """Adds a get_platform_name method to the decorated class"""

    def wrapper(cls: Type) -> Type:
        cls.get_platform_name = lambda: platform_name
        cls.get_platform_id = lambda: id or platform_name.lower().replace(" ", "-")
        cls.get_platform_doc_order = lambda: doc_order or None

        return cls

    if id and " " in id:
        raise Exception(
            f'Platform id "{id}" contains white-space, please use a platform id without spaces.'
        )

    return wrapper


class SupportStatus(Enum):
    ALPHA = 1
    """
    Alpha Sources are early-stage integrations with limited production adoption, and are typically maintained by the community, the field team, or a team outside of Ingestion. They are available for experimentation and may change without notice.
    """
    BETA = 2
    """
    Beta Sources are maintained by the DataHub Ingestion team but have limited production adoption so far. They are ready to use, but have not been exercised against a wide variety of edge-cases; we eagerly solicit feedback to strengthen them.
    """
    GA = 3
    """
    GA (Generally Available) Sources are maintained by the DataHub Ingestion team and are widely adopted in production. We expect the integration to be stable with few user-facing issues.
    """
    UNKNOWN = 0
    """
    System-default value for when the connector author has declined to provide a status on this connector.
    """

    # The values order the tiers (unknown < alpha < beta < ga) so that a platform
    # bundling several plugins can report the highest one. The pre-2026 names
    # (CERTIFIED / INCUBATING / TESTING) are deliberately absent: keeping them as
    # aliases let a stale branch silently ship a brand-new connector as GA. Legacy
    # names in previously-generated connector registries are translated on read,
    # in docgen's _LEGACY_STATUS_NAMES.

    @property
    def display_name(self) -> str:
        """Human-readable label, e.g. for docs badges and integration cards."""
        # `.title()` would render GA as "Ga", so the labels are explicit.
        return _SUPPORT_STATUS_DISPLAY_NAMES.get(self, self.name.title())


_SUPPORT_STATUS_DISPLAY_NAMES: Dict[SupportStatus, str] = {
    SupportStatus.ALPHA: "Alpha",
    SupportStatus.BETA: "Beta",
    SupportStatus.GA: "GA",
}


def support_status(
    support_status: SupportStatus,
) -> Callable[[Type], Type]:
    """Adds a get_support_status method to the decorated class"""

    def wrapper(cls: Type) -> Type:
        cls.get_support_status = lambda: support_status
        return cls

    return wrapper


@dataclass
class CapabilitySetting:
    capability: SourceCapability
    description: str
    supported: bool
    subtype_modifier: Optional[List[SourceCapabilityModifier]] = None


def capability(
    capability_name: SourceCapability,
    description: str,
    supported: bool = True,
    subtype_modifier: Optional[List[SourceCapabilityModifier]] = None,
) -> Callable[[Type], Type]:
    """
    A decorator to mark a source as having a certain capability
    """

    def wrapper(cls: Type) -> Type:
        if not hasattr(cls, "__capabilities") or any(
            # It's from this class and not a superclass.
            cls.__capabilities is getattr(base, "__capabilities", None)
            for base in cls.__bases__
        ):
            cls.__capabilities = {}

            cls.get_capabilities = lambda: cls.__capabilities.values()

            # If the superclasses have capability annotations, copy those over.
            for base in cls.__bases__:
                base_caps = getattr(base, "__capabilities", None)
                if base_caps:
                    cls.__capabilities.update(base_caps)

        cls.__capabilities[capability_name] = CapabilitySetting(
            capability=capability_name,
            description=description,
            supported=supported,
            subtype_modifier=subtype_modifier,
        )
        return cls

    return wrapper
