from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Dict, Optional, Type

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceCapability


def config_class(config_cls: Type) -> Callable[[Type], Type]:
    """Adds a get_config_class method to the decorated class"""

    def default_create(cls: Type, config_dict: Dict, ctx: PipelineContext) -> Type:
        config = config_cls.parse_obj(config_dict)
        return cls(config=config, ctx=ctx)

    def wrapper(cls: Type) -> Type:
        # add a get_config_class method
        cls.get_config_class = lambda: config_cls
        if not hasattr(cls, "create") or (
            cls.create.__func__ == Source.create.__func__  # type: ignore
        ):
            # add the create method only if it has not been overridden from the base Source.create method
            cls.create = classmethod(default_create)

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
    CERTIFIED = auto()
    """
    Certified Sources are well-tested & widely-adopted by the DataHub Community. We expect the integration to be stable with few user-facing issues.
    """
    INCUBATING = auto()
    """
    Incubating Sources are ready for DataHub Community adoption but have not been tested for a wide variety of edge-cases. We eagerly solicit feedback from the Community to strengthen the connector; minor version changes may arise in future releases.
    """
    TESTING = auto()
    """
    Testing Sources are available for experimentation by DataHub Community members, but may change without notice.
    """
    UNKNOWN = auto()
    """
    System-default value for when the connector author has declined to provide a status on this connector.
    """


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


def capability(
    capability_name: SourceCapability, description: str, supported: bool = True
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
            capability=capability_name, description=description, supported=supported
        )
        return cls

    return wrapper
