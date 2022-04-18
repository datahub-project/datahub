from enum import Enum, auto
from typing import Callable, Dict, Type

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source


def config_class(config_cls: Type) -> Callable[[Type], Type]:
    """Adds a get_config_class method to the decorated class"""

    def default_create(cls: Type, config_dict: Dict, ctx: PipelineContext) -> Type:
        config = config_cls.parse_obj(config_dict)
        return cls(config, ctx)

    def wrapper(cls: Type) -> Type:
        # add a get_config_class method
        setattr(cls, "get_config_class", lambda: config_cls)
        # add the create method
        setattr(cls, "create", classmethod(default_create))
        return cls

    return wrapper


def platform_name(platform_name: str) -> Callable[[Type], Type]:
    """Adds a get_platform_name method to the decorated class"""

    def wrapper(cls: Type) -> Type:
        setattr(cls, "get_platform_name", lambda: platform_name)
        return cls

    return wrapper


class SupportStatus(Enum):
    CERTIFIED = auto()
    INCUBATING = auto()
    TESTING = auto()
    UNKNOWN = auto()


def support_status(
    support_status: SupportStatus,
) -> Callable[[Type[Source]], Type[Source]]:
    """Adds a get_support_status method to the decorated class"""

    def wrapper(cls: Type[Source]) -> Type[Source]:
        setattr(cls, "get_support_status", lambda: support_status)
        return cls

    return wrapper
