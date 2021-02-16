from abc import ABC, abstractmethod
from typing import TypeVar, Type, List
from pydantic import BaseModel, ValidationError
from pathlib import Path
import re


class ConfigModel(BaseModel):
    class Config:
        extra = "allow"


class DynamicTypedConfig(ConfigModel):
    type: str


class MetaError(Exception):
    """A base class for all meta exceptions"""


class ConfigurationError(MetaError):
    """A configuration error has happened"""


T = TypeVar("T", bound=ConfigModel)


class ConfigurationMechanism(ABC):
    @abstractmethod
    def load_config(self, cls: Type[T], config_file: Path) -> T:
        pass

class AllowDenyPattern(BaseModel):
    """ A class to store allow deny regexes"""
    allow: List[str] = [".*"]
    deny: List[str] = []

    @classmethod
    def allow_all(cls):
        return AllowDenyPattern()

    def allowed(self, string: str) -> bool:
        for deny_pattern in self.deny:
            if re.match(deny_pattern, string):
                return False

        for allow_pattern in self.allow:
            if re.match(allow_pattern, string):
                return True

        return False



class DynamicFactory:
    def __init__(self):
        self.factory = {}

    def register(self, type, cfg_cls: Type[T]):
        self.factory[type] = cfg_cls

    def load_config(self, dyn_config: DynamicTypedConfig) -> ConfigModel:
        if self.factory[dyn_config.type]:
            config_class = self.factory[dyn_config.type]
            try:
                return config_class.parse_obj(dyn_config.dict()[dyn_config.type])
            except ValidationError as e:
                messages = []
                for err in e.errors():
                    location = ".".join((str(x) for x in err["loc"]))
                    reason = err["msg"]
                    messages.append(f"  - {location}: {reason}")

                msg = "\n".join(messages)
                raise ConfigurationError(f"Invalid value in configuration : \n{msg}") from e


def generic_load_file(cls: Type[T], path: Path, loader_func) -> T:
    if not path.exists():
        return cls()

    with path.open() as f:
        try:
            config = loader_func(f)
        except ValueError as e:
            raise ConfigurationError(f'File {path} is unparseable: {e}') from e

    try:
        return cls.parse_obj(config)
    except ValidationError as e:
        messages = []
        for err in e.errors():
            location = ".".join((str(x) for x in err["loc"]))
            reason = err["msg"]
            messages.append(f"  - {location}: {reason}")

        msg = "\n".join(messages)
        raise ConfigurationError(f"Invalid value in configuration file {path}: \n{msg}") from e
