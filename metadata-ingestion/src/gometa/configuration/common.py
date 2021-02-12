from abc import ABC, abstractmethod
from typing import TypeVar, Type, List, IO
from pydantic import BaseModel, ValidationError
from pathlib import Path
from contextlib import contextmanager
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


class ConfigurationMechanism(ABC):
    @abstractmethod
    def load_config(self, config_fp: IO) -> dict:
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


@contextmanager
def nicely_formatted_validation_errors():
    try:
        yield
    except ValidationError as e:
        messages = []
        for err in e.errors():
            location = ".".join((str(x) for x in err["loc"]))
            reason = err["msg"]
            messages.append(f"  - {location}: {reason}")

        msg = "\n".join(messages)
        raise ConfigurationError(f"Invalid value in configuration: \n{msg}") from e
