import re
from abc import ABC, abstractmethod
from typing import IO, Any, List, Optional

from pydantic import BaseModel


class ConfigModel(BaseModel):
    class Config:
        extra = "forbid"


class DynamicTypedConfig(ConfigModel):
    type: str
    # This config type is declared Optional[Any] here. The eventual parser for the
    # specified type is responsible for further validation.
    config: Optional[Any]


class MetaError(Exception):
    """A base class for all meta exceptions"""


class PipelineExecutionError(MetaError):
    """An error occurred when executing the pipeline"""


class OperationalError(PipelineExecutionError):
    """An error occurred because of client-provided metadata"""

    message: str
    info: dict

    def __init__(self, message: str, info: dict = None):
        self.message = message
        if info:
            self.info = info
        else:
            self.info = {}


class ConfigurationError(MetaError):
    """A configuration error has happened"""


class ConfigurationMechanism(ABC):
    @abstractmethod
    def load_config(self, config_fp: IO) -> dict:
        pass


class AllowDenyPattern(ConfigModel):
    """A class to store allow deny regexes"""

    allow: List[str] = [".*"]
    deny: List[str] = []
    alphabet: str = "[A-Za-z0-9 _.-]"

    @property
    def alphabet_pattern(self):
        return re.compile(f"^{self.alphabet}+$")

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

    def is_fully_specified_allow_list(self) -> bool:
        """
        If the allow patterns are literals and not full regexes, then it is considered
        fully specified. This is useful if you want to convert a 'list + filter'
        pattern into a 'search for the ones that are allowed' pattern, which can be
        much more efficient in some cases.
        """
        for allow_pattern in self.allow:
            if not self.alphabet_pattern.match(allow_pattern):
                return False
        return True

    def get_allowed_list(self):
        """Return the list of allowed strings as a list, after taking into account deny patterns, if possible"""
        assert self.is_fully_specified_allow_list()
        return [a for a in self.allow if self.allowed(a)]
