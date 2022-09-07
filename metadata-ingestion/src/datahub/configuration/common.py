import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import IO, Any, ClassVar, Dict, List, Optional, Pattern, cast

from cached_property import cached_property
from pydantic import BaseModel, Extra, validator
from pydantic.fields import Field


class ConfigModel(BaseModel):
    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True
        keep_untouched = (
            cached_property,
        )  # needed to allow cached_property to work. See https://github.com/samuelcolvin/pydantic/issues/1241 for more info.


class TransformerSemantics(Enum):
    """Describes semantics for aspect changes"""

    OVERWRITE = "OVERWRITE"  # Apply changes blindly
    PATCH = "PATCH"  # Only apply differences from what exists already on the server


class TransformerSemanticsConfigModel(ConfigModel):
    semantics: TransformerSemantics = TransformerSemantics.OVERWRITE
    replace_existing: bool = False

    @validator("semantics", pre=True)
    def ensure_semantics_is_upper_case(cls, v: str) -> str:
        if isinstance(v, str):
            return v.upper()
        return v


class DynamicTypedConfig(ConfigModel):
    type: str = Field(
        description="The type of the dynamic object",
    )
    # This config type is declared Optional[Any] here. The eventual parser for the
    # specified type is responsible for further validation.
    config: Optional[Any] = Field(
        default=None,
        description="The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19).",
    )


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
        self.info = info or {}


class ConfigurationError(MetaError):
    """A configuration error has happened"""


class SensitiveError(Exception):
    """Wraps an exception that should not be logged with variable information."""

    @classmethod
    def get_sensitive_cause(cls, exc: Exception) -> Optional[Exception]:
        """
        Returns the underlying exception if the exception is sensitive, and None otherwise.
        """

        e: Optional[Exception] = exc
        while e:
            # This cast converts BaseException to Exception.
            inner = cast(Optional[Exception], e.__cause__)

            if isinstance(e, cls):
                return inner
            e = inner
        return None


class ConfigurationMechanism(ABC):
    @abstractmethod
    def load_config(self, config_fp: IO) -> dict:
        pass


class OauthConfiguration(ConfigModel):
    provider: Optional[str] = Field(
        description="Identity provider for oauth, e.g- microsoft"
    )
    client_id: Optional[str] = Field(
        description="client id of your registered application"
    )
    scopes: Optional[List[str]] = Field(
        description="scopes required to connect to snowflake"
    )
    use_certificate: bool = Field(
        description="Do you want to use certificate and private key to authenticate using oauth",
        default=False,
    )
    client_secret: Optional[str] = Field(
        description="client secret of the application if use_certificate = false"
    )
    authority_url: Optional[str] = Field(
        description="Authority url of your identity provider"
    )
    encoded_oauth_public_key: Optional[str] = Field(
        description="base64 encoded certificate content if use_certificate = true"
    )
    encoded_oauth_private_key: Optional[str] = Field(
        description="base64 encoded private key content if use_certificate = true"
    )


class AllowDenyPattern(ConfigModel):
    """A class to store allow deny regexes"""

    # This regex is used to check if a given rule is a regex expression or a literal.
    # Note that this is not a perfect check. For example, the '.' character should
    # be considered a regex special character, but it's used frequently in literal
    # patterns and hence we allow it anyway.
    IS_SIMPLE_REGEX: ClassVar = re.compile(r"^[A-Za-z0-9 _.-]+$")

    allow: List[str] = Field(
        default=[".*"],
        description="List of regex patterns to include in ingestion",
    )
    deny: List[str] = Field(
        default=[],
        description="List of regex patterns to exclude from ingestion.",
    )
    ignoreCase: Optional[bool] = Field(
        default=True,
        description="Whether to ignore case sensitivity during pattern matching.",
    )  # Name comparisons should default to ignoring case

    @property
    def regex_flags(self) -> int:
        return re.IGNORECASE if self.ignoreCase else 0

    @classmethod
    def allow_all(cls) -> "AllowDenyPattern":
        return AllowDenyPattern()

    def allowed(self, string: str) -> bool:
        for deny_pattern in self.deny:
            if re.match(deny_pattern, string, self.regex_flags):
                return False

        return any(
            re.match(allow_pattern, string, self.regex_flags)
            for allow_pattern in self.allow
        )

    def is_fully_specified_allow_list(self) -> bool:
        """
        If the allow patterns are literals and not full regexes, then it is considered
        fully specified. This is useful if you want to convert a 'list + filter'
        pattern into a 'search for the ones that are allowed' pattern, which can be
        much more efficient in some cases.
        """
        return all(
            self.IS_SIMPLE_REGEX.match(allow_pattern) for allow_pattern in self.allow
        )

    def get_allowed_list(self) -> List[str]:
        """Return the list of allowed strings as a list, after taking into account deny patterns, if possible"""
        assert self.is_fully_specified_allow_list()
        return [a for a in self.allow if self.allowed(a)]

    def __eq__(self, other):  # type: ignore
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


class KeyValuePattern(ConfigModel):
    """A class to store allow deny regexes"""

    rules: Dict[str, List[str]] = {".*": []}
    alphabet: str = "[A-Za-z0-9 _.-]"

    @property
    def alphabet_pattern(self) -> Pattern:
        return re.compile(f"^{self.alphabet}+$")

    @classmethod
    def all(cls) -> "KeyValuePattern":
        return KeyValuePattern()

    def value(self, string: str) -> List[str]:
        return next(
            (self.rules[key] for key in self.rules.keys() if re.match(key, string)), []
        )

    def matched(self, string: str) -> bool:
        return any(re.match(key, string) for key in self.rules.keys())

    def is_fully_specified_key(self) -> bool:
        """
        If the allow patterns are literals and not full regexes, then it is considered
        fully specified. This is useful if you want to convert a 'list + filter'
        pattern into a 'search for the ones that are allowed' pattern, which can be
        much more efficient in some cases.
        """
        return any(not self.alphabet_pattern.match(key) for key in self.rules.keys())

    def get(self) -> Dict[str, List[str]]:
        """Return the list of allowed strings as a list, after taking into account deny patterns, if possible"""
        assert self.is_fully_specified_key()
        return self.rules


class VersionedConfig(ConfigModel):
    version: str = "1"
