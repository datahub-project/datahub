from typing import Any, Dict

import pydantic

from datahub.configuration.common import ConfigModel, ConfigurationError


class CSVEnricherConfig(ConfigModel):
    filename: str = pydantic.Field(
        description="File path or URL of CSV file to ingest."
    )
    write_semantics: str = pydantic.Field(
        default="PATCH",
        description='Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE"',
    )
    delimiter: str = pydantic.Field(
        default=",", description="Delimiter to use when parsing CSV"
    )
    array_delimiter: str = pydantic.Field(
        default="|",
        description="Delimiter to use when parsing array fields (tags, terms and owners)",
    )

    @pydantic.validator("write_semantics")
    def validate_write_semantics(cls, write_semantics: str) -> str:
        if write_semantics.lower() not in {"patch", "override"}:
            raise ConfigurationError(
                "write_semantics cannot be any other value than PATCH or OVERRIDE. Default value is PATCH. "
                "For PATCH semantics consider using the datahub-rest sink or "
                "provide a datahub_api: configuration on your ingestion recipe"
            )
        return write_semantics

    @pydantic.validator("array_delimiter")
    def validator_diff(cls, array_delimiter: str, values: Dict[str, Any]) -> str:
        if array_delimiter == values["delimiter"]:
            raise ConfigurationError(
                "array_delimiter and delimiter are the same. Please choose different delimiters."
            )
        return array_delimiter
