from typing import Dict, Optional

import pydantic
from pydantic import field_validator

from datahub.configuration.common import ConfigModel


class CSVEnricherConfig(ConfigModel):
    filename: str = pydantic.Field(
        description="File path or URL of CSV file to ingest."
    )
    write_semantics: str = pydantic.Field(
        default="PATCH",
        description='Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE". NOTE: this will apply to all metadata for the entity, not just a single aspect.',
    )
    delimiter: str = pydantic.Field(
        default=",", description="Delimiter to use when parsing CSV"
    )
    array_delimiter: str = pydantic.Field(
        default="|",
        description="Delimiter to use when parsing array fields (tags, terms and owners)",
    )
    structured_properties: Optional[Dict[str, str]] = pydantic.Field(
        default=None,
        description=(
            "Explicit mapping from CSV column names to structured property ids or URNs. "
            "Example: {'classification': 'urn:li:structuredProperty:io.acryl.privacy.classification'}"
        ),
    )

    @field_validator("write_semantics", mode="after")
    @classmethod
    def validate_write_semantics(cls, write_semantics: str) -> str:
        if write_semantics.lower() not in {"patch", "override"}:
            raise ValueError(
                "write_semantics cannot be any other value than PATCH or OVERRIDE. Default value is PATCH. "
                "For PATCH semantics consider using the datahub-rest sink or "
                "provide a datahub_api: configuration on your ingestion recipe"
            )
        return write_semantics

    @field_validator("array_delimiter", mode="after")
    @classmethod
    def validator_diff(cls, array_delimiter: str, info: pydantic.ValidationInfo) -> str:
        if array_delimiter == info.data["delimiter"]:
            raise ValueError(
                "array_delimiter and delimiter are the same. Please choose different delimiters."
            )
        return array_delimiter

    @field_validator("structured_properties", mode="after")
    @classmethod
    def validate_structured_properties(
        cls, structured_properties: Optional[Dict[str, str]]
    ) -> Optional[Dict[str, str]]:
        if not structured_properties:
            return structured_properties

        for column_name, property_name_or_urn in structured_properties.items():
            if not column_name or not column_name.strip():
                raise ValueError(
                    "structured_properties mapping contains an empty CSV column name."
                )
            if not property_name_or_urn or not property_name_or_urn.strip():
                raise ValueError(
                    f"structured_properties mapping for column '{column_name}' has an empty property name/URN."
                )

        return structured_properties
