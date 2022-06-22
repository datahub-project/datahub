import pydantic

from datahub.configuration.common import ConfigModel


class CSVEnricherConfig(ConfigModel):
    filename: str = pydantic.Field(description="Path to ingestion CSV file")
    should_overwrite: bool = pydantic.Field(
        default=False,
        description="Whether the ingestion should overwrite. Otherwise, we will append data.",
    )
    delimiter: str = pydantic.Field(
        default=",", description="Delimiter to use when parsing CSV"
    )
    array_delimiter: str = pydantic.Field(
        default="|",
        description="Delimiter to use when parsing array fields (tags, terms, owners)",
    )
