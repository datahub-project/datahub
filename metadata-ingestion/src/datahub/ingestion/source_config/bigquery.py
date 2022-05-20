import pydantic

from datahub.configuration.common import ConfigModel


class BigQueryBaseConfig(ConfigModel):
    requests_per_min: int = pydantic.Field(
        default=60,
        description="Used to control number of API calls made per min to GCP APIs.",
    )
