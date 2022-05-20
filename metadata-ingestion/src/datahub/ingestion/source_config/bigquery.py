import pydantic

from datahub.configuration.common import ConfigModel


class BigQueryBaseConfig(ConfigModel):
    rate_limit: bool = pydantic.Field(
        default=False, description="Should we rate limit reqeusts made to API."
    )
    requests_per_min: int = pydantic.Field(
        default=60,
        description="Used to control number of API calls made per min. Only used when `rate_limit` is set to `True`.",
    )
