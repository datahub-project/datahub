from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel


class TelemetryConfig(ConfigModel):
    disable_response_time_collection: bool = Field(
        default=False,
        description="Disable response time collection",
    )
    capture_response_times_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns to include in response time collection",
    )
