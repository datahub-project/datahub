from typing import Optional

from pydantic.fields import Field

from datahub.configuration import ConfigModel


class DBTTargetPlatformMixin(ConfigModel):
    """
    Target platform config
    """

    target_platform: str = Field(
        description="The platform that dbt is loading onto. (e.g. bigquery / redshift / postgres etc.)",
    )
    target_platform_instance: Optional[str] = Field(
        default=None,
        description="The platform instance for the platform that dbt is operating on. Use this if you have multiple instances of the same platform (e.g. redshift) and need to distinguish between them.",
    )
