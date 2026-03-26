from typing import Optional

from datahub.configuration.common import ConfigModel


class BaseAssertion(ConfigModel):
    description: Optional[str] = None
