from typing import Optional

from datahub.configuration import ConfigModel


class BaseAssertion(ConfigModel):
    description: Optional[str] = None
