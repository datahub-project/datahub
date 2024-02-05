from typing import Optional

from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel


class BaseAssertion(v1_ConfigModel):
    description: Optional[str] = None
