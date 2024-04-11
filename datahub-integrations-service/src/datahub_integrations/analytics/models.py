from datahub.configuration.pydantic_migration_helpers import PYDANTIC_VERSION_2
from pydantic import BaseModel, Extra


class ConnectionModel(BaseModel):
    """Represents the config associated with a connection"""

    class Config:
        if PYDANTIC_VERSION_2:
            extra = "forbid"
        else:
            extra = Extra.forbid
            # underscore_attrs_are_private = True
