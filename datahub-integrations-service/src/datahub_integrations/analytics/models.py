from pydantic import BaseModel, Extra


class ConnectionModel(BaseModel):
    """Represents the config associated with a connection"""

    class Config:
        extra = Extra.allow
        underscore_attrs_are_private = True
