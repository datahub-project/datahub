from pydantic import BaseModel
from typing import Optional
from .sql_common import SQLAlchemyConfig, get_sql_workunits
from gometa.ingestion.api.source import Source

class SQLServerConfig(SQLAlchemyConfig):
    #defaults
    host_port = "localhost:1433"
    scheme = "mssql+pytds"

class SQLServerSource(Source):

    def __init__(self, config, ctx):
        super().__init__(ctx)
        self.config = config


    @classmethod
    def create(cls, config_dict, ctx):
        config = SQLServerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self):
        return get_sql_workunits(self.config, "mssql")
        
    def close(self):
        pass
