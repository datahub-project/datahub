from pydantic import BaseModel
from typing import Optional
from .sql_common import SQLAlchemyConfig, get_sql_workunits
from gometa.ingestion.api.source import Source

class SQLServerConfig(SQLAlchemyConfig):
    #defaults
    host_port = "localhost:1433"
    scheme = "mssql+pytds"

class SQLServerSource(Source):
    def configure(self, config_dict):
        self.config = SQLServerConfig.parse_obj(config_dict)

    def get_workunits(self):
        return get_sql_workunits(self.config, "mssql")
        
    def close(self):
        pass
