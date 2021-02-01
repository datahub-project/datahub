from .sql_common import get_sql_workunits

class MySqlConfig(BaseModel):
    username: Optional[str] = "datahub"
    password: Optional[str] = "datahub"
    host_port: Optional[str] = "localhost:3306"
    options: Optional[dict]

class MySqlSource(Source):
    def configure(self, config_dict):
        self.config = MySqlConfig.parse_obj(config_dict)
        scheme = "mysql+pymysql"
        self.sql_alchemy_url = f'{scheme}://{self.config.username}:{self.config.password}@{self.config.host_port}'

    def get_workunits(self):
        get_sql_workunits(self.sql_alchemy_url, self.config.options, "mysql")
        
