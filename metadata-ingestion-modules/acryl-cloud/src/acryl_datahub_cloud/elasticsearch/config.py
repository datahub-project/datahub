import os
from typing import Optional

from datahub.configuration import ConfigModel


class ElasticSearchClientConfig(ConfigModel):
    host: str = os.getenv("ELASTICSEARCH_HOST", "localhost")
    port: int = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
    use_ssl: bool = bool(os.getenv("ELASTICSEARCH_USE_SSL", ""))
    verify_certs: bool = False
    ca_certs: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    username: Optional[str] = os.getenv("ELASTICSEARCH_USERNAME", "admin")
    password: Optional[str] = os.getenv("ELASTICSEARCH_PASSWORD", "admin")
    index_prefix: str = os.getenv("INDEX_PREFIX", "")

    @property
    def endpoint(self) -> str:
        if self.host and not self.port:
            return f"{self.host}"
        elif self.host:
            return f"{self.host}:{self.port}"
        else:
            raise ValueError("ElasticSearch host must be provided.")
