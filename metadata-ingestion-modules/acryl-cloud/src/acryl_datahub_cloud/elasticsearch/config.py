import os
from typing import Optional

import pydantic

from datahub.configuration import ConfigModel


class ElasticSearchClientConfig(ConfigModel):
    host: str = os.getenv("ELASTICSEARCH_HOST", "localhost")
    port: int = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
    use_ssl: bool = os.getenv("ELASTICSEARCH_USE_SSL", "").lower() == "true"
    verify_certs: bool = False
    ca_certs: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    username: Optional[str] = os.getenv("ELASTICSEARCH_USERNAME", "admin")
    password: Optional[str] = os.getenv("ELASTICSEARCH_PASSWORD", "admin")
    index_prefix: str = os.getenv("INDEX_PREFIX", "")
    opensearch_dialect: bool = False

    @pydantic.validator("index_prefix", always=True)
    def index_prefix_must_end_with_underscore_if_not_empty(cls, v: str) -> str:
        if v and not v.endswith("_"):
            return f"{v}_"
        return v

    @property
    def endpoint(self) -> str:
        if self.host and not self.port:
            return f"{self.host}"
        elif self.host:
            return f"{self.host}:{self.port}"
        else:
            raise ValueError("ElasticSearch host must be provided.")
