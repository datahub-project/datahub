import logging
import threading
from typing import Any, Dict, List, Optional

from cachetools import TTLCache
from google.api_core.retry import Retry
from google.cloud import secretmanager
from pydantic import BaseModel, field_validator

from datahub.secret.secret_store import SecretStore
from datahub.secret.secret_utils import validate_prefix

logger = logging.getLogger(__name__)

# Retry config for GCP Secret Manager.
# The first gRPC call to establish the channel can take 10-30s ("cold channel" problem).
_DEFAULT_RETRY = Retry(initial=1.0, maximum=10.0, multiplier=2.0, deadline=30.0)


class GcpSecretManagerStoreConfig(BaseModel):
    project_id: str
    prefix: str
    cache_ttl: int

    @field_validator("prefix")
    @classmethod
    def check_prefix(cls, v: str) -> str:
        return validate_prefix(v)


class GcpSecretManagerStore(SecretStore):
    """SecretStore implementation that fetches secrets from GCP Secret Manager."""

    def __init__(self, config: GcpSecretManagerStoreConfig):
        self.config = config
        self._client = None
        self._cache: TTLCache = TTLCache(maxsize=1000, ttl=config.cache_ttl)
        self._cache_lock = threading.Lock()

    def _get_client(self) -> secretmanager.SecretManagerServiceClient:
        if self._client is None:
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def _fetch_from_gcp(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        client = self._get_client()
        results: Dict[str, Optional[str]] = {}
        for name in secret_names:
            secret_id = f"{self.config.prefix}{name}"
            resource = (
                f"projects/{self.config.project_id}/secrets/{secret_id}/versions/latest"
            )
            try:
                resp = client.access_secret_version(
                    request={"name": resource}, retry=_DEFAULT_RETRY
                )
                results[name] = resp.payload.data.decode("UTF-8")
            except Exception:
                logger.exception(
                    f"Failed to fetch secret '{name}' from GCP Secret Manager"
                )
                results[name] = None
        return results

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        if not secret_names:
            return {}

        cached: Dict[str, Optional[str]] = {}
        to_fetch: List[str] = []

        with self._cache_lock:
            for name in secret_names:
                if name in self._cache:
                    cached[name] = self._cache[name]
                else:
                    to_fetch.append(name)

        if to_fetch:
            fresh = self._fetch_from_gcp(to_fetch)

            with self._cache_lock:
                for k, v in fresh.items():
                    if v is not None:
                        self._cache[k] = v

            cached.update(fresh)

        return cached

    def get_id(self) -> str:
        return "gcp-sm"

    def close(self) -> None:
        pass

    @classmethod
    def create(cls, config: Any) -> "GcpSecretManagerStore":
        config = GcpSecretManagerStoreConfig.model_validate(config)
        return cls(config)
