import logging
import threading
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from google.cloud.secretmanager_v1 import SecretManagerServiceClient

from cachetools import TTLCache
from pydantic import BaseModel, field_validator

from datahub.secret.secret_store import SecretStore
from datahub.secret.secret_utils import validate_prefix

logger = logging.getLogger(__name__)


class GcpSecretManagerStoreConfig(BaseModel):
    project_id: str
    prefix: str
    cache_ttl: int

    @field_validator("prefix")
    @classmethod
    def check_prefix(cls, v: str) -> str:
        return validate_prefix(v)

    @field_validator("cache_ttl")
    @classmethod
    def check_cache_ttl(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("cache_ttl must be a positive integer")
        return v


class GcpSecretManagerStore(SecretStore):
    """SecretStore implementation that fetches secrets from GCP Secret Manager."""

    def __init__(self, config: GcpSecretManagerStoreConfig):
        self.config = config
        self._client: Optional["SecretManagerServiceClient"] = None
        self._client_lock = threading.Lock()
        self._cache: TTLCache = TTLCache(maxsize=1000, ttl=config.cache_ttl)
        self._cache_lock = threading.Lock()

        # Retry config created once — the first gRPC call to establish the
        # channel can take 10-30s ("cold channel" problem).
        from google.api_core.retry import Retry

        self._default_retry = Retry(
            initial=1.0, maximum=10.0, multiplier=2.0, deadline=30.0
        )

    def _get_client(self):
        # Double-checked locking: multiple workload threads share this store
        # instance, so we prevent duplicate client creation on first access.
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    try:
                        from google.cloud import (
                            secretmanager,
                        )
                    except ImportError as e:
                        raise ImportError(
                            "google-cloud-secret-manager is required for GCP Secret Manager support. "
                            "Install with: pip install 'acryl-datahub[gcp-secret-manager]'"
                        ) from e
                    self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def _fetch_from_gcp(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        from google.api_core.exceptions import NotFound

        client = self._get_client()
        results: Dict[str, Optional[str]] = {}
        for name in secret_names:
            secret_id = f"{self.config.prefix}{name}"
            resource = (
                f"projects/{self.config.project_id}/secrets/{secret_id}/versions/latest"
            )
            try:
                resp = client.access_secret_version(
                    request={"name": resource}, retry=self._default_retry
                )
                results[name] = resp.payload.data.decode("UTF-8")
            except NotFound:
                logger.warning(f"Secret '{name}' not found in GCP Secret Manager")
                results[name] = None
            except Exception as e:
                logger.error(
                    f"Failed to fetch secret '{name}' from GCP Secret Manager: {e}"
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
                    self._cache[k] = v

            cached.update(fresh)

        return cached

    def get_id(self) -> str:
        return "gcp-sm"

    def close(self) -> None:
        with self._client_lock:
            if self._client is not None:
                self._client.transport.close()
                self._client = None

    @classmethod
    def create(cls, configs: dict) -> "GcpSecretManagerStore":
        config = GcpSecretManagerStoreConfig.model_validate(configs)
        return cls(config)
