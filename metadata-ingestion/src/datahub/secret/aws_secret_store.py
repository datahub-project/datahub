import logging
import threading
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from botocore.client import BaseClient

from cachetools import TTLCache
from pydantic import BaseModel, field_validator

from datahub.secret.secret_store import SecretStore
from datahub.secret.secret_utils import validate_prefix

logger = logging.getLogger(__name__)

# AWS BatchGetSecretValue supports up to 20 secrets per call.
_AWS_BATCH_SIZE = 20


class AwsSecretsManagerStoreConfig(BaseModel):
    region: str
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


class AwsSecretsManagerStore(SecretStore):
    """SecretStore implementation that fetches secrets from AWS Secrets Manager."""

    def __init__(self, config: AwsSecretsManagerStoreConfig):
        self.config = config
        self._client: Optional["BaseClient"] = None
        self._client_lock = threading.Lock()
        self._cache: TTLCache = TTLCache(maxsize=1000, ttl=config.cache_ttl)
        self._cache_lock = threading.Lock()

    def _get_client(self):
        # Double-checked locking: multiple workload threads share this store
        # instance, so we prevent duplicate client creation on first access.
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    try:
                        import boto3
                    except ImportError as e:
                        raise ImportError(
                            "boto3 is required for AWS Secrets Manager support. "
                            "Install with: pip install 'acryl-datahub[aws-secret-manager]'"
                        ) from e
                    self._client = boto3.client(
                        "secretsmanager", region_name=self.config.region
                    )
        return self._client

    def _fetch_from_aws(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        client = self._get_client()
        results: Dict[str, Optional[str]] = {}

        prefixed_ids = [f"{self.config.prefix}{name}" for name in secret_names]

        for i in range(0, len(prefixed_ids), _AWS_BATCH_SIZE):
            batch_ids = prefixed_ids[i : i + _AWS_BATCH_SIZE]
            batch_names = secret_names[i : i + _AWS_BATCH_SIZE]

            try:
                response = client.batch_get_secret_value(SecretIdList=batch_ids)

                found = {}
                for secret in response.get("SecretValues", []):
                    found[secret["Name"]] = secret.get("SecretString")

                for name, prefixed_id in zip(batch_names, batch_ids, strict=False):
                    results[name] = found.get(prefixed_id)

                for error in response.get("Errors", []):
                    if error["ErrorCode"] == "ResourceNotFoundException":
                        logger.warning(
                            f"Secret '{error['SecretId']}' not found in AWS Secrets Manager"
                        )
                    else:
                        logger.warning(
                            f"AWS Secrets Manager error for '{error['SecretId']}': "
                            f"{error['ErrorCode']} - {error['Message']}"
                        )

            except Exception as e:
                logger.error(
                    f"Failed to batch fetch secrets from AWS Secrets Manager: {e}"
                )
                for name in batch_names:
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
            fresh = self._fetch_from_aws(to_fetch)

            with self._cache_lock:
                for k, v in fresh.items():
                    self._cache[k] = v

            cached.update(fresh)

        return cached

    def get_id(self) -> str:
        return "aws-sm"

    def close(self) -> None:
        pass

    @classmethod
    def create(cls, configs: dict) -> "AwsSecretsManagerStore":
        config = AwsSecretsManagerStoreConfig.model_validate(configs)
        return cls(config)
