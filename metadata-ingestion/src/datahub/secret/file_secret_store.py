import logging
import os
from typing import Any, Dict, List, Union

from pydantic import BaseModel

from datahub.secret.secret_store import SecretStore

logger = logging.getLogger(__name__)


class FileSecretStoreConfig(BaseModel):
    basedir: str = "/mnt/secrets"
    max_length: int = 1024768


# Simple SecretStore implementation that fetches Secret values from the local files.
class FileSecretStore(SecretStore):
    def __init__(self, config: FileSecretStoreConfig):
        self.config = config

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Union[str, None]]:
        values = {}
        for secret_name in secret_names:
            values[secret_name] = self.get_secret_value(secret_name)
        return values

    def get_secret_value(self, secret_name: str) -> Union[str, None]:
        secret_path = os.path.join(self.config.basedir, secret_name)
        if os.path.exists(secret_path):
            with open(secret_path, "r") as f:
                secret_value = f.read(self.config.max_length + 1)
                if len(secret_value) > self.config.max_length:
                    logger.warning(
                        f"Secret {secret_name} is longer than {self.config.max_length} and will be truncated."
                    )
                return secret_value[: self.config.max_length].rstrip()
        return None

    def get_id(self) -> str:
        return "file"

    def close(self) -> None:
        return

    @classmethod
    def create(cls, config: Any) -> "FileSecretStore":
        config = FileSecretStoreConfig.model_validate(config)
        return cls(config)
