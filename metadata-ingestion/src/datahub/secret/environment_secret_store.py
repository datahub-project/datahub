import os
from typing import Dict, List, Union

from datahub.secret.secret_store import SecretStore


# Simple SecretStore implementation that fetches Secret values from the local environment.
class EnvironmentSecretStore(SecretStore):
    def __init__(self, config):
        pass

    def close(self) -> None:
        return

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Union[str, None]]:
        values = {}
        for secret_name in secret_names:
            values[secret_name] = os.getenv(secret_name)
        return values

    def get_secret_value(self, secret_name: str) -> Union[str, None]:
        return os.getenv(secret_name)

    def get_id(self) -> str:
        return "env"

    @classmethod
    def create(cls, config: Dict) -> "EnvironmentSecretStore":
        return cls(config)
