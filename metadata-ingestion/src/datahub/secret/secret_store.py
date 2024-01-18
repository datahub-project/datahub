from abc import abstractmethod
from typing import Dict, List, Optional

from datahub.configuration.common import ConfigModel


class SecretStoreConfig(ConfigModel):
    type: str
    config: Dict


class SecretStore:
    """
    Abstract base class for a Secret Store, or a class that resolves "secret" values by name.
    """

    @classmethod
    @abstractmethod
    def create(cls, configs: dict) -> "SecretStore":
        pass

    @abstractmethod
    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        """
        Attempt to fetch a group of secrets, returning a Dictionary of the secret of None if one
        cannot be resolved by the store.
        """

    def get_secret_value(self, secret_name: str) -> Optional[str]:
        secret_value_dict = self.get_secret_values([secret_name])
        return secret_value_dict.get(secret_name)

    @abstractmethod
    def get_id(self) -> str:
        """
        Get a unique name or id associated with the Secret Store.
        """

    @abstractmethod
    def close(self) -> None:
        """
        Wraps up the task
        """
