from typing import Dict, Optional

from pydantic import Field, root_validator

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError


class AdlsSourceConfig(ConfigModel):
    """
    Common Azure credentials config.

    https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python
    """

    base_path: str = Field(
        default="/",
        description="Base folder in hierarchical namespaces to start from.",
    )
    container_name: str = Field(
        description="Azure storage account container name.",
    )
    account_name: str = Field(
        description="Name of the Azure storage account.  See [Microsoft official documentation on how to create a storage account.](https://docs.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)",
    )
    account_key: Optional[str] = Field(
        description="Azure storage account access key that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
        default=None,
    )
    sas_token: Optional[str] = Field(
        description="Azure storage account Shared Access Signature (SAS) token that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
        default=None,
    )
    client_secret: Optional[str] = Field(
        description="Azure client secret that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
        default=None,
    )
    client_id: Optional[str] = Field(
        description="Azure client (Application) ID required when a `client_secret` is used as a credential.",
        default=None,
    )
    tenant_id: Optional[str] = Field(
        description="Azure tenant (Directory) ID required when a `client_secret` is used as a credential.",
        default=None,
    )

    def get_abfss_url(self, folder_path: str = "") -> str:
        return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net{strltrim(folder_path, self.container_name)}"

    @root_validator()
    def _check_credential_values(cls, values: Dict) -> Dict:
        if (
            values.get("account_key")
            or values.get("sas_token")
            or (
                values.get("client_id")
                and values.get("client_secret")
                and values.get("tenant_id")
            )
        ):
            return values
        raise ConfigurationError(
            "credentials missing, requires one combination of account_key or sas_token or (client_id and client_secret and tenant_id)"
        )


def strltrim(to_trim: str, prefix: str) -> str:
    return to_trim[len(prefix) :] if to_trim.startswith(prefix) else to_trim
