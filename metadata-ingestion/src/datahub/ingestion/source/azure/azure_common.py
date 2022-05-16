from typing import Dict, Optional, Union

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
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
    )
    sas_token: Optional[str] = Field(
        description="Azure storage account Shared Access Signature (SAS) token that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
    )
    client_secret: Optional[str] = Field(
        description="Azure client secret that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
    )
    client_id: Optional[str] = Field(
        description="Azure client (Application) ID required when a `client_secret` is used as a credential.",
    )
    tenant_id: Optional[str] = Field(
        description="Azure tenant (Directory) ID required when a `client_secret` is used as a credential.",
    )

    def get_abfss_url(self, folder_path: str = "") -> str:
        if not folder_path.startswith("/"):
            folder_path = f"/{folder_path}"
        return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net{folder_path}"

    def get_filesystem_client(self) -> FileSystemClient:
        return self.get_service_client().get_file_system_client(self.container_name)

    def get_service_client(self) -> DataLakeServiceClient:
        return DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=self.get_credentials(),
        )

    def get_credentials(
        self,
    ) -> Union[Optional[str], ClientSecretCredential]:
        if self.client_id and self.client_secret and self.tenant_id:
            return ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        return self.sas_token if self.sas_token is not None else self.account_key

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
