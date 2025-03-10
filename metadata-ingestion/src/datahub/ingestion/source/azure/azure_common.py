from typing import Dict, Optional, Union

from azure.core.exceptions import ClientAuthenticationError
from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from pydantic import Field, root_validator

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError


class AzureConnectionConfig(ConfigModel):
    """
    Common Azure credentials config.

    https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python
    """

    base_path: str = Field(
        default="/",
        description="Base folder in hierarchical namespaces to start from.",
    )
    container_name: Optional[str] = Field(
        default=None,
        description="Azure storage account container name.",
    )
    account_name: Optional[str] = Field(
        default=None,
        description="Name of the Azure storage account.  See [Microsoft official documentation on how to create a storage account.](https://docs.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)",
    )
    # Authentication Options
    use_managed_identity: bool = Field(
        default=False,
        description="Whether to use Azure Managed Identity authentication.",
    )
    use_cli_auth: bool = Field(
        default=False,
        description="Whether to authenticate using the Azure CLI.",
    )
    account_key: Optional[str] = Field(
        description="Azure storage account access key.",
        default=None,
    )
    sas_token: Optional[str] = Field(
        description="Azure storage account SAS token.",
        default=None,
    )
    client_id: Optional[str] = Field(
        description="Azure client (Application) ID for service principal auth.",
        default=None,
    )
    client_secret: Optional[str] = Field(
        description="Azure client secret for service principal auth.",
        default=None,
    )
    tenant_id: Optional[str] = Field(
        description="Azure tenant ID required for service principal auth.",
        default=None,
    )

    def get_abfss_url(self, folder_path: str = "") -> str:
        if not folder_path.startswith("/"):
            folder_path = f"/{folder_path}"
        return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net{folder_path}"

    # TODO DEX-1010
    def get_filesystem_client(self) -> FileSystemClient:
        return self.get_data_lake_service_client().get_file_system_client(
            file_system=self.container_name
        )

    def get_blob_service_client(self):
        return BlobServiceClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net",
            credential=f"{self.get_credentials()}",
        )

    def get_data_lake_service_client(self) -> DataLakeServiceClient:
        return DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=f"{self.get_credentials()}",
        )

    def get_credentials(
        self,
    ) -> Union[
        AzureCliCredential,
        ClientSecretCredential,
        DefaultAzureCredential,
        ManagedIdentityCredential,
        str,
    ]:
        """Get appropriate Azure credential based on configuration"""
        try:
            if self.use_managed_identity:
                return ManagedIdentityCredential()

            elif self.use_cli_auth:
                return AzureCliCredential()

            elif self.client_id and self.client_secret and self.tenant_id:
                return ClientSecretCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                )

            elif self.account_key:
                return self.account_key

            elif self.sas_token:
                return self.sas_token

            else:
                return DefaultAzureCredential()

        except ClientAuthenticationError as e:
            raise ConfigurationError(f"Failed to initialize Azure credential: {str(e)}")

    @root_validator()
    def _check_credential_values(cls, values: Dict) -> Dict:
        """Validate that at least one valid authentication method is configured"""
        if (
            values.get("use_managed_identity")
            or values.get("use_cli_auth")
            or values.get("account_key")
            or values.get("sas_token")
            or (
                values.get("client_id")
                and values.get("client_secret")
                and values.get("tenant_id")
            )
        ):
            return values
        raise ConfigurationError(
            "Authentication configuration missing. Please provide one of: "
            "managed identity, CLI auth, account key, SAS token, or service principal credentials"
        )
