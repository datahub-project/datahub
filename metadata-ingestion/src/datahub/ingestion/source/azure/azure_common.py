from typing import Optional

from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from pydantic import Field, root_validator

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError


class AdlsSourceConfig(ConfigModel):
    """
    Common Azure credentials config.

    https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python
    """

    # The following could be made optional when we add support for client_id/client_secret.  See above doc.
    account_name: str = Field(
        description="Name of the Azure storage account.  See [Microsoft official documentation on how to create a storage account.](https://docs.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)",
    )
    account_key: Optional[str] = Field(
        description="Azure storage account access key that can be used as a credential. **An account key or a SAS token needs to be provided.**",
    )
    sas_token: Optional[str] = Field(
        description="Azure storage account Shared Access Signature (SAS) token that can be used as a credential. **An account key or a SAS token needs to be provided.**",
    )
    container_name: str = Field(
        description="Azure storage account container name.",
    )
    base_path: str = Field(
        default="/",
        description="Base folder in hierarchical namespaces to start from.",
    )

    def get_abfss_url(self, folder_path: str = "") -> str:
        if not folder_path.startswith("/"):
            folder_path = f"/{folder_path}"
        return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net{folder_path}"

    def get_filesystem_client(self) -> FileSystemClient:
        return self.get_service_client().get_file_system_client(self.container_name)

    def get_service_client(self):
        return DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=self.get_credentials(),
        )

    def get_credentials(self):
        return self.sas_token if self.sas_token is not None else self.account_key

    @root_validator()
    def _check_sas_or_account_key(cls, values):
        if not values.get("account_key") and not values.get("sas_token"):
            raise ConfigurationError("either account_key or sas_token is required")
        return values
