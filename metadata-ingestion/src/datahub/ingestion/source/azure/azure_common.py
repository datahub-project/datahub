from typing import Optional, Union

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from pydantic import Field, model_validator

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError, TransparentSecretStr


class AzureConnectionConfig(ConfigModel):
    """
    Common Azure credentials config.

    Reads use ``connection_string`` when set (it takes precedence over the other
    credential fields); ``account_name`` still names the account in dataset URNs,
    so its ``AccountName`` must match the connection string.

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
    account_key: Optional[TransparentSecretStr] = Field(
        description="Azure storage account access key that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
        default=None,
    )
    sas_token: Optional[TransparentSecretStr] = Field(
        description="Azure storage account Shared Access Signature (SAS) token that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
        default=None,
    )
    client_secret: Optional[TransparentSecretStr] = Field(
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
    connection_string: Optional[TransparentSecretStr] = Field(
        description="Azure storage account connection string. When set, it takes precedence over "
        "the other credential fields and its `BlobEndpoint`/`DataLakeEndpoint` override the default "
        "`*.core.windows.net` account URLs. Primarily useful for pointing at a local storage "
        "emulator (e.g. Azurite).",
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
        if self.connection_string is not None:
            return BlobServiceClient.from_connection_string(
                self.connection_string.get_secret_value()
            )
        return BlobServiceClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net",
            credential=self.get_credentials(),
        )

    def get_data_lake_service_client(self) -> DataLakeServiceClient:
        if self.connection_string is not None:
            return DataLakeServiceClient.from_connection_string(
                self.connection_string.get_secret_value()
            )
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
                client_secret=self.client_secret.get_secret_value(),
            )
        if self.sas_token is not None:
            return self.sas_token.get_secret_value()
        if self.account_key is not None:
            return self.account_key.get_secret_value()
        return None

    @model_validator(mode="after")
    def _check_credential_values(self) -> "AzureConnectionConfig":
        if (
            self.connection_string
            or self.account_key
            or self.sas_token
            or (self.client_id and self.client_secret and self.tenant_id)
        ):
            return self
        raise ConfigurationError(
            "credentials missing, requires one combination of connection_string or account_key or sas_token or (client_id and client_secret and tenant_id)"
        )

    @model_validator(mode="after")
    def _check_connection_string_account(self) -> "AzureConnectionConfig":
        # Reads use the connection string's AccountName, but dataset URNs are
        # derived from account_name / the path. If they disagree, ingestion would
        # read one account while naming another — fail fast instead.
        if self.connection_string is None:
            return self
        # Bounded split: base64 account keys contain '=' padding.
        parts = dict(
            segment.split("=", 1)
            for segment in self.connection_string.get_secret_value().split(";")
            if "=" in segment
        )
        cs_account = parts.get("AccountName")
        if cs_account and cs_account != self.account_name:
            raise ConfigurationError(
                f"account_name '{self.account_name}' does not match AccountName "
                f"'{cs_account}' in connection_string; dataset URNs are derived from "
                "account_name and would name an account that was never read."
            )
        return self
