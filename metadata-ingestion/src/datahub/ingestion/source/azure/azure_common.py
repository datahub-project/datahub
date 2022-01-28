from datahub.configuration import ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV

from azure.storage.filedatalake import FileSystemClient, DataLakeServiceClient

from pydantic import validator
from typing import Optional

class AdlsSourceConfig(ConfigModel):
    """
    Common Azure credentials config.

    https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python
    """

    env: str = DEFAULT_ENV
    # The following could be made optional when we add support for client_id/client_secret.  See above doc.
    account_name: str
    account_key: Optional[str]
    sas_token: Optional[str]
    container_name: str

    def get_abfss_url(self) -> str:
        return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net"

    def get_filesystem_client(self) -> FileSystemClient:
        return self.get_service_client().get_file_system_client(self.container_name)

    def get_service_client(self):
        return DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format("https", self.account_name), 
            credential=self.getCredentials())
    
    def getCredentials(self):
        return self.sas_token if (self.sas_token is not None) else self.account_key

    @validator('sas_token')
    def check_sas_or_account_key(cls, sas_token, values):
        if 'account_key' not in values and not sas_token:
            raise ValueError('either account_key or sas_token is required')
        return sas_token
