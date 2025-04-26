from typing import Dict, Optional

from pydantic import Field

from datahub.configuration.source_common import EnvConfigMixin
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential


class VertexAIConfig(EnvConfigMixin):
    credential: Optional[GCPCredential] = Field(
        default=None, description="GCP credential information"
    )
    project_id: str = Field(description=("Project ID in Google Cloud Platform"))
    region: str = Field(
        description=("Region of your project in Google Cloud Platform"),
    )
    bucket_uri: Optional[str] = Field(
        default=None,
        description=("Bucket URI used in your project"),
    )
    vertexai_url: Optional[str] = Field(
        default="https://console.cloud.google.com/vertex-ai",
        description=("VertexUI URI"),
    )

    def get_credentials(self) -> Optional[Dict[str, str]]:
        if self.credential:
            return self.credential.to_dict(self.project_id)
        return None
