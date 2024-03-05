from typing import Optional, Union

from pydantic import BaseModel

from datahub_integrations.analytics.models import ConnectionModel


class S3StaticCreds(BaseModel):
    s3_access_key: str
    s3_access_secret: str
    s3_session_token: Optional[str] = None


class S3Connection(ConnectionModel):
    s3_reqion: str
    s3_creds: Optional[Union[S3StaticCreds]] = None
