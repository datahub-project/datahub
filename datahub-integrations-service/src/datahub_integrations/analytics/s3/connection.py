from typing import Optional

from datahub.configuration.common import ConnectionModel
from pydantic import BaseModel


class S3StaticCreds(BaseModel):
    s3_access_key: str
    s3_access_secret: str
    s3_session_token: Optional[str] = None


class S3Connection(ConnectionModel):
    s3_reqion: str
    s3_creds: Optional[S3StaticCreds] = None
