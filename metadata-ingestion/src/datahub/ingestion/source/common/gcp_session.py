from typing import List, Optional

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential


def build_authorized_session(
    credential: Optional[GCPCredential], scopes: List[str]
) -> AuthorizedSession:
    """An AuthorizedSession for a Google REST API.

    Uses the service-account key when one is configured, otherwise Application
    Default Credentials. Scopes are applied at construction, since unscoped
    service-account credentials cannot mint tokens.
    """
    if credential is None:
        credentials, _ = google.auth.default(scopes=scopes)
        return AuthorizedSession(credentials)
    return AuthorizedSession(
        service_account.Credentials.from_service_account_info(
            credential.to_dict(), scopes=scopes
        )
    )
