import base64
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import msal
import requests
from OpenSSL.crypto import FILETYPE_PEM, load_certificate
from pydantic.types import SecretStr

from datahub.ingestion.source.snowflake.oauth_config import OAuthIdentityProvider

logger = logging.getLogger(__name__)


OKTA_SCOPE_DELIMITER = ","


@dataclass
class OAuthTokenGenerator:
    client_id: str
    authority_url: str
    provider: OAuthIdentityProvider
    username: Optional[str] = None
    password: Optional[SecretStr] = None

    def _get_token(
        self,
        credentials: Union[str, Dict[str, Any]],
        scopes: Optional[List[str]],
        check_cache: bool,
    ) -> dict:
        if self.provider == OAuthIdentityProvider.MICROSOFT:
            return self._get_microsoft_token(credentials, scopes, check_cache)
        elif self.provider == OAuthIdentityProvider.OKTA:
            assert isinstance(credentials, str)
            assert scopes is not None
            return self._get_okta_token(credentials, scopes)
        else:
            raise Exception(f"Unknown oauth provider: {self.provider}")

    def _get_microsoft_token(self, credentials, scopes, check_cache):
        app = msal.ConfidentialClientApplication(
            self.client_id, authority=self.authority_url, client_credential=credentials
        )
        _token = None
        if check_cache:
            _token = app.acquire_token_silent(scopes=scopes, account=None)

        if not _token:
            _token = app.acquire_token_for_client(scopes=scopes)

        return _token

    def _get_okta_token(self, credentials: str, scopes: List[str]) -> dict:
        data = {
            "grant_type": "client_credentials",
            "scope": OKTA_SCOPE_DELIMITER.join(scopes),
        }
        if self.username and self.password:
            data["grant_type"] = "password"
            data["username"] = self.username
            data["password"] = self.password.get_secret_value()

        resp = requests.post(
            self.authority_url,
            headers={"Accept": "application/json"},
            auth=(self.client_id, credentials),
            data=data,
        )
        return resp.json()

    def get_public_certificate_thumbprint(self, public_cert_str: str) -> str:
        cert_str = public_cert_str
        certificate = load_certificate(FILETYPE_PEM, cert_str.encode("utf-8"))
        return (certificate.digest("sha1")).decode("utf-8").replace(":", "")

    def get_token_with_certificate(
        self,
        private_key_content: str,
        public_key_content: str,
        scopes: Optional[List[str]],
        check_cache: bool = False,
    ) -> Any:
        decoded_private_key_content = base64.b64decode(private_key_content)
        decoded_public_key_content = base64.b64decode(public_key_content)
        public_cert_thumbprint = self.get_public_certificate_thumbprint(
            str(decoded_public_key_content, "UTF-8")
        )

        CLIENT_CREDENTIAL = {
            "thumbprint": public_cert_thumbprint,
            "private_key": decoded_private_key_content,
        }
        return self._get_token(CLIENT_CREDENTIAL, scopes, check_cache)

    def get_token_with_secret(
        self, secret: str, scopes: Optional[List[str]], check_cache: bool = False
    ) -> Any:
        return self._get_token(secret, scopes, check_cache)
