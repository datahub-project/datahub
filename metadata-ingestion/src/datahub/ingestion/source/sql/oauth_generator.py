import base64
import logging
from typing import Any, Dict, List, Union

import msal
from OpenSSL.crypto import FILETYPE_PEM, load_certificate

log = logging.getLogger(__name__)


class OauthTokenGenerator:
    def __init__(self, client_id, authority_url):
        self.client_id = client_id
        self.authority_url = authority_url

    def _get_token(
        self,
        credentials: Union[str, Dict[str, Any]],
        scope: Union[str, List[str]],
        check_cache: bool,
    ) -> str:
        app = msal.ConfidentialClientApplication(
            self.client_id, authority=self.authority_url, client_credential=credentials
        )
        token = None
        if isinstance(scope, str):
            scope = [scope]
        if check_cache:
            token = app.acquire_token_silent(scopes=scope, account=None)

        if not token:
            log.debug("No token available in cache. Getting a new one from AAD.")
            token = app.acquire_token_for_client(scopes=scope)
        return token

    def get_public_certificate_thumbprint(self, public_cert_str: str) -> str:
        cert_str = public_cert_str
        certificate = load_certificate(FILETYPE_PEM, cert_str.encode("utf-8"))
        return (certificate.digest("sha1")).decode("utf-8").replace(":", "")

    def get_token_with_certificate(
        self,
        private_key_content: str,
        public_key_content: str,
        scope: Union[str, List[str]] = "https://management.core.windows.net/.default",
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
        return self._get_token(CLIENT_CREDENTIAL, scope, check_cache)

    def get_token_with_secret(
        self, secret: str, scope: Union[str, List[str]], check_cache: bool = False
    ) -> Any:
        return self._get_token(secret, scope, check_cache)
