import base64
import logging
from typing import Any, Dict, List, Union

import msal
from OpenSSL.crypto import FILETYPE_PEM, load_certificate

logger = logging.getLogger(__name__)


class OauthTokenGenerator:
    def __init__(self, client_id, authority_url, provider):
        self.client_id = client_id
        self.authority_url = authority_url
        self.provider = provider

    def _get_token(
        self,
        credentials: Union[str, Dict[str, Any]],
        scopes: List[str],
        check_cache: bool,
    ) -> str:
        logger.info("hola")
        token = getattr(self, "_get_{}_token".format(self.provider))(
            scopes, check_cache, credentials
        )
        return token

    def _get_microsoft_token(self, scopes, check_cache, credentials):
        logger.info("i am here")
        app = msal.ConfidentialClientApplication(
            self.client_id, authority=self.authority_url, client_credential=credentials
        )
        _token = None
        logger.info("i am here 1")
        if check_cache:
            logger.info("i am here 2")
            _token = app.acquire_token_silent(scopes=scopes, account=None)
            logger.info("i am here 3")

        if not _token:
            logger.info("No token available in cache. Getting a new one from AAD.")
            logger.info("scope is " + str(scopes))
            _token = app.acquire_token_for_client(scopes=scopes)
            logger.info("token is " + str(_token))
            logger.info("i am here 4")

        return _token

    def get_public_certificate_thumbprint(self, public_cert_str: str) -> str:
        cert_str = public_cert_str
        certificate = load_certificate(FILETYPE_PEM, cert_str.encode("utf-8"))
        return (certificate.digest("sha1")).decode("utf-8").replace(":", "")

    def get_token_with_certificate(
        self,
        private_key_content: str,
        public_key_content: str,
        scopes: List[str],
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
        self, secret: str, scopes: List[str], check_cache: bool = False
    ) -> Any:
        #         logger.info("in get secrets")
        return self._get_token(secret, scopes, check_cache)
