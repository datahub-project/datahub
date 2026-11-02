import logging
import re
from typing import List, Optional, Union

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
from cryptography.x509 import Certificate, load_pem_x509_certificate
from typing_extensions import TypedDict

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig

logger = logging.getLogger(__name__)


class MsalCertificateCredential(TypedDict):
    private_key: str
    thumbprint: str
    public_certificate: str


# The credential value accepted by msal.ConfidentialClientApplication: a plain
# client secret, or a dict describing a certificate credential.
MsalClientCredential = Union[str, MsalCertificateCredential]

# PEM files exported from Azure commonly bundle the private key and the
# certificate (sometimes a whole chain) in one file, in any order.
# cryptography's loaders are not guaranteed to skip unrelated PEM blocks, so
# extract each block explicitly.
_PRIVATE_KEY_BLOCK_PATTERN = re.compile(
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.+?-----END [A-Z0-9 ]*PRIVATE KEY-----",
    re.DOTALL,
)
_CERTIFICATE_BLOCK_PATTERN = re.compile(
    r"-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----",
    re.DOTALL,
)


def _read_certificate_pem(config: PowerBiDashboardSourceConfig) -> str:
    if config.certificate_data is not None:
        return config.certificate_data.get_secret_value()

    if config.certificate_path is None:
        # Unreachable when the config validator has run; kept explicit so a
        # config built without validation fails with a clear error.
        raise ConfigurationError(
            "No certificate configured. Set `certificate_path` or `certificate_data`."
        )
    try:
        with open(config.certificate_path, encoding="utf-8") as f:
            return f.read()
    except (OSError, UnicodeDecodeError) as e:
        raise ConfigurationError(
            f"Unable to read certificate file {config.certificate_path} as PEM: {e}. "
            "The file must be a PEM containing both the private key and the "
            "certificate; PFX/DER files are not supported."
        ) from e


def _load_private_key(pem_content: str, passphrase: Optional[str]) -> PrivateKeyTypes:
    key_match = _PRIVATE_KEY_BLOCK_PATTERN.search(pem_content)
    if key_match is None:
        raise ConfigurationError(
            "No private key found in the configured certificate PEM. The PEM must "
            "contain both the private key and the certificate."
        )

    try:
        return serialization.load_pem_private_key(
            key_match.group(0).encode("utf-8"),
            password=passphrase.encode("utf-8") if passphrase is not None else None,
        )
    except (ValueError, TypeError, UnsupportedAlgorithm) as e:
        raise ConfigurationError(
            f"Unable to load the private key from the configured certificate PEM. "
            f"Verify the key material and `certificate_password`: {e}"
        ) from e


def _load_certificate_matching_key(
    pem_content: str, private_key: PrivateKeyTypes
) -> Certificate:
    certificate_blocks = _CERTIFICATE_BLOCK_PATTERN.findall(pem_content)
    if not certificate_blocks:
        raise ConfigurationError(
            "No certificate found in the configured certificate PEM. The PEM must "
            "contain both the private key and the certificate."
        )

    certificates: List[Certificate] = []
    for block in certificate_blocks:
        try:
            certificates.append(load_pem_x509_certificate(block.encode("utf-8")))
        except ValueError as e:
            raise ConfigurationError(
                f"Unable to parse a certificate in the configured PEM: {e}"
            ) from e

    # A PEM exported with the full chain contains intermediate/root certificates
    # too; Entra ID identifies the app by the leaf, so pick the certificate that
    # actually pairs with the private key.
    key_public_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    for certificate in certificates:
        certificate_public_bytes = certificate.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        if certificate_public_bytes == key_public_bytes:
            return certificate

    raise ConfigurationError(
        f"None of the {len(certificates)} certificate(s) in the configured PEM "
        "match the private key. Ensure the PEM contains the certificate issued "
        "for this key."
    )


def build_msal_client_credential(
    config: PowerBiDashboardSourceConfig,
) -> MsalClientCredential:
    if config.client_secret is not None:
        return config.client_secret.get_secret_value()

    pem_content = _read_certificate_pem(config)
    passphrase = (
        config.certificate_password.get_secret_value()
        if config.certificate_password is not None
        else None
    )

    private_key = _load_private_key(pem_content, passphrase)
    certificate = _load_certificate_matching_key(pem_content, private_key)

    # Re-serialize to an unencrypted PKCS8 PEM so MSAL can consume the key
    # directly, regardless of the original encoding or encryption.
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    # Entra ID identifies the uploaded certificate by its SHA-1 thumbprint.
    # (msal >= 1.35 can derive a SHA-256 thumbprint itself when `thumbprint` is
    # omitted; switch to that once the msal floor is raised.)
    thumbprint = certificate.fingerprint(hashes.SHA1()).hex()
    certificate_pem = certificate.public_bytes(serialization.Encoding.PEM).decode(
        "utf-8"
    )
    logger.info(
        f"Using certificate-based service principal authentication "
        f"(certificate thumbprint: {thumbprint})"
    )

    return MsalCertificateCredential(
        private_key=private_key_pem,
        thumbprint=thumbprint,
        # Enables subject name/issuer authentication for tenants that require it;
        # harmless otherwise.
        public_certificate=certificate_pem,
    )
