import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.powerbi.auth import (
    MsalCertificateCredential,
    MsalClientCredential,
    build_msal_client_credential,
)
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI

KEY_PASSPHRASE = "test-passphrase"

UNPARSEABLE_CERTIFICATE_BLOCK = (
    "-----BEGIN CERTIFICATE-----\nbm90LWEtY2VydA==\n-----END CERTIFICATE-----\n"
)


@dataclass
class KeyAndCertificate:
    key_pem: str
    traditional_key_pem: str
    encrypted_key_pem: str
    certificate_pem: str
    unrelated_certificate_pem: str
    thumbprint: str


def _self_signed_certificate(key: rsa.RSAPrivateKey, common_name: str) -> str:
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc))
        .not_valid_after(datetime.datetime(2050, 1, 1, tzinfo=datetime.timezone.utc))
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM).decode("utf-8")


@pytest.fixture(scope="module")
def key_and_certificate() -> KeyAndCertificate:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    unrelated_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    certificate_pem = _self_signed_certificate(key, "datahub-powerbi-test")
    certificate = x509.load_pem_x509_certificate(certificate_pem.encode("utf-8"))
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    traditional_key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    encrypted_key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(
            KEY_PASSPHRASE.encode("utf-8")
        ),
    ).decode("utf-8")
    return KeyAndCertificate(
        key_pem=key_pem,
        traditional_key_pem=traditional_key_pem,
        encrypted_key_pem=encrypted_key_pem,
        certificate_pem=certificate_pem,
        unrelated_certificate_pem=_self_signed_certificate(
            unrelated_key, "datahub-powerbi-unrelated"
        ),
        thumbprint=certificate.fingerprint(hashes.SHA1()).hex(),
    )


def make_config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    return PowerBiDashboardSourceConfig.model_validate(
        {"tenant_id": "fake", "client_id": "foo", **overrides}
    )


def assert_certificate_credential(
    credential: MsalClientCredential, key_and_certificate: KeyAndCertificate
) -> MsalCertificateCredential:
    assert isinstance(credential, dict)
    assert credential["thumbprint"] == key_and_certificate.thumbprint
    # Re-serialization to unencrypted PKCS8 is deterministic, so the emitted key
    # must be byte-identical to the fixture's PKCS8 PEM regardless of the input
    # encoding (traditional format, encrypted, etc.).
    assert credential["private_key"] == key_and_certificate.key_pem
    assert credential["public_certificate"] == key_and_certificate.certificate_pem
    return credential


def test_client_secret_returns_plain_secret() -> None:
    config = make_config(client_secret="bar")
    assert build_msal_client_credential(config) == "bar"


def test_certificate_data_key_then_cert(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.key_pem
        + key_and_certificate.certificate_pem
    )
    assert_certificate_credential(
        build_msal_client_credential(config), key_and_certificate
    )


def test_certificate_data_cert_then_key(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.certificate_pem
        + key_and_certificate.key_pem
    )
    assert_certificate_credential(
        build_msal_client_credential(config), key_and_certificate
    )


def test_certificate_data_with_escaped_newlines(
    key_and_certificate: KeyAndCertificate,
) -> None:
    pem = key_and_certificate.key_pem + key_and_certificate.certificate_pem
    config = make_config(certificate_data=pem.replace("\n", "\\n"))
    assert_certificate_credential(
        build_msal_client_credential(config), key_and_certificate
    )


def test_traditional_format_key_normalized_to_pkcs8(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.traditional_key_pem
        + key_and_certificate.certificate_pem
    )
    assert_certificate_credential(
        build_msal_client_credential(config), key_and_certificate
    )


def test_certificate_chain_selects_certificate_matching_key(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.key_pem
        + key_and_certificate.unrelated_certificate_pem
        + key_and_certificate.certificate_pem
    )
    assert_certificate_credential(
        build_msal_client_credential(config), key_and_certificate
    )


def test_no_certificate_matching_key(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.key_pem
        + key_and_certificate.unrelated_certificate_pem
    )
    with pytest.raises(ConfigurationError, match="match the private key"):
        build_msal_client_credential(config)


def test_unparseable_certificate_block(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.key_pem + UNPARSEABLE_CERTIFICATE_BLOCK
    )
    with pytest.raises(ConfigurationError, match="Unable to parse a certificate"):
        build_msal_client_credential(config)


def test_certificate_path(
    tmp_path: Path, key_and_certificate: KeyAndCertificate
) -> None:
    pem_file = tmp_path / "powerbi.pem"
    pem_file.write_text(
        key_and_certificate.key_pem + key_and_certificate.certificate_pem
    )
    config = make_config(certificate_path=str(pem_file))
    assert_certificate_credential(
        build_msal_client_credential(config), key_and_certificate
    )


def test_certificate_path_missing_file(tmp_path: Path) -> None:
    config = make_config(certificate_path=str(tmp_path / "does-not-exist.pem"))
    with pytest.raises(ConfigurationError, match="Unable to read certificate file"):
        build_msal_client_credential(config)


def test_certificate_path_non_pem_file(
    tmp_path: Path, key_and_certificate: KeyAndCertificate
) -> None:
    pfx_file = tmp_path / "powerbi.pfx"
    pfx_file.write_bytes(b"\x30\x82\x01\x00" + bytes(range(256)))
    config = make_config(certificate_path=str(pfx_file))
    with pytest.raises(ConfigurationError, match="PFX/DER files are not supported"):
        build_msal_client_credential(config)


def test_encrypted_key_with_passphrase(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.encrypted_key_pem
        + key_and_certificate.certificate_pem,
        certificate_password=KEY_PASSPHRASE,
    )
    assert_certificate_credential(
        build_msal_client_credential(config), key_and_certificate
    )


def test_encrypted_key_wrong_passphrase(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.encrypted_key_pem
        + key_and_certificate.certificate_pem,
        certificate_password="wrong-passphrase",
    )
    with pytest.raises(ConfigurationError, match="Unable to load the private key"):
        build_msal_client_credential(config)


def test_encrypted_key_missing_passphrase(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.encrypted_key_pem
        + key_and_certificate.certificate_pem,
    )
    with pytest.raises(ConfigurationError, match="Unable to load the private key"):
        build_msal_client_credential(config)


def test_passphrase_on_unencrypted_key(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.key_pem
        + key_and_certificate.certificate_pem,
        certificate_password="not-needed",
    )
    with pytest.raises(ConfigurationError, match="Unable to load the private key"):
        build_msal_client_credential(config)


def test_missing_private_key_block(key_and_certificate: KeyAndCertificate) -> None:
    config = make_config(certificate_data=key_and_certificate.certificate_pem)
    with pytest.raises(ConfigurationError, match="No private key found"):
        build_msal_client_credential(config)


def test_missing_certificate_block(key_and_certificate: KeyAndCertificate) -> None:
    config = make_config(certificate_data=key_and_certificate.key_pem)
    with pytest.raises(ConfigurationError, match="No certificate found"):
        build_msal_client_credential(config)


def test_powerbi_api_passes_certificate_credential_to_msal(
    key_and_certificate: KeyAndCertificate,
) -> None:
    config = make_config(
        certificate_data=key_and_certificate.key_pem
        + key_and_certificate.certificate_pem
    )
    with mock.patch("msal.ConfidentialClientApplication") as mock_msal:
        mock_msal.return_value.acquire_token_for_client.return_value = {
            "access_token": "dummy",
            "expires_in": 3600,
        }
        PowerBiAPI(config, PowerBiDashboardSourceReport())

    # Both the regular and admin API resolvers authenticate with the certificate.
    assert mock_msal.call_count == 2
    for call in mock_msal.call_args_list:
        assert_certificate_credential(
            call.kwargs["client_credential"], key_and_certificate
        )
