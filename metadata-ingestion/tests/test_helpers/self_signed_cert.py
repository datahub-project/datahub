"""Throwaway TLS material for database integration tests.

A private CA signs a server certificate. The client trusts the CA; the
server presents the leaf. Nothing here is secret: it is generated per run
and gitignored.

Built with `cryptography` rather than the `openssl` binary: the library is
already a dependency and ships in the wheel, where the binary is a native
tool that the executor image does not carry. Shelling out passed in CI and
failed the in-image connector validation after merge.
"""

import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

_VALID_FOR = datetime.timedelta(days=3650)


def _new_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    # PKCS#8 and unencrypted: SQL Server on Linux rejects a traditional
    # PKCS#1 key, and the server process has no passphrase to offer.
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def write_self_signed_cert(directory: Path, common_name: str = "localhost") -> Path:
    """Write a CA, a server cert, and an unencrypted PKCS#8 server key.

    Returns the CA certificate path (the client trust anchor). Also writes
    ``server.crt`` and ``server.key`` for the server process.
    """
    directory.mkdir(parents=True, exist_ok=True)
    ca_key_path = directory / "ca.key"
    ca_cert_path = directory / "ca.crt"
    server_key_path = directory / "server.key"
    server_cert_path = directory / "server.crt"

    now = datetime.datetime.now(datetime.timezone.utc)
    # Backdated a little so a clock skew between this process and the
    # database container cannot make a just-issued certificate not-yet-valid.
    not_before = now - datetime.timedelta(minutes=5)
    not_after = now + _VALID_FOR

    ca_key = _new_key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost-test-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )

    # No CSR: the CA key is right here, so the leaf is signed directly.
    server_key = _new_key()
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca_name)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName(common_name),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )

    _write_key(ca_key_path, ca_key)
    _write_cert(ca_cert_path, ca_cert)
    _write_key(server_key_path, server_key)
    _write_cert(server_cert_path, server_cert)

    # The database container runs as a different uid and only sees this
    # directory through a bind mount. The leaf key and the certificates have
    # to be world-readable. The CA key does not: the server never reads it.
    for path in (ca_cert_path, server_key_path, server_cert_path):
        path.chmod(0o644)
    ca_key_path.chmod(0o600)
    return ca_cert_path
