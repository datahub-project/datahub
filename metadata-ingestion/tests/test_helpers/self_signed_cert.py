"""Throwaway TLS material for database integration tests.

A private CA signs a server certificate. The client trusts the CA; the
server presents the leaf. Nothing here is secret: it is generated per run
and gitignored.
"""

import subprocess
from pathlib import Path

_CA_CNF = """\
[req]
distinguished_name = dn
x509_extensions = v3_ca
prompt = no
[dn]
CN = localhost-test-ca
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
"""

_SERVER_CNF = """\
[req]
distinguished_name = dn
prompt = no
[dn]
CN = {common_name}
[v3_server]
basicConstraints = CA:FALSE
subjectAltName = DNS:{common_name},IP:127.0.0.1
extendedKeyUsage = serverAuth
keyUsage = digitalSignature,keyEncipherment
"""


def _openssl(args: list[str]) -> None:
    subprocess.run(["openssl", *args], check=True, capture_output=True)


def write_self_signed_cert(directory: Path, common_name: str = "localhost") -> Path:
    """Write a CA, a server cert, and an unencrypted PKCS#8 server key.

    Returns the CA certificate path (the client trust anchor). Also writes
    ``server.crt`` and ``server.key`` for the server process. SQL Server on
    Linux rejects a traditional PKCS#1 key, so the key is PKCS#8.
    """
    directory.mkdir(parents=True, exist_ok=True)
    ca_key = directory / "ca.key"
    ca_cert = directory / "ca.crt"
    server_key = directory / "server.key"
    server_csr = directory / "server.csr"
    server_cert = directory / "server.crt"
    ca_cnf = directory / "ca.cnf"
    server_cnf = directory / "server.cnf"
    pkcs8 = directory / "server.pkcs8.key"

    ca_cnf.write_text(_CA_CNF)
    server_cnf.write_text(_SERVER_CNF.format(common_name=common_name))

    _openssl(
        [
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "3650",
            "-nodes",
            "-keyout",
            str(ca_key),
            "-out",
            str(ca_cert),
            "-config",
            str(ca_cnf),
            "-extensions",
            "v3_ca",
        ]
    )
    _openssl(
        [
            "req",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-keyout",
            str(server_key),
            "-out",
            str(server_csr),
            "-config",
            str(server_cnf),
        ]
    )
    _openssl(
        [
            "x509",
            "-req",
            "-in",
            str(server_csr),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(server_cert),
            "-days",
            "3650",
            "-sha256",
            "-extfile",
            str(server_cnf),
            "-extensions",
            "v3_server",
        ]
    )
    _openssl(
        [
            "pkcs8",
            "-topk8",
            "-nocrypt",
            "-in",
            str(server_key),
            "-out",
            str(pkcs8),
        ]
    )
    pkcs8.replace(server_key)
    server_csr.unlink(missing_ok=True)

    # The database container runs as a different uid and only sees this
    # directory through a bind mount. The leaf key and the certificates have
    # to be world-readable. The CA key does not: the server never reads it.
    for path in (ca_cert, server_key, server_cert):
        path.chmod(0o644)
    ca_key.chmod(0o600)
    return ca_cert
