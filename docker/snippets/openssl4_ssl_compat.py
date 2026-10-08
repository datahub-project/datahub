"""Define ssl.PROTOCOL_TLSv1 on OpenSSL 4.

Installed as sitecustomize.py in the interpreter stdlib, so the image venv,
bundled ingestion venvs, and `uv venv --python` subprocesses all load it.
A .pth copied into venvs that already exist does not cover venvs created later.

Wolfi's Python 3.11 is built against OpenSSL 4, which drops ssl.PROTOCOL_TLSv1.
snowflake-connector-python's vendored urllib3 reads that name while importing,
so the executor and integrations processes die before serving. pyOpenSSL still
provides TLSv1_METHOD; the missing piece is the ssl constant used as a dict key.

The sentinel must not be PROTOCOL_TLS (2) or PROTOCOL_TLS_CLIENT (16). Those
are already keys in the vendored map, and reusing one would point them at
TLSv1_METHOD.
"""

import ssl

_MISSING_PROTOCOL_SENTINEL = 99

if not hasattr(ssl, "PROTOCOL_TLSv1"):
    ssl.PROTOCOL_TLSv1 = _MISSING_PROTOCOL_SENTINEL  # type: ignore[attr-defined]
