"""Map DataHub cloud-connection configs to DuckDB CREATE SECRET statements.

Credentials are injected via DuckDB's secret manager, never via os.environ
(process-environment leakage; see repo security guidance).
"""

from urllib.parse import urlparse

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig


def _esc(value: str) -> str:
    """Escape single quotes for safe embedding in a DuckDB SQL string literal."""
    return value.replace("'", "''")


def build_s3_secret_sql(aws: AwsConnectionConfig) -> str:
    parts = ["TYPE s3"]
    secret = (
        aws.aws_secret_access_key.get_secret_value()
        if aws.aws_secret_access_key
        else None
    )
    if aws.aws_access_key_id and secret:
        parts.append(f"KEY_ID '{_esc(aws.aws_access_key_id)}'")
        parts.append(f"SECRET '{_esc(secret)}'")
        if aws.aws_session_token:
            parts.append(
                f"SESSION_TOKEN '{_esc(aws.aws_session_token.get_secret_value())}'"
            )
    else:
        # Reuse the ambient AWS credential chain (instance/role/profile).
        parts.append("PROVIDER credential_chain")
    if aws.aws_region:
        parts.append(f"REGION '{_esc(aws.aws_region)}'")
    if aws.aws_endpoint_url:
        # DuckDB's ENDPOINT wants host[:port] without the scheme; USE_SSL reflects https.
        # urlparse mishandles scheme-less strings like "minio.example.com:9000" —
        # it treats the whole string as a path. Detect this and prepend a dummy
        # scheme so urlparse gives us a usable netloc.
        url = aws.aws_endpoint_url
        if "://" not in url:
            parsed = urlparse(f"http://{url}")
            use_ssl = False  # no scheme → assume plain HTTP
        else:
            parsed = urlparse(url)
            use_ssl = parsed.scheme == "https"
        host = parsed.netloc or parsed.path
        if not host:
            raise ValueError(
                f"aws_endpoint_url {aws.aws_endpoint_url!r} has no host. "
                "Provide a value like 'https://minio.example.com:9000' or a bare "
                "'minio.example.com:9000'."
            )
        parts.append(f"ENDPOINT '{_esc(host)}'")
        parts.append("USE_SSL true" if use_ssl else "USE_SSL false")
    body = ", ".join(parts)
    return f"CREATE OR REPLACE SECRET datahub_s3 ({body})"
