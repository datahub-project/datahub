from typing import Optional


def make_s3_urn(s3_uri: str, env: str, suffix: Optional[str] = None) -> str:

    if not s3_uri.startswith("s3://"):
        raise ValueError("S3 URIs should begin with 's3://'")
    # remove S3 prefix (s3://)
    s3_name = s3_uri[5:]

    if s3_name.endswith("/"):
        s3_name = s3_name[:-1]

    if suffix is not None:
        return f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_name}_{suffix},{env})"

    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_name},{env})"
