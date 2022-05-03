import os

S3_PREFIXES = ["s3://", "s3n://", "s3a://"]


def is_s3_uri(uri: str) -> bool:
    return any(uri.startswith(prefix) for prefix in S3_PREFIXES)


def strip_s3_prefix(s3_uri: str) -> str:
    # remove S3 prefix (s3://)
    for s3_prefix in S3_PREFIXES:
        if s3_uri.startswith(s3_prefix):
            plain_base_path = s3_uri[len(s3_prefix) :]
            return plain_base_path

    raise ValueError(
        f"Not an S3 URI. Must start with one of the following prefixes: {str(S3_PREFIXES)}"
    )


def get_bucket_relative_path(s3_uri: str) -> str:
    return "/".join(strip_s3_prefix(s3_uri).split("/")[1:])


def make_s3_urn(s3_uri: str, env: str) -> str:

    s3_name = strip_s3_prefix(s3_uri)

    if s3_name.endswith("/"):
        s3_name = s3_name[:-1]

    name, extension = os.path.splitext(s3_name)

    if extension != "":
        extension = extension[1:]  # remove the dot
        return f"urn:li:dataset:(urn:li:dataPlatform:s3,{name}_{extension},{env})"

    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_name},{env})"


def get_bucket_name(s3_uri: str) -> str:
    if not is_s3_uri(s3_uri):
        raise ValueError(
            f"Not an S3 URI. Must start with one of the following prefixes: {str(S3_PREFIXES)}"
        )
    return strip_s3_prefix(s3_uri).split("/")[0]


def get_key_prefix(s3_uri: str) -> str:
    if not is_s3_uri(s3_uri):
        raise ValueError(
            f"Not an S3 URI. Must start with one of the following prefixes: {str(S3_PREFIXES)}"
        )
    return strip_s3_prefix(s3_uri).split("/", maxsplit=1)[1]
