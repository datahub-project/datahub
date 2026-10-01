"""Metadata-only probe over S3, through the AwsConnectionConfig, listing helpers
and path_specs S3Source ingests with. The listings themselves live in the
shared S3-compatible base; this adds what only native S3 has."""

import re
from typing import Dict, List

from botocore.exceptions import ClientError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig, aws_error_code
from datahub.ingestion.source.aws.s3_boto_utils import (
    get_bucket_tag_set,
    get_object_tag_set,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.data_lake_common.object_store_probe import (
    S3CompatibleMetadataProbe,
)
from datahub.ingestion.source.s3.config import DataLakeSourceConfig, s3_probe_refusal


class S3MetadataProbe(S3CompatibleMetadataProbe):
    """Lists S3 the way S3Source does, and never reads an object's contents."""

    display_scheme = "s3://"
    # Current S3 rules are stricter, but buckets created in us-east-1 before
    # 2018 may hold uppercase letters and underscores, up to 255 characters,
    # and ingestion lists them. This only keeps '/' and empty names out of
    # the request; botocore validates the rest.
    bucket_name_pattern = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{1,253}[A-Za-z0-9]$")

    def __init__(self, config: DataLakeSourceConfig) -> None:
        super().__init__(
            # Never used when aws_config is missing: the refusal stops every
            # command first. A placeholder keeps the base's type non-Optional.
            aws_config=(
                config.aws_config
                if config.aws_config is not None
                else AwsConnectionConfig()
            ),
            path_specs=config.path_specs,
            refusal=s3_probe_refusal(config),
        )
        self._config = config

    @classmethod
    def for_config(cls, config: DataLakeSourceConfig) -> "S3MetadataProbe":
        # No I/O: AwsConnectionConfig resolves credentials, and assumes
        # aws_role through STS, in get_session -- first called by a listing.
        return cls(config)

    @probe_method(kind=DatasetContainerSubTypes.S3_BUCKET, row_limit_param="limit")
    def buckets(self, limit: int = 200) -> List[str]:
        """Buckets the credential's account owns (ListBuckets: every
        general-purpose bucket, in all regions; a custom aws_endpoint_url lists
        that server's), whether or not a path_spec reaches them -- judge them
        with `probe filter --kind "S3 bucket"`. Needs s3:ListAllMyBuckets: a
        credential scoped to named buckets reports a failure, not an empty
        list. S3 Express directory buckets are not listed."""
        return self._list_buckets(limit)

    @probe_method()
    def tags(self, bucket: str, key: str = "") -> Dict[str, List[str]]:
        """The tags use_s3_bucket_tags / use_s3_object_tags would attach, as
        "key:value" strings: the bucket's, and the object's when `key` is given.
        The one command beyond ListBuckets / ListObjectsV2: it issues
        GetBucketTagging and GetObjectTagging, and only the lookups the recipe
        turns on, so it never reads more than ingestion would. Ingestion
        attaches object tags only to datasets that are a single file (a
        path_spec without {table}); a denied object-tag read fails an ingestion
        run, which is what this lets you check first. Metadata only -- object
        contents are never read."""
        self._bucket_uri(bucket, "")  # the refusal, then the bucket-name check
        flags = self._config
        if not (flags.use_s3_bucket_tags or flags.use_s3_object_tags):
            raise ValueError(
                "this recipe sets neither use_s3_bucket_tags nor "
                "use_s3_object_tags, so ingestion reads no tags and neither "
                "does the probe"
            )
        if key and not flags.use_s3_object_tags:
            raise ValueError(
                "`key` asks for object tags, but use_s3_object_tags is off, so "
                "ingestion would not read them"
            )
        if not key and not flags.use_s3_bucket_tags:
            raise ValueError(
                "this recipe reads object tags only (use_s3_object_tags), so pass "
                "`key` to name the object to read them from"
            )
        found: Dict[str, List[str]] = {"bucket": [], "object": []}
        if flags.use_s3_bucket_tags:
            with self._storage_errors(
                f"reading the tags of bucket '{bucket}'", whole=True
            ):
                try:
                    tag_set = get_bucket_tag_set(
                        bucket, self._aws_config, flags.verify_ssl
                    )
                except ClientError as exc:
                    # What an untagged bucket answers; ingestion logs it and
                    # attaches nothing.
                    if aws_error_code(exc) != "NoSuchTagSet":
                        raise
                    tag_set = []
                found["bucket"] = [f"{t['Key']}:{t['Value']}" for t in tag_set]
        if key:
            with self._storage_errors(
                f"reading the tags of object '{key}'", whole=True
            ):
                found["object"] = [
                    f"{t['Key']}:{t['Value']}"
                    for t in get_object_tag_set(bucket, key, self._aws_config)
                ]
        return found
