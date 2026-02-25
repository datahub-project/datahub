import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Optional, Union

from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    is_s3_uri,
)
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import ObjectSummary

logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

LIST_OBJECTS_PAGE_SIZE = 1000


def get_s3_tags(
    bucket_name: str,
    key_name: Optional[str],
    dataset_urn: str,
    aws_config: Optional[AwsConnectionConfig],
    ctx: PipelineContext,
    use_s3_bucket_tags: Optional[bool] = False,
    use_s3_object_tags: Optional[bool] = False,
    verify_ssl: Optional[Union[bool, str]] = None,
) -> Optional[GlobalTagsClass]:
    if aws_config is None:
        raise ValueError("aws_config not set. Cannot browse s3")
    new_tags = GlobalTagsClass(tags=[])
    tags_to_add = []
    if use_s3_bucket_tags:
        s3 = aws_config.get_s3_resource(verify_ssl)
        bucket = s3.Bucket(bucket_name)
        try:
            tags_to_add.extend(
                [
                    make_tag_urn(f"""{tag["Key"]}:{tag["Value"]}""")
                    for tag in bucket.Tagging().tag_set
                ]
            )
        except s3.meta.client.exceptions.ClientError:
            logger.warning(f"No tags found for bucket={bucket_name}")

    if use_s3_object_tags and key_name is not None:
        s3_client = aws_config.get_s3_client()
        object_tagging = s3_client.get_object_tagging(Bucket=bucket_name, Key=key_name)
        tag_set = object_tagging["TagSet"]
        if tag_set:
            tags_to_add.extend(
                [make_tag_urn(f"""{tag["Key"]}:{tag["Value"]}""") for tag in tag_set]
            )
        else:
            # Unlike bucket tags, if an object does not have tags, it will just return an empty array
            # as opposed to an exception.
            logger.warning(f"No tags found for bucket={bucket_name} key={key_name}")
    if len(tags_to_add) == 0:
        return None
    if ctx.graph is not None:
        logger.debug("Connected to DatahubApi, grabbing current tags to maintain.")
        current_tags: Optional[GlobalTagsClass] = ctx.graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=GlobalTagsClass,
        )
        if current_tags:
            tags_to_add.extend([current_tag.tag for current_tag in current_tags.tags])
    else:
        logger.warning("Could not connect to DatahubApi. No current tags to maintain")
    # Remove duplicate tags
    tags_to_add = sorted(list(set(tags_to_add)))
    new_tags = GlobalTagsClass(
        tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
    )
    return new_tags


@dataclass
class DirEntry:
    """
    Intended to be similar to os.DirEntry, which contains a name, full path, and possibly
    other attributes of a directory entry. Currently only used to represent S3 folder-like
    paths.
    """

    name: str
    path: str


def list_folders_path(
    s3_uri: str,
    *,
    startswith: str = "",
    aws_config: Optional[AwsConnectionConfig] = None,
) -> Iterable[DirEntry]:
    """
    Given an S3 URI to a folder or bucket, return all sub-folders underneath that URI,
    optionally filtering by startswith. Returned entries never contain a trailing slash.
    """

    if not is_s3_uri(s3_uri):
        raise ValueError("Not a s3 URI: " + s3_uri)
    if aws_config is None:
        raise ValueError("aws_config not set. Cannot browse s3")

    if not s3_uri.endswith("/"):
        s3_uri += "/"

    bucket_name = get_bucket_name(s3_uri)
    if not bucket_name:
        # No bucket name means we only have the s3[an]:// protocol, not a full bucket and
        # prefix.
        for folder in list_buckets(startswith, aws_config):
            yield DirEntry(name=folder, path=f"{s3_uri}{folder}")
        return

    prefix = get_bucket_relative_path(s3_uri) + startswith
    for folder in list_folders(bucket_name, prefix, aws_config):
        folder = folder.removesuffix("/").split("/")[-1]
        yield DirEntry(name=folder, path=f"{s3_uri}{folder}")


def list_objects_recursive_path(
    s3_uri: str,
    *,
    startswith: str = "",
    aws_config: Optional[AwsConnectionConfig] = None,
) -> Iterable["ObjectSummary"]:
    """
    Given an S3 URI to a folder or bucket, return all objects underneath that URI, optionally
    filtering by startswith.
    """

    if not is_s3_uri(s3_uri):
        raise ValueError("Not a s3 URI: " + s3_uri)
    if aws_config is None:
        raise ValueError("aws_config not set. Cannot browse s3")
    if startswith and "/" in startswith:
        raise ValueError(f"startswith contains forward slash: {repr(startswith)}")

    if not s3_uri.endswith("/"):
        s3_uri += "/"

    bucket_name = get_bucket_name(s3_uri)
    if not bucket_name:
        # No bucket name means we only have the s3[an]:// protocol, not a full bucket and
        # prefix.
        for bucket_name in list_buckets(startswith, aws_config):
            yield from list_objects_recursive(bucket_name, "", aws_config)
        return

    prefix = get_bucket_relative_path(s3_uri) + startswith
    yield from list_objects_recursive(bucket_name, prefix, aws_config)


def list_folders(
    bucket_name: str, prefix: str, aws_config: Optional[AwsConnectionConfig]
) -> Iterable[str]:
    if aws_config is None:
        raise ValueError("aws_config not set. Cannot browse s3")
    s3_client = aws_config.get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        for o in page.get("CommonPrefixes", []):
            folder: str = str(o.get("Prefix"))
            if folder.endswith("/"):
                folder = folder[:-1]
            yield f"{folder}"


def list_buckets(
    prefix: str, aws_config: Optional[AwsConnectionConfig]
) -> Iterable[str]:
    if aws_config is None:
        raise ValueError("aws_config not set. Cannot browse s3")
    s3_client = aws_config.get_s3_client()
    paginator = s3_client.get_paginator("list_buckets")
    for page in paginator.paginate(Prefix=prefix):
        for o in page.get("Buckets", []):
            yield str(o.get("Name"))


def list_objects_recursive(
    bucket_name: str, prefix: str, aws_config: Optional[AwsConnectionConfig]
) -> Iterable["ObjectSummary"]:
    if aws_config is None:
        raise ValueError("aws_config not set. Cannot browse s3")
    s3_resource = aws_config.get_s3_resource()
    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix).page_size(LIST_OBJECTS_PAGE_SIZE):
        yield obj
