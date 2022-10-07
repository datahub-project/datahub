import logging
from typing import Iterable, Optional

from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    is_s3_uri,
)
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


def get_s3_tags(
    bucket_name: str,
    key_name: Optional[str],
    dataset_urn: str,
    aws_config: Optional[AwsConnectionConfig],
    ctx: PipelineContext,
    use_s3_bucket_tags: Optional[bool] = False,
    use_s3_object_tags: Optional[bool] = False,
) -> Optional[GlobalTagsClass]:
    if aws_config is None:
        raise ValueError("aws_config not set. Cannot browse s3")
    new_tags = GlobalTagsClass(tags=[])
    tags_to_add = []
    if use_s3_bucket_tags:
        s3 = aws_config.get_s3_resource()
        bucket = s3.Bucket(bucket_name)
        try:
            tags_to_add.extend(
                [
                    make_tag_urn(f"""{tag["Key"]}:{tag["Value"]}""")
                    for tag in bucket.Tagging().tag_set
                ]
            )
        except s3.meta.client.exceptions.ClientError:
            logger.warn(f"No tags found for bucket={bucket_name}")

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
            logger.warn(f"No tags found for bucket={bucket_name} key={key_name}")
    if len(tags_to_add) == 0:
        return None
    if ctx.graph is not None:
        logger.debug("Connected to DatahubApi, grabbing current tags to maintain.")
        current_tags: Optional[GlobalTagsClass] = ctx.graph.get_aspect_v2(
            entity_urn=dataset_urn,
            aspect="globalTags",
            aspect_type=GlobalTagsClass,
        )
        if current_tags:
            tags_to_add.extend([current_tag.tag for current_tag in current_tags.tags])
    else:
        logger.warn("Could not connect to DatahubApi. No current tags to maintain")
    # Remove duplicate tags
    tags_to_add = list(set(tags_to_add))
    new_tags = GlobalTagsClass(
        tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
    )
    return new_tags


def list_folders_path(
    s3_uri: str, aws_config: Optional[AwsConnectionConfig]
) -> Iterable[str]:
    if not is_s3_uri(s3_uri):
        raise ValueError("Not a s3 URI: " + s3_uri)
    if aws_config is None:
        raise ValueError("aws_config not set. Cannot browse s3")
    bucket_name = get_bucket_name(s3_uri)
    prefix = get_bucket_relative_path(s3_uri)
    yield from list_folders(bucket_name, prefix, aws_config)


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
