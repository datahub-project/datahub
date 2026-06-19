"""Shared tag extraction utilities for KDS streams + KDF delivery streams.

The AWS resource-tag shape is identical for both — list of {"Key": str, "Value": str}
dicts — and the global-tag flattening (`{Key}:{Value}` -> urn:li:tag:...) is the
same too. Centralizing here keeps the two extractors aligned.

Ownership is intentionally NOT derived here: it's covered generically by the
`extract_ownership_from_tags` transformer applied to the emitted globalTags.
"""

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport


def fetch_aws_resource_tags(
    *,
    fetch: Callable[[], Mapping[str, Any]],
    report: "KinesisSourceReport",
    api_label: str,
    context: str,
) -> List[Dict[str, str]]:
    """Fetch and normalize AWS resource tags, shared by KDS and Firehose extractors.

    ``fetch`` is the bound boto3 list-tags call (e.g.
    ``lambda: client.list_tags_for_stream(StreamName=name)``). On AWS API failure
    (e.g. AccessDenied) this emits a warning and returns ``[]`` so entity emission
    continues without tags. ``api_label`` is the AWS action shown in the warning
    (e.g. ``"kinesis:ListTagsForStream"``); ``context`` is the report context
    (e.g. ``"stream=events"``).
    """
    try:
        resp = fetch()
        # boto3 TagTypeDef declares Key/Value as NotRequired[str]; materialise to a
        # concrete Dict[str, str] (with empty-string fallbacks) for downstream helpers.
        return [
            {"Key": str(t.get("Key", "")), "Value": str(t.get("Value", ""))}
            for t in (resp.get("Tags") or [])
        ]
    except (ClientError, BotoCoreError) as e:
        code = aws_error_code(e)
        report.warning(
            title="Tag fetch failed",
            message=f"{api_label} returned {code}; emitting without tags.",
            context=context,
            exc=e,
        )
        return []


def build_global_tags_from_aws_tags(
    tags: List[Dict[str, str]],
) -> Optional[GlobalTagsClass]:
    """Flatten AWS resource tags to a DataHub GlobalTags aspect.

    Each `{Key, Value}` becomes a `urn:li:tag:{Key}:{Value}`. Returns None when the
    tag list is empty so callers can decide whether to emit the aspect at all.
    """
    if not tags:
        return None
    associations = [
        TagAssociationClass(tag=make_tag_urn(f"{t['Key']}:{t['Value']}"))
        for t in tags
        if t.get("Key") and t.get("Value")
    ]
    return GlobalTagsClass(tags=associations) if associations else None
