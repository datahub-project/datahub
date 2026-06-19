"""Shared tag extraction utilities for KDS streams + KDF delivery streams.

The AWS resource-tag shape is identical for both — list of {"Key": str, "Value": str}
dicts — and the global-tag flattening (`{Key}:{Value}` -> urn:li:tag:...) is the
same too. Centralizing here keeps the two extractors aligned.

Ownership is intentionally NOT derived here: it's covered generically by the
`extract_ownership_from_tags` transformer applied to the emitted globalTags.
"""

from typing import Dict, List, Optional

from datahub.emitter.mce_builder import make_tag_urn
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)


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
