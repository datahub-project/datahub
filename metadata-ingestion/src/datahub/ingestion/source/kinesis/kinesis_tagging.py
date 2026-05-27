"""Shared tag and ownership extraction utilities for KDS streams + KDF delivery streams.

The AWS resource-tag shape is identical for both — list of {"Key": str, "Value": str}
dicts. The owner-tag-to-URN mapping and the global-tag flattening
(`{Key}:{Value}` -> urn:li:tag:...) are the same too. Centralizing here keeps
the two extractors aligned.
"""

from typing import Dict, List, Optional

from datahub.emitter.mce_builder import make_tag_urn, make_user_urn
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)


def extract_owner_urns_from_tags(
    tags: List[Dict[str, str]], owner_tag_key: str
) -> List[str]:
    """Return owner URNs for the first tag matching ``owner_tag_key``.

    Always emits a ``corpuser`` URN — matches the dominant pattern across
    DataHub connectors (see Snowflake, Tableau, Looker, Dremio, etc., which
    all use ``make_user_urn`` for owner identifiers without trying to
    distinguish user vs group from the value shape). DataHub's identity
    layer handles group-membership mapping at the platform level, so the
    URN-type semantic isn't load-bearing here.

    Returns an empty list if no tag matches or the matching tag's Value is
    empty. Only the first matching tag is used — AWS allows duplicate keys
    through misconfiguration, but the first match wins.
    """
    for tag in tags:
        if tag.get("Key") == owner_tag_key and tag.get("Value"):
            return [make_user_urn(tag["Value"])]
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


def build_ownership_aspect(owners: List[str]) -> Optional[OwnershipClass]:
    """Wrap a list of owner URNs into an OwnershipClass (DATAOWNER type).

    Returns None when owners is empty so callers can skip emission.
    """
    if not owners:
        return None
    return OwnershipClass(
        owners=[
            OwnerClass(owner=urn, type=OwnershipTypeClass.DATAOWNER) for urn in owners
        ]
    )
