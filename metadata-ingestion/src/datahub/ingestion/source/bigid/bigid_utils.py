import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import quote

from datahub.emitter.mce_builder import datahub_guid, make_term_urn
from datahub.ingestion.source.bigid.constants import (
    CLASSIFIER_MD_PREFIX,
    CLASSIFIER_PREFIX,
    CLF_TYPE_BUSINESS_TERM,
    CLF_TYPE_METADATA,
    CLF_TYPE_VALUE,
    CONFIDENCE_RANK_FLOATS,
    FIELD_TYPE_BOOLEAN_TOKENS,
    FIELD_TYPE_BYTES_TOKENS,
    FIELD_TYPE_NUMERIC_TOKENS,
    FIELD_TYPE_STRING_TOKENS,
    FIELD_TYPE_TIME_TOKENS,
    IDSOR_ATTRIBUTE_TYPE,
    PROFILE_NUMERIC_TYPES,
)
from datahub.ingestion.source.bigid.models import (
    BigIDAttributeDetail,
    BigIDColumn,
    BigIDFieldClassification,
    ClassificationStats,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DatasetFieldProfileClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)


def _rank_to_float(rank: str) -> float:
    # Unknown ranks warn rather than silently returning 0.0: a silent 0.0 would let an
    # unrecognised rank slip past minimum_confidence_threshold filtering without anyone
    # noticing BigID changed its rank vocabulary.
    value = CONFIDENCE_RANK_FLOATS.get((rank or "").upper())
    if value is None:
        logger.warning(
            "Unknown BigID confidence rank %r; treating as 0.0. Expected one of %s.",
            rank,
            sorted(CONFIDENCE_RANK_FLOATS),
        )
        return 0.0
    return value


def _map_field_type(field_type: str) -> SchemaFieldDataTypeClass:
    normalized = (field_type or "").lower()

    def matches(needles: Tuple[str, ...]) -> bool:
        return any(needle in normalized for needle in needles)

    if matches(FIELD_TYPE_STRING_TOKENS):
        return SchemaFieldDataTypeClass(type=StringTypeClass())
    if matches(FIELD_TYPE_NUMERIC_TOKENS):
        return SchemaFieldDataTypeClass(type=NumberTypeClass())
    if matches(FIELD_TYPE_BOOLEAN_TOKENS):
        return SchemaFieldDataTypeClass(type=BooleanTypeClass())
    if matches(FIELD_TYPE_TIME_TOKENS):
        return SchemaFieldDataTypeClass(type=TimeTypeClass())
    if matches(FIELD_TYPE_BYTES_TOKENS):
        return SchemaFieldDataTypeClass(type=BytesTypeClass())
    return SchemaFieldDataTypeClass(type=StringTypeClass())


_SLUG_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _slugify(name: str) -> str:
    slug = _SLUG_NON_ALNUM_RE.sub("_", name.lower().strip())
    return slug.strip("_")


# DataHub dataset URN format: urn:li:dataset:(urn:li:dataPlatform:{p},{name},{env})
# The name component must not contain the delimiter characters ( ) ,
# '?' is deliberately excluded from the safe set (i.e. it is percent-encoded):
# a literal '?' in a URN reads as a query string to URL-based tooling.
_URN_RESERVED = "(),:?"
_URN_SAFE_CHARS = "".join(
    char
    for char in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'*+;=@/"
    if char not in _URN_RESERVED
)


def _encode_urn_name(name: str) -> str:
    return quote(name, safe=_URN_SAFE_CHARS)


def _bigid_term_urn(suffix: str) -> str:
    # GUID derived from the stable `bigid.<suffix>` path key, mirroring the business-glossary
    # source's datahub_guid keying. The URN is opaque but deterministic across runs; the
    # readable label lives on GlossaryTermInfo.name.
    return make_term_urn(datahub_guid({"path": f"bigid.{suffix}"}))


def _tag_display_name(tag_name: str, tag_value: str) -> str:
    # Strip the BigID-internal `system.*` prefix so only the meaningful segment shows in the
    # UI pill (e.g. system.risk.riskGroup + high -> "riskGroup : high"). Names without the
    # prefix are kept verbatim. The URN is unaffected.
    segment = (
        tag_name.rsplit(".", 1)[-1] if tag_name.startswith("system.") else tag_name
    )
    return f"{segment} : {tag_value}"


def _classifier_type(attr_name: str) -> str:
    # MD:: must be tested before the bare classifier prefix (longer match on the same
    # string): this ordering is a correctness invariant.
    if attr_name.startswith(CLASSIFIER_MD_PREFIX):
        return CLF_TYPE_METADATA
    if attr_name.startswith(CLASSIFIER_PREFIX):
        return CLF_TYPE_VALUE
    return CLF_TYPE_BUSINESS_TERM


def _strip_classifier_prefix(attr_name: str) -> str:
    bare_name = attr_name[len(CLASSIFIER_PREFIX) :]
    if bare_name.startswith("MD::"):
        bare_name = bare_name[len("MD::") :]
    return bare_name


def _build_clf_stats(
    field_classifications: List[BigIDFieldClassification],
) -> Dict[str, ClassificationStats]:
    clf_stats: Dict[str, ClassificationStats] = {}
    for classification in field_classifications:
        if classification.classification_name:
            clf_stats[classification.classification_name] = ClassificationStats(
                row_count=str(classification.rows_or_fields_counter),
                distinct_count=str(classification.distinct_value_count),
            )
    return clf_stats


def _coerce_int(value: Union[int, str, None]) -> Optional[int]:
    # BigID returns counts/sizes as ints or numeric strings. None and blank strings mean
    # "absent" (field omitted from the profile); a genuine 0 is a valid count and must be
    # preserved rather than dropped by a falsy check.
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_iso_to_ms(ts: Optional[str]) -> Optional[int]:
    if not ts:
        return None
    try:
        normalized = ts.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return int(parsed.timestamp() * 1000)
    except ValueError:
        return None


def _is_idsor_attr(attr: BigIDAttributeDetail) -> bool:
    # IDSoR attributes carry `type` as an array (e.g. ["IDSoR Attribute"]), whereas
    # classifiers and business terms carry `type` as a plain string.
    return isinstance(attr.attr_type, list) and IDSOR_ATTRIBUTE_TYPE in attr.attr_type


def _build_field_profile(column: BigIDColumn) -> Optional[DatasetFieldProfileClass]:
    field_path = column.column_name
    profile = column.column_profile
    if not field_path or profile is None or not profile.has_data():
        return None

    unique_prop: Optional[float] = None
    unique_count: Optional[int] = None
    if profile.distinct_pct is not None:
        unique_prop = profile.distinct_pct / 100.0
        if profile.field_count is not None:
            unique_count = round(profile.field_count * unique_prop)

    null_prop: Optional[float] = None
    null_count: Optional[int] = None
    if profile.empty_pct is not None:
        null_prop = profile.empty_pct / 100.0
        if profile.field_count is not None:
            null_count = round(profile.field_count * null_prop)

    is_numeric = profile.inferred_data_type.lower() in PROFILE_NUMERIC_TYPES

    min_val: Optional[str] = None
    max_val: Optional[str] = None
    mean_val: Optional[str] = None
    stdev_val: Optional[str] = None
    sample_values: Optional[List[str]] = None

    if is_numeric:
        if profile.min_num is not None:
            min_val = str(profile.min_num)
        if profile.max_num is not None:
            max_val = str(profile.max_num)
        if profile.avg_num is not None:
            mean_val = str(profile.avg_num)
        if profile.num_dev is not None:
            stdev_val = str(profile.num_dev)
    elif profile.min_lex_str is not None or profile.max_lex_str is not None:
        if profile.min_lex_str is not None:
            min_val = str(profile.min_lex_str)
        if profile.max_lex_str is not None:
            max_val = str(profile.max_lex_str)
        if profile.min_lex_str is not None and profile.max_lex_str is not None:
            sample_values = [str(profile.min_lex_str), str(profile.max_lex_str)]

    return DatasetFieldProfileClass(
        fieldPath=field_path,
        uniqueCount=unique_count,
        uniqueProportion=unique_prop,
        nullCount=null_count,
        nullProportion=null_prop,
        min=min_val,
        max=max_val,
        mean=mean_val,
        stdev=stdev_val,
        sampleValues=sample_values,
    )
