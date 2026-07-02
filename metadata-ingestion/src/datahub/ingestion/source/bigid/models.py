from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass,
    TagAssociationClass,
)


class IDSoRAttributeInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    friendly_name: str
    glossary_id: Optional[str] = None


# ---------------------------------------------------------------------------
# BigID API response models
#
# These mirror the subset of each BigID REST response the connector consumes.
# `extra="ignore"` keeps them tolerant of the many fields we don't use, and every
# field is optional so a partial payload never fails validation. Union[int, ...]
# / str fields preserve BigID's original JSON type so downstream str() output is
# unchanged. Validation happens once, at the BigIDClient boundary.
# ---------------------------------------------------------------------------

_API_MODEL_CONFIG = ConfigDict(populate_by_name=True, extra="ignore")


class BigIDTagProperties(BaseModel):
    model_config = _API_MODEL_CONFIG

    hidden: bool = False
    application_type: str = Field(default="", alias="applicationType")


class BigIDTag(BaseModel):
    model_config = _API_MODEL_CONFIG

    tag_name: str = Field(default="", alias="tagName")
    tag_value: str = Field(default="", alias="tagValue")
    tag_type: str = Field(default="", alias="tagType")
    properties: BigIDTagProperties = Field(default_factory=BigIDTagProperties)


class BigIDAttributeDetail(BaseModel):
    model_config = _API_MODEL_CONFIG

    name: str = ""
    count: Optional[int] = None
    ranks: list[str] = Field(default_factory=list)
    # `type` is a plain string on column findings but an array on unstructured
    # attribute_details, so both shapes must be accepted.
    attr_type: Union[str, list[str], None] = Field(default=None, alias="type")


class BigIDFieldClassification(BaseModel):
    model_config = _API_MODEL_CONFIG

    classification_name: str = Field(default="", alias="classificationName")
    # Kept as int|str with an empty-string default so str() yields "" when absent,
    # matching the pre-model dict.get(..., "") behaviour.
    rows_or_fields_counter: Union[int, str] = Field(
        default="", alias="rowsOrFieldsCounter"
    )
    distinct_value_count: Union[int, str] = Field(
        default="", alias="distinctValueCount"
    )


class BigIDColumnProfile(BaseModel):
    model_config = _API_MODEL_CONFIG

    field_count: Optional[Union[int, float]] = Field(default=None, alias="fieldCount")
    distinct_pct: Optional[float] = Field(default=None, alias="distinctPct")
    empty_pct: Optional[float] = Field(default=None, alias="emptyPct")
    inferred_data_type: str = Field(default="", alias="inferredDataType")
    min_num: Optional[Union[int, float]] = Field(default=None, alias="minNum")
    max_num: Optional[Union[int, float]] = Field(default=None, alias="maxNum")
    avg_num: Optional[Union[int, float]] = Field(default=None, alias="avgNum")
    num_dev: Optional[Union[int, float]] = Field(default=None, alias="numDev")
    min_lex_str: Optional[str] = Field(default=None, alias="minLexStr")
    max_lex_str: Optional[str] = Field(default=None, alias="maxLexStr")

    def has_data(self) -> bool:
        # An empty columnProfile ({}) carries no profiling signal and must be skipped.
        return bool(self.model_dump(exclude_defaults=True))


class BigIDColumn(BaseModel):
    model_config = _API_MODEL_CONFIG

    column_name: str = Field(default="", alias="columnName")
    field_type: str = Field(default="", alias="fieldType")
    fully_qualified_name: str = Field(default="", alias="fullyQualifiedName")
    object_name: str = Field(default="", alias="objectName")
    order: int = 9999
    nullable: bool = True
    is_primary: bool = Field(default=False, alias="isPrimary")
    column_profile: Optional[BigIDColumnProfile] = Field(
        default=None, alias="columnProfile"
    )
    attribute_details: list[BigIDAttributeDetail] = Field(
        default_factory=list, alias="attributeDetails"
    )
    field_classifications: list[BigIDFieldClassification] = Field(
        default_factory=list, alias="fieldClassifications"
    )


class BigIDCatalogObject(BaseModel):
    model_config = _API_MODEL_CONFIG

    source: str = ""
    fully_qualified_name: str = Field(default="", alias="fullyQualifiedName")
    object_name: str = Field(default="", alias="objectName")
    scanner_type_group: str = ""
    scan_date: Optional[str] = Field(default=None, alias="scanDate")
    last_scanned: Optional[str] = None
    tags: list[BigIDTag] = Field(default_factory=list)
    attribute_details: list[BigIDAttributeDetail] = Field(default_factory=list)
    total_pii_count: Optional[Union[int, str]] = None
    size_in_bytes: Optional[Union[int, str]] = Field(default=None, alias="sizeInBytes")


class BigIDConnection(BaseModel):
    model_config = _API_MODEL_CONFIG

    name: str = ""
    conn_type: str = Field(default="", alias="type")


class BigIDClassification(BaseModel):
    model_config = _API_MODEL_CONFIG

    original_name: str = ""
    glossary_id: Optional[str] = None
    friendly_name: Optional[str] = None


class BigIDGlossaryItem(BaseModel):
    model_config = _API_MODEL_CONFIG

    bigid_id: str = Field(default="", alias="_id")
    glossary_id: str = ""
    item_type: str = Field(default="", alias="type")
    name: str = ""
    description: Optional[str] = None
    is_ootb: bool = False
    update_date: str = ""
    owner: str = ""
    domain: str = ""
    sub_domain: str = ""


class BigIDFriendlyName(BaseModel):
    model_config = _API_MODEL_CONFIG

    friendly_name: str = Field(default="", alias="friendlyName")
    glossary_id: Optional[str] = Field(default=None, alias="glossaryId")


class BigIDResultsTuningAttribute(BaseModel):
    model_config = _API_MODEL_CONFIG

    attribute_type: str = Field(default="", alias="attributeType")
    attribute_name: str = Field(default="", alias="attributeName")
    display_name: str = Field(default="", alias="displayName")
    friendly_name_obj: Optional[BigIDFriendlyName] = Field(
        default=None, alias="friendlyName"
    )


class PendingTerm(BaseModel):
    """A GlossaryTerm entity that must be emitted before it is referenced."""

    model_config = ConfigDict(frozen=True)

    attr_name: str
    term_urn: str


class ClassificationStats(BaseModel):
    """Per-classifier finding counts surfaced in MetadataAttribution sourceDetail."""

    model_config = ConfigDict(frozen=True)

    row_count: str = ""
    distinct_count: str = ""


class TagPair(BaseModel):
    """A unique (tagName, tagValue, applicationType) triple seen in the catalog."""

    model_config = ConfigDict(frozen=True)

    tag_name: str
    tag_value: str
    application_type: str


class FieldEnrichment(BaseModel):
    """Accumulated enrichment for one schema field across all attributeDetails entries."""

    # `terms` holds DataHub Avro-generated aspect objects, which pydantic cannot
    # introspect, so they are treated as opaque values.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    terms: list[GlossaryTermAssociationClass] = Field(default_factory=list)
    tag_urns: list[str] = Field(default_factory=list)
    seen_urns: set[str] = Field(default_factory=set)
    # Parallel dedup for tag_urns: two findings of the same rank on one field both
    # yield e.g. urn:li:tag:bigid.confidence:HIGH, and GMS rejects a GlobalTagsClass
    # containing duplicate tag URNs.
    seen_tag_urns: set[str] = Field(default_factory=set)

    def add_tag_urns(self, tag_urns: list[str]) -> None:
        for tag_urn in tag_urns:
            if tag_urn not in self.seen_tag_urns:
                self.seen_tag_urns.add(tag_urn)
                self.tag_urns.append(tag_urn)


class AttrEnrichment(BaseModel):
    """Enrichment derived from a single attributeDetails entry."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    term_assoc: GlossaryTermAssociationClass
    confidence_tag_urns: list[str] = Field(default_factory=list)
    pending_term: Optional[PendingTerm] = None


class DatasetTagExtract(BaseModel):
    """Tag associations and an optional riskScore extracted from OBJECT-level tags."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    tag_assocs: list[TagAssociationClass] = Field(default_factory=list)
    risk_score: Optional[float] = None


class IDSoRResolution(BaseModel):
    model_config = ConfigDict(frozen=True)

    term_urn: str
    resolved_friendly: Optional[str] = None
    pending_term: Optional[PendingTerm] = None


class TermResolution(BaseModel):
    model_config = ConfigDict(frozen=True)

    term_urn: str
    resolved_friendly: Optional[str] = None
    pending_term: Optional[PendingTerm] = None
    clf_type: str
