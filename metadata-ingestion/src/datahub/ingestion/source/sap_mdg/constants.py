import re
from typing import Dict, FrozenSet, Tuple

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

SAP_MDG_PLATFORM = "sap-mdg"

METADATA_DOCUMENT_PATH = "$metadata"
SAP_CLIENT_PARAM = "sap-client"

HTTP_SCHEME_HTTP = "http://"
HTTP_SCHEME_HTTPS = "https://"
HEADER_AUTHORIZATION = "Authorization"
AUTH_BEARER_PREFIX = "Bearer "
HTTP_RETRY_MAX_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_FACTOR = 1.0
HTTP_RETRY_STATUS_CODES: Tuple[int, ...] = (429, 500, 502, 503, 504)
HTTP_RETRY_ALLOWED_METHODS: FrozenSet[str] = frozenset({"GET"})

# EDMX/CSDL element local names. We match on the namespace-stripped local name so
# a single parser handles both OData V2 and V4, whose EDM namespaces differ.
TAG_SCHEMA = "Schema"
TAG_ENTITY_TYPE = "EntityType"
TAG_PROPERTY = "Property"
TAG_PROPERTY_REF = "PropertyRef"
TAG_KEY = "Key"
TAG_NAVIGATION_PROPERTY = "NavigationProperty"
TAG_ENTITY_CONTAINER = "EntityContainer"
TAG_ENTITY_SET = "EntitySet"
TAG_ASSOCIATION = "Association"
TAG_END = "End"
TAG_REFERENTIAL_CONSTRAINT = "ReferentialConstraint"
TAG_PRINCIPAL = "Principal"
TAG_DEPENDENT = "Dependent"

# EDMX/CSDL attribute local names. Matched namespace-agnostically like the tags.
ATTR_NAMESPACE = "Namespace"
ATTR_NAME = "Name"
ATTR_TYPE = "Type"
ATTR_NULLABLE = "Nullable"
ATTR_MAX_LENGTH = "MaxLength"
ATTR_PRECISION = "Precision"
ATTR_SCALE = "Scale"
ATTR_RELATIONSHIP = "Relationship"
ATTR_FROM_ROLE = "FromRole"
ATTR_TO_ROLE = "ToRole"
ATTR_ROLE = "Role"
ATTR_PROPERTY = "Property"
ATTR_REFERENCED_PROPERTY = "ReferencedProperty"
ATTR_ENTITY_TYPE = "EntityType"

# Boolean literals as they appear in EDMX attribute values.
XML_BOOLEAN_TRUE = "true"
XML_BOOLEAN_FALSE = "false"

# Delimiters wrapping the namespace URI in a Clark-notation qualified tag, e.g.
# "{http://ns}LocalName".
XML_NAMESPACE_OPEN = "{"
XML_NAMESPACE_CLOSE = "}"

# SAP OData annotation attribute local names (sap: namespace) carrying labels.
SAP_ANNOTATION_LABEL = "label"
SAP_ANNOTATION_QUICKINFO = "quickinfo"

# Dataset/container custom property keys.
PROPERTY_ODATA_ENTITY_TYPE = "odata_entity_type"
PROPERTY_ODATA_VERSION = "odata_version"

# DRF (Data Replication Framework) customizing tables that define, per replication
# model, the governed data model and the target business systems. There is no
# standard OData service for these, so they are read through a customer-exposed
# generic table-reader service (see `drf` config). Column names below match the
# ABAP DDIC fields of the tables.
DRF_MODEL_TABLE = "DRFC_APPL"
DRF_SYSTEM_TABLE = "DRFC_APPL_SYS"
DRF_FIELD_MODEL = "APPL"
DRF_FIELD_DATA_MODEL = "USMD_MODEL"
DRF_FIELD_ACTIVE = "ACTIVE"
DRF_FIELD_BUSINESS_SYSTEM = "BUSINESS_SYSTEM"
# ABAP boolean flag: 'X' = true, ' ' = false.
DRF_ABAP_TRUE = "X"

# Response envelopes: OData V2 wraps rows in {"d": {"results": [...]}} (or {"d": [...]}),
# OData V4 in {"value": [...]}.
ODATA_V2_ENVELOPE = "d"
ODATA_V2_RESULTS = "results"
ODATA_V4_ENVELOPE = "value"
ODATA_JSON_FORMAT_PARAM = "$format"
ODATA_JSON_FORMAT = "json"

# OData collection wrapper, e.g. "Collection(Edm.String)" or "Collection(NS.Type)".
COLLECTION_TYPE_PATTERN = re.compile(r"^Collection\((?P<inner>.+)\)$")
# Namespace URIs that identify the OData CSDL version.
EDM_V2_NAMESPACE_PATTERN = re.compile(r"schemas\.microsoft\.com/ado/\d{4}/\d{2}/edm")
EDM_V4_NAMESPACE_PATTERN = re.compile(r"docs\.oasis-open\.org/odata/ns/edm")
# Leading/trailing slashes stripped from a configured service path.
SERVICE_PATH_STRIP_PATTERN = re.compile(r"^/+|/+$")

# OData EDM primitive type -> DataHub schema field type. Complex/entity types
# (namespace-qualified, non-Edm) fall back to RecordTypeClass at resolution time.
EDM_TYPE_TO_SCHEMA_FIELD_TYPE: Dict[str, type] = {
    "Edm.String": StringTypeClass,
    "Edm.Guid": StringTypeClass,
    "Edm.Boolean": BooleanTypeClass,
    "Edm.Byte": NumberTypeClass,
    "Edm.SByte": NumberTypeClass,
    "Edm.Int16": NumberTypeClass,
    "Edm.Int32": NumberTypeClass,
    "Edm.Int64": NumberTypeClass,
    "Edm.Single": NumberTypeClass,
    "Edm.Double": NumberTypeClass,
    "Edm.Decimal": NumberTypeClass,
    "Edm.Date": DateTypeClass,
    "Edm.DateTime": DateTypeClass,
    "Edm.DateTimeOffset": DateTypeClass,
    "Edm.Time": TimeTypeClass,
    "Edm.TimeOfDay": TimeTypeClass,
    "Edm.Duration": TimeTypeClass,
    "Edm.Binary": BytesTypeClass,
    "Edm.Stream": BytesTypeClass,
}

FALLBACK_SCHEMA_FIELD_TYPE: type = RecordTypeClass

# Best-effort fallback from common SAP system tokens to DataHub platform ids,
# consulted (case-insensitively) only when a logical system is not declared in
# ``logical_system_to_platform``. Logical-system names are customer-specific, so
# exact matches here are unlikely; users should declare precise targets in config.
KNOWN_LOGICAL_SYSTEM_TO_PLATFORM: Dict[str, str] = {
    "hana": "hana",
    "s4": "hana",
    "s4hana": "hana",
    "ecc": "hana",
}
