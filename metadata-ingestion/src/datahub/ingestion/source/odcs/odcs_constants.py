from typing import Dict, Tuple, Type

from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    MapTypeClass,
    NumberTypeClass,
    OwnershipTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

# ODCS team `role` value -> DataHub OwnershipType. Anything not listed here
# falls back to TECHNICAL_OWNER.
OWNER_ROLE_MAP: Dict[str, str] = {
    "owner": OwnershipTypeClass.TECHNICAL_OWNER,
    "dataOwner": OwnershipTypeClass.TECHNICAL_OWNER,
    "data_owner": OwnershipTypeClass.TECHNICAL_OWNER,
    "technical_owner": OwnershipTypeClass.TECHNICAL_OWNER,
    "technicalOwner": OwnershipTypeClass.TECHNICAL_OWNER,
    "business_owner": OwnershipTypeClass.BUSINESS_OWNER,
    "businessOwner": OwnershipTypeClass.BUSINESS_OWNER,
    "steward": OwnershipTypeClass.DATA_STEWARD,
    "data_steward": OwnershipTypeClass.DATA_STEWARD,
    "dataSteward": OwnershipTypeClass.DATA_STEWARD,
}

# ODCS library metric vocabulary, spec-exact. v3.1 names the library key
# `metric` (with `rule` as a deprecated alias); v3.0.x used `rule`. These are
# the only library rules either spec version defines — anything else falls
# through to a CustomAssertion so intent is preserved, never guessed.
METRIC_NULL_VALUES = "nullValues"  # v3.1, property-level
METRIC_MISSING_VALUES = "missingValues"  # v3.1, property-level
METRIC_INVALID_VALUES = "invalidValues"  # v3.1, property-level
METRIC_DUPLICATE_VALUES = "duplicateValues"  # v3.1, property- or schema-level
METRIC_ROW_COUNT = "rowCount"  # v3.0 + v3.1, schema-level
METRIC_DUPLICATE_COUNT = "duplicateCount"  # v3.0 name for duplicateValues
METRIC_VALID_VALUES = "validValues"  # v3.0 name for invalidValues

UNIT_ROWS = "rows"
UNIT_PERCENT = "percent"

RULE_TYPE_LIBRARY = "library"
RULE_TYPE_TEXT = "text"
RULE_TYPE_SQL = "sql"
RULE_TYPE_CUSTOM = "custom"

# ODCS provenance customProperty keys. Hoisted so a typo produces a reference
# error rather than a silently divergent property key no test would catch.
PROP_ID = "odcs.id"
PROP_SCHEMA_NAME = "odcs.schemaName"
PROP_QUALITY_RULE_COUNT = "odcs.qualityRuleCount"
PROP_VERSION = "odcs.version"
PROP_API_VERSION = "odcs.apiVersion"
PROP_STATUS = "odcs.status"
PROP_PHYSICAL_NAME = "odcs.physicalName"
PROP_SOURCE_FILE = "odcs.sourceFile"
PROP_DOMAIN = "odcs.domain"
PROP_DATA_PRODUCT = "odcs.dataProduct"
PROP_TENANT = "odcs.tenant"
PROP_SCOPE = "odcs.scope"
PROP_ASSERTION = "odcs.assertion"
PROP_RULE = "odcs.rule"
PROP_RULE_ID = "odcs.rule.id"
PROP_RULE_NAME = "odcs.rule.name"
PROP_RULE_METRIC = "odcs.rule.metric"
PROP_RULE_UNIT = "odcs.rule.unit"
PROP_RULE_ARGUMENTS = "odcs.rule.arguments"
PROP_RULE_DIMENSION = "odcs.rule.dimension"
PROP_RULE_SEVERITY = "odcs.rule.severity"
PROP_RULE_BUSINESS_IMPACT = "odcs.rule.businessImpact"
PROP_RULE_TYPE = "odcs.rule.type"

# Rule/assertion scope: where in the contract the item originates.
SCOPE_SCHEMA = "schema"
SCOPE_PROPERTY = "property"

# Marker value written to PROP_ASSERTION for schema-compliance assertions.
ASSERTION_SCHEMA_COMPLIANCE = "schema-compliance"

# ODCS `logicalType` (and a few common `physicalType` spellings) -> the DataHub
# SchemaFieldDataType union member. ODCS logical/physical types are free-form
# strings, so this is a best-effort mapping; unmapped types fall back to
# NullType and are surfaced via SchemaBuildResult.unmapped_types rather than
# silently swallowed (a silent NullType hides real type information).
LOGICAL_TYPE_MAP: Dict[str, Type] = {
    "string": StringTypeClass,
    "text": StringTypeClass,
    "varchar": StringTypeClass,
    "char": StringTypeClass,
    "uuid": StringTypeClass,
    "integer": NumberTypeClass,
    "int": NumberTypeClass,
    "bigint": NumberTypeClass,
    "smallint": NumberTypeClass,
    "long": NumberTypeClass,
    "number": NumberTypeClass,
    "numeric": NumberTypeClass,
    "decimal": NumberTypeClass,
    "double": NumberTypeClass,
    "float": NumberTypeClass,
    "boolean": BooleanTypeClass,
    "bool": BooleanTypeClass,
    "date": DateTypeClass,
    "timestamp": TimeTypeClass,
    "timestamp_tz": TimeTypeClass,
    "timestamp_ntz": TimeTypeClass,
    "datetime": TimeTypeClass,
    "time": TimeTypeClass,
    "object": RecordTypeClass,
    "record": RecordTypeClass,
    "struct": RecordTypeClass,
    "json": RecordTypeClass,
    "variant": RecordTypeClass,
    "array": ArrayTypeClass,
    "list": ArrayTypeClass,
    "map": MapTypeClass,
    "bytes": BytesTypeClass,
    "binary": BytesTypeClass,
    "enum": EnumTypeClass,
}

# DataHub platform id -> the contract server fields (in order) that compose a
# fully-qualified physical name. `schema_` is the pydantic-safe name of the
# ODCS `schema` server field. Platforms absent here (e.g. oracle, whose ODCS
# server entry carries only host/port/serviceName) cannot be composed — a
# dotted physicalName or an explicit override is needed.
PLATFORM_NAME_PARTS: Dict[str, Tuple[str, ...]] = {
    "postgres": ("database", "schema_"),
    "redshift": ("database", "schema_"),
    "mssql": ("database", "schema_"),
    "snowflake": ("database", "schema_"),
    "trino": ("catalog", "schema_"),
    "databricks": ("catalog", "schema_"),
    "bigquery": ("project", "dataset"),
    "mysql": ("database",),
}
