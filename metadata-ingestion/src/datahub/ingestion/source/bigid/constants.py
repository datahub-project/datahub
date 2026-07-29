from typing import Dict, List, Set, Tuple

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_data_platform_urn,
)
from datahub.metadata.urns import GlossaryNodeUrn

# BigID ds-connection `type` → DataHub platform name.
# Source: `type` field values from GET /api/v1/ds-connections; verified against BigID 6.x.
BIGID_TYPE_TO_PLATFORM: Dict[str, str] = {
    # Relational databases
    "rdb-mysql": "mysql",
    "rdb-postgresql": "postgres",
    "rdb-mssql": "mssql",
    "rdb-oracle": "oracle",
    "rdb-db2": "db2",
    "rdb-redshift": "redshift",
    "rdb-hive": "hive",
    "rdb-teradata": "teradata",
    # Cloud warehouses / lakehouses
    "snowflake": "snowflake",
    "gcp-big-query": "bigquery",
    "databricks-v2": "databricks",
    # Object / file storage
    "s3-v2": "s3",
    # SaaS / other structured sources
    "salesforce": "salesforce",
    "kafka": "kafka",
    # Collaboration / document stores (unstructured; platform name used for URN only)
    "sharepoint-online-v2": "sharepoint",
    "confluence-v2": "confluence",
    # Partner integrations
    "mongodb": "mongodb",
    "azure-sql": "mssql",
    "sap-hana": "hana",
    "adls-v2": "abs",
    # Intentionally unmapped (no standard DataHub platform name):
    #   smb_v2, gdrive-v2, onedrive-v2, o365-outlook-v2, sap-successfactors-v2
    #   amazon-sagemaker, azure-openai, openai, hugging-face, atlas-vectorsearch
}

# Platforms whose identifiers are lowercased for canonical URN form.
LOWERCASE_PLATFORMS: Set[str] = {"snowflake", "bigquery", "redshift"}

# BigID confidence rank → numeric confidence, compared against minimum_confidence_threshold.
CONFIDENCE_RANK_FLOATS: Dict[str, float] = {"HIGH": 0.75, "MEDIUM": 0.50, "LOW": 0.25}

# Native-type substrings matched (case-insensitively) when mapping a BigID column type to a
# DataHub SchemaFieldDataType. Order matters: the first group whose token appears in the
# native type wins.
FIELD_TYPE_STRING_TOKENS: Tuple[str, ...] = (
    "varchar",
    "char",
    "text",
    "string",
    "nvarchar",
    "clob",
)
FIELD_TYPE_NUMERIC_TOKENS: Tuple[str, ...] = (
    "int",
    "bigint",
    "smallint",
    "tinyint",
    "number",
    "numeric",
    "decimal",
    "float",
    "double",
    "real",
)
FIELD_TYPE_BOOLEAN_TOKENS: Tuple[str, ...] = ("bool", "boolean", "bit")
FIELD_TYPE_TIME_TOKENS: Tuple[str, ...] = ("date", "time", "timestamp", "datetime")
FIELD_TYPE_BYTES_TOKENS: Tuple[str, ...] = ("bytes", "binary", "blob", "varbinary")

# BigID columnProfile.inferredDataType values that carry numeric min/max/mean/stdev stats
# (exact match, unlike the substring matching above).
PROFILE_NUMERIC_TYPES: Tuple[str, ...] = ("numeric", "integer", "float", "number")


# Glossary nodes that anchor emitted terms. URNs are GUIDs derived from a stable path key
# (the same datahub_guid convention the business-glossary source uses), so they are opaque
# but deterministic across runs. The readable label lives on GlossaryNodeInfo.name.
def _glossary_node_urn(path: str) -> str:
    return GlossaryNodeUrn(datahub_guid({"path": path})).urn()


BIGID_ROOT_GLOSSARY_NODE_URN = _glossary_node_urn("bigid")
BIGID_IDSOR_GLOSSARY_NODE_URN = _glossary_node_urn("bigid.idsor")
BIGID_CLASSIFIER_GLOSSARY_NODE_URN = _glossary_node_urn("bigid.classifier")

# GlossaryTermInfo.termSource value: BigID terms originate outside DataHub.
TERM_SOURCE_EXTERNAL = "EXTERNAL"

# BigID glossary item_type for out-of-the-box Personal Data Items. These are always
# emitted because column enrichment references their term URNs.
PERSONAL_DATA_ITEM_TYPE = "Personal Data Item"

# BigID tag name fragment routed to a structured property rather than a Tag entity.
RISK_SCORE_TAG_NAME = "riskScore"

# Definition of the StructuredProperty the riskScore tag is emitted as.
RISK_SCORE_PROPERTY_QUALIFIED_NAME = "bigid.riskScore"
RISK_SCORE_PROPERTY_DISPLAY_NAME = "BigID Risk Score"
RISK_SCORE_PROPERTY_DESCRIPTION = "Numeric risk score from BigID (0–100)."
RISK_SCORE_PROPERTY_VALUE_TYPE = "urn:li:dataType:datahub.number"
RISK_SCORE_PROPERTY_ENTITY_TYPES: List[str] = ["urn:li:entityType:datahub.dataset"]

# DataHub data platform name for BigID; the data-platform URN form is the
# provenance recorded on GlossaryTerm attributions.
BIGID_PLATFORM_NAME = "bigid"
BIGID_DATA_PLATFORM_URN = make_data_platform_urn(BIGID_PLATFORM_NAME)

# BigID attributeDetails `name` prefixes. MD:: must be tested before the bare
# classifier prefix since it is a longer match on the same string.
CLASSIFIER_PREFIX = "classifier."
CLASSIFIER_MD_PREFIX = "classifier.MD::"
BUSINESS_TERM_PREFIX = "businessTerm."

# BigID attributeDetails `type` value marking an IDSoR correlation finding.
IDSOR_ATTRIBUTE_TYPE = "IDSoR Attribute"

# BigID tag scoping: only OBJECT-scoped tags become dataset-level Tag entities.
TAG_TYPE_OBJECT = "OBJECT"
# BigID applicationType routed to the riskScore structured property.
APP_TYPE_RISK = "risk"

# BigID scanner_type_group value for column-bearing (structured) sources.
SCANNER_TYPE_STRUCTURED = "structured"

# Internal classifier-type discriminator used when building MetadataAttribution.
CLF_TYPE_VALUE = "value"
CLF_TYPE_METADATA = "metadata"
CLF_TYPE_BUSINESS_TERM = "business_term"
CLF_TYPE_IDSOR = "idsor_attribute"
