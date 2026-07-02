# BigID ds-connection `type` → DataHub platform name.
# Source: `type` field values from GET /api/v1/ds-connections; verified against BigID 6.x.
BIGID_TYPE_TO_PLATFORM: dict[str, str] = {
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
    "sap-hana": "saphana",
    "adls-v2": "adls-gen2",
    # Intentionally unmapped (no standard DataHub platform name):
    #   smb_v2, gdrive-v2, onedrive-v2, o365-outlook-v2, sap-successfactors-v2
    #   amazon-sagemaker, azure-openai, openai, hugging-face, atlas-vectorsearch
}

# Platforms whose identifiers are lowercased for canonical URN form.
LOWERCASE_PLATFORMS: set[str] = {"snowflake", "bigquery", "redshift"}

# Glossary nodes that anchor emitted terms.
BIGID_ROOT_GLOSSARY_NODE_URN = "urn:li:glossaryNode:bigid"
BIGID_IDSOR_GLOSSARY_NODE_URN = "urn:li:glossaryNode:bigid.idsor"

# BigID tag name fragment routed to a structured property rather than a Tag entity.
RISK_SCORE_TAG_NAME = "riskScore"

# Actor stamped on every aspect this connector emits.
DATAHUB_ACTOR_URN = "urn:li:corpuser:datahub"
# Provenance recorded on GlossaryTerm attributions.
BIGID_DATA_PLATFORM_URN = "urn:li:dataPlatform:bigid"

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
