# Platform identifiers
HIGHTOUCH_PLATFORM = "hightouch"

# HTTP configuration
HTTP_METHOD_GET = "GET"
HTTP_PROTOCOL_HTTP = "http://"
HTTP_PROTOCOL_HTTPS = "https://"
HTTP_HEADER_AUTHORIZATION = "Authorization"
HTTP_HEADER_CONTENT_TYPE = "Content-Type"
HTTP_CONTENT_TYPE_JSON = "application/json"

# HTTP retry configuration
HTTP_RETRY_MAX_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_FACTOR = 1
HTTP_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
HTTP_RETRY_ALLOWED_METHODS = [HTTP_METHOD_GET]

# API pagination configuration
API_PAGINATION_DEFAULT_LIMIT = 100
API_PAGINATION_INITIAL_OFFSET = 0

# API response field names
API_RESPONSE_FIELD_DATA = "data"
API_RESPONSE_FIELD_HAS_MORE = "hasMore"

# Container types for organization
CONTAINER_TYPE_MODELS = "Models"
CONTAINER_TYPE_SYNCS = "Syncs"

# Assertion types
ASSERTION_TYPE_HIGHTOUCH_CONTRACT = "hightouch_contract"

# API endpoints
API_ENDPOINT_WORKSPACES = "workspaces"
API_ENDPOINT_SOURCES = "sources"
API_ENDPOINT_MODELS = "models"
API_ENDPOINT_DESTINATIONS = "destinations"
API_ENDPOINT_SYNCS = "syncs"
API_ENDPOINT_USERS = "users"
API_ENDPOINT_CONTRACTS = "events/contracts"

# Entity names for logging
ENTITY_NAME_WORKSPACE = "workspace"
ENTITY_NAME_SOURCE = "source"
ENTITY_NAME_MODEL = "model"
ENTITY_NAME_DESTINATION = "destination"
ENTITY_NAME_SYNC = "sync"
ENTITY_NAME_SYNC_RUN = "sync run"
ENTITY_NAME_USER = "user"
ENTITY_NAME_CONTRACT = "contract"
ENTITY_NAME_CONTRACT_RUN = "contract run"

# Mapping of Hightouch source types to DataHub platform names
# Reference: https://hightouch.com/docs/getting-started/concepts/#sources
KNOWN_SOURCE_PLATFORM_MAPPING = {
    # Data Warehouses
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "synapse": "mssql",
    "athena": "athena",
    # Databases
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mssql": "mssql",
    "sql_server": "mssql",
    "azure_sql": "mssql",
    "oracle": "oracle",
    "mongodb": "mongodb",
    "dynamodb": "dynamodb",
    # BI & Analytics Tools
    "looker": "looker",
    "tableau": "tableau",
    "metabase": "metabase",
    "mode": "mode",
    "sigma": "sigma",
    # Cloud Storage
    "s3": "s3",
    "gcs": "gcs",
    "azure_blob": "abs",
    "azure_blob_storage": "abs",
    "azure_storage": "abs",
    "adls": "abs",
    "adls_gen1": "abs",
    "adls_gen2": "abs",
    "azure_data_lake": "abs",
    "azure_data_lake_storage": "abs",
    # SaaS & Other
    "salesforce": "salesforce",
    "google_sheets": "google-sheets",
    "airtable": "airtable",
    "google_analytics": "google-analytics",
    "hubspot": "hubspot",
}

# Mapping of Hightouch destination types to DataHub platform names
# Reference: https://hightouch.com/docs/destinations/overview/
KNOWN_DESTINATION_PLATFORM_MAPPING = {
    # Data Warehouses & Databases
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mssql": "mssql",
    # Cloud Storage
    "s3": "s3",
    "gcs": "gcs",
    "azure_blob": "abs",
    "azure_blob_storage": "abs",
    "azure_storage": "abs",
    "adls": "abs",
    "adls_gen1": "abs",
    "adls_gen2": "abs",
    "azure_data_lake": "abs",
    "azure_data_lake_storage": "abs",
    # CRM & Sales
    "salesforce": "salesforce",
    "hubspot": "hubspot",
    "zendesk": "zendesk",
    "pipedrive": "pipedrive",
    "outreach": "outreach",
    "salesloft": "salesloft",
    # Marketing Automation
    "braze": "braze",
    "iterable": "iterable",
    "customer_io": "customerio",
    "marketo": "marketo",
    "klaviyo": "klaviyo",
    "mailchimp": "mailchimp",
    "activecampaign": "activecampaign",
    "eloqua": "eloqua",
    "sendgrid": "sendgrid",
    # Analytics & Product
    "segment": "segment",
    "mixpanel": "mixpanel",
    "amplitude": "amplitude",
    "google_analytics": "google-analytics",
    "heap": "heap",
    "pendo": "pendo",
    "intercom": "intercom",
    # Advertising
    "facebook_ads": "facebook",
    "google_ads": "google-ads",
    "linkedin_ads": "linkedin",
    "snapchat_ads": "snapchat",
    "tiktok_ads": "tiktok",
    "pinterest_ads": "pinterest",
    "twitter_ads": "twitter",
    # Support & Customer Success (additional)
    "freshdesk": "freshdesk",
    "kustomer": "kustomer",
    # Collaboration & Productivity
    "google_sheets": "google-sheets",
    "airtable": "airtable",
    "slack": "slack",
    # Payment & Finance
    "stripe": "stripe",
    "chargebee": "chargebee",
    # Other
    "webhook": "http",
    "http": "http",
}
