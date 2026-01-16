# Platform identifiers
CORPUSER_DATAHUB = "urn:li:corpuser:datahub"
HIGHTOUCH_PLATFORM = "hightouch"

# Container types for organization
CONTAINER_TYPE_MODELS = "Models"
CONTAINER_TYPE_SYNCS = "Syncs"

# Assertion types
ASSERTION_TYPE_HIGHTOUCH_CONTRACT = "hightouch_contract"

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
