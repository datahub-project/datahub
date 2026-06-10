"""
Realistic vocabulary for DataHub search perf-test data generation.

All lists are deterministically ordered — do NOT sort them at import time.
Generators sample from these lists using a seeded Rng so output is reproducible.
"""

# ── Platforms ─────────────────────────────────────────────────────────────────
PLATFORMS: list[str] = [
    "snowflake", "bigquery", "redshift", "mysql", "postgres", "hive",
]

# ── Database names (per platform flavour) ────────────────────────────────────
DATABASE_NAMES: list[str] = [
    "sales_db", "marketing_db", "analytics_db", "finance_db", "operations_db",
    "product_db", "customer_db", "logistics_db", "hr_db", "data_platform_db",
    "growth_db", "risk_db", "compliance_db", "supply_chain_db", "billing_db",
]

# ── Schema layer names ───────────────────────────────────────────────────────
SCHEMA_LAYERS: list[str] = [
    "raw", "staging", "trusted", "curated", "analytics",
    "mart", "gold", "silver", "bronze", "reporting",
]

# ── Table domain prefixes ────────────────────────────────────────────────────
TABLE_PREFIXES: list[str] = [
    "orders", "customers", "products", "payments", "sessions",
    "events", "inventory", "users", "transactions", "campaigns",
    "subscriptions", "accounts", "leads", "opportunities", "contracts",
    "invoices", "shipments", "returns", "reviews", "tickets",
    "employees", "projects", "incidents", "alerts", "pipelines",
    "features", "models", "metrics", "reports", "segments",
]

# ── Table type suffixes ──────────────────────────────────────────────────────
TABLE_SUFFIXES: list[str] = [
    "_fact", "_dim", "_staging", "_raw", "_mart",
    "_agg", "_summary", "_history", "_snapshot", "_delta",
]

# ── Column name pool (~500 entries, domain-tagged for realism) ───────────────
# Organized in semantic groups so that per-dataset column sampling produces
# coherent schemas rather than random noise.

COLUMN_POOL: list[str] = [
    # Identity & keys
    "id", "uuid", "guid", "external_id", "reference_id", "correlation_id",
    "parent_id", "ancestor_id", "root_id", "sequence_id", "batch_id",
    "session_id", "request_id", "trace_id", "span_id", "record_id",
    "entity_id", "object_id", "item_id", "row_id", "primary_key",

    # User / customer
    "user_id", "customer_id", "account_id", "subscriber_id", "member_id",
    "client_id", "contact_id", "lead_id", "prospect_id", "buyer_id",
    "seller_id", "vendor_id", "merchant_id", "partner_id", "agent_id",
    "employee_id", "manager_id", "owner_id", "creator_id", "assignee_id",
    "reviewer_id", "approver_id", "sponsor_id", "referrer_id",

    # Names & identity
    "name", "first_name", "last_name", "full_name", "display_name",
    "username", "nickname", "alias", "handle", "slug",
    "company_name", "brand_name", "product_name", "category_name",
    "legal_name", "preferred_name", "maiden_name",

    # Contact
    "email", "email_address", "secondary_email", "phone", "phone_number",
    "mobile", "mobile_number", "fax", "website", "url", "domain",
    "linkedin_url", "twitter_handle",

    # Location
    "address", "address_line_1", "address_line_2", "street", "street_name",
    "street_number", "city", "town", "municipality", "district", "county",
    "region", "state", "province", "country", "country_code",
    "zip_code", "postal_code", "latitude", "longitude",
    "geo_lat", "geo_lng", "timezone", "locale", "language_code",

    # Temporal
    "created_at", "updated_at", "deleted_at", "modified_at", "accessed_at",
    "published_at", "started_at", "ended_at", "completed_at", "canceled_at",
    "scheduled_at", "processed_at", "received_at", "submitted_at", "approved_at",
    "rejected_at", "expired_at", "activated_at", "deactivated_at",
    "last_login_at", "last_seen_at", "last_modified_at",
    "birth_date", "hire_date", "termination_date", "due_date",
    "event_date", "report_date", "transaction_date", "effective_date",
    "settlement_date", "posting_date", "value_date",
    "year", "month", "day", "quarter", "week", "hour", "minute", "second",
    "fiscal_year", "fiscal_quarter", "fiscal_period", "fiscal_month",
    "date_key", "time_key", "datetime_key",

    # Financial
    "amount", "total", "subtotal", "price", "unit_price", "list_price",
    "sale_price", "retail_price", "wholesale_price", "cost_price",
    "discount", "discount_amount", "discount_rate", "discount_percent",
    "tax", "tax_amount", "tax_rate", "tax_code", "vat", "vat_amount",
    "revenue", "gross_revenue", "net_revenue", "deferred_revenue",
    "cost", "unit_cost", "total_cost", "cogs", "overhead_cost",
    "profit", "gross_profit", "net_profit", "operating_profit",
    "margin", "gross_margin", "net_margin", "contribution_margin",
    "fee", "commission", "royalty", "refund", "credit", "debit",
    "balance", "opening_balance", "closing_balance", "available_balance",
    "payment_amount", "invoice_amount", "charge_amount", "billing_amount",
    "currency", "currency_code", "exchange_rate", "fx_rate",
    "budget", "forecast", "actuals", "variance", "target", "quota",
    "mrr", "arr", "acv", "ltv", "cac", "arpu",

    # Product / inventory
    "product_id", "sku", "barcode", "upc", "ean", "isbn", "asin",
    "quantity", "quantity_on_hand", "quantity_ordered", "quantity_shipped",
    "quantity_returned", "quantity_reserved", "quantity_available",
    "weight", "height", "width", "length", "volume", "size",
    "color", "material", "brand", "manufacturer", "supplier", "vendor",
    "stock_level", "inventory_level", "reorder_point", "safety_stock",
    "unit", "unit_of_measure", "pack_size", "case_count",
    "shelf_life", "expiry_date", "manufacture_date",

    # Order / transaction
    "order_id", "order_number", "order_line_id", "transaction_id",
    "invoice_id", "payment_id", "shipment_id", "return_id",
    "tracking_number", "reference_number", "confirmation_number",
    "order_date", "ship_date", "delivery_date", "estimated_delivery_date",
    "payment_method", "payment_status", "payment_terms",

    # Status / state / flags
    "status", "stage", "phase", "step", "sub_status",
    "is_active", "is_deleted", "is_verified", "is_enabled", "is_disabled",
    "is_approved", "is_published", "is_featured", "is_premium", "is_trial",
    "is_archived", "is_locked", "is_flagged", "is_blocked", "is_suspended",
    "is_internal", "is_external", "is_test", "is_demo", "is_sample",
    "is_primary", "is_default", "is_latest", "is_current",

    # Category / classification
    "type", "category", "subcategory", "classification", "segment",
    "channel", "source", "medium", "campaign", "referrer", "attribution",
    "label", "tag", "group", "tier", "level", "rank", "priority",
    "department", "division", "business_unit", "cost_center", "profit_center",
    "vertical", "industry", "sector", "market",

    # Metrics / analytics
    "count", "total_count", "unique_count", "distinct_count", "row_count",
    "views", "impressions", "clicks", "conversions", "sessions",
    "pageviews", "visits", "hits",
    "bounce_rate", "click_through_rate", "conversion_rate", "open_rate",
    "score", "rating", "percentile", "quartile",
    "average", "weighted_average", "mean", "median", "mode",
    "std_dev", "min_value", "max_value", "sum_value",
    "p50", "p90", "p95", "p99",
    "rolling_average", "moving_average", "cumulative_sum",
    "retention_rate", "churn_rate", "growth_rate", "error_rate",
    "success_rate", "failure_rate", "completion_rate", "abandonment_rate",
    "nps_score", "csat_score", "health_score",

    # Content / text
    "title", "description", "summary", "body", "content", "text",
    "notes", "comments", "remarks", "reason", "justification",
    "message", "subject", "excerpt", "abstract", "bio",
    "template", "format", "pattern", "schema",
    "html_content", "markdown_content",

    # System / technical
    "version", "revision", "build_number", "release", "checksum", "hash",
    "encoding", "compression", "content_type", "mime_type",
    "file_name", "file_path", "file_size", "file_type", "file_extension",
    "ip_address", "ipv4", "ipv6", "mac_address",
    "user_agent", "device_id", "device_type", "browser", "os",
    "app_version", "sdk_version", "api_version",
    "environment", "zone", "cluster", "host", "port",
    "namespace", "tenant_id", "org_id", "workspace_id",
    "endpoint", "method", "protocol", "scheme",
    "duration_ms", "latency_ms", "response_time_ms",
    "cpu_percent", "memory_mb", "disk_gb", "network_bytes",
    "retry_count", "attempt_number",

    # ML / data science
    "feature_value", "prediction", "probability", "confidence",
    "ground_truth", "predicted_label", "actual_label",
    "model_id", "model_version", "experiment_id", "run_id",
    "embedding_vector", "cluster_id", "centroid_distance",

    # Audit / governance
    "created_by", "updated_by", "deleted_by", "approved_by", "reviewed_by",
    "owned_by", "assigned_to", "reported_by",
    "source_system", "source_table", "source_file",
    "data_quality_score", "completeness_score", "accuracy_score",
    "sensitivity_level", "classification_level", "retention_period",
    "gdpr_basis", "ccpa_category",
]

# ── Realistic tag names (~30) ────────────────────────────────────────────────
TAG_NAMES: list[str] = [
    "pii", "sensitive", "confidential", "internal", "public",
    "financial", "customer-data", "product-data", "operational",
    "deprecated", "experimental", "certified", "authoritative",
    "raw", "curated", "enriched", "aggregated",
    "daily-refresh", "hourly-refresh", "real-time",
    "gdpr", "ccpa", "hipaa", "sox",
    "high-value", "business-critical", "tier-1",
    "ml-feature", "analytics-ready", "reporting",
]

# ── Business domains ─────────────────────────────────────────────────────────
DOMAIN_NAMES: list[str] = [
    "Sales", "Marketing", "Finance", "Engineering", "Operations",
    "Product", "Customer Success", "Legal & Compliance", "HR", "Data Platform",
]

# ── Owner identities (corpuser URNs) ────────────────────────────────────────
OWNER_NAMES: list[str] = [
    "alice.wang", "bob.chen", "carol.smith", "david.kim", "eve.patel",
    "frank.garcia", "grace.lee", "henry.brown", "iris.zhang", "jack.wilson",
    "kate.anderson", "liam.taylor", "mia.johnson", "noah.harris", "olivia.martin",
    "peter.thompson", "quinn.davis", "rachel.miller", "sam.white", "tara.jackson",
    "uma.thomas", "victor.moore", "wendy.clark", "xavier.lewis", "yuki.hall",
    "zach.robinson", "anna.walker", "ben.young", "clara.allen", "dan.scott",
    "elena.green", "finn.baker", "gina.nelson", "hugo.carter", "isabella.mitchell",
    "james.perez", "karen.roberts", "leo.turner", "maya.phillips", "nick.campbell",
    "ophelia.parker", "paul.evans", "qing.edwards", "rosa.collins", "simon.stewart",
    "tina.sanchez", "umar.morris", "viola.rogers", "will.reed", "xin.cook",
]

# ── Dataset description templates ────────────────────────────────────────────
DESCRIPTION_TEMPLATES: list[str] = [
    "Contains {entity} data from the {source} system, refreshed {cadence}.",
    "Aggregated {entity} metrics for the {domain} team, covering {period}.",
    "Raw {entity} records ingested from {source}, before any transformations.",
    "Curated {entity} dataset with quality checks applied, owned by {team}.",
    "Fact table for {entity} transactions, partitioned by {partition}.",
    "Dimension table for {entity} attributes, slowly changing type 2.",
    "Staging layer for {entity} data from {source}, awaiting validation.",
    "Daily snapshot of {entity} state, used for trend analysis.",
    "Historical {entity} records going back {period}, for audit purposes.",
    "Joined {entity} dataset combining data from {source} and other sources.",
    "Materialized view of {entity} for fast {domain} reporting.",
    "Event stream for {entity} interactions, streamed from Kafka.",
    "ML training data for {entity} predictions, prepared by {team}.",
    "Reconciliation table for {entity} discrepancies between {source} systems.",
    "Compliance extract of {entity} records required for {regulation}.",
]

DESCRIPTION_FILLERS: dict[str, list[str]] = {
    "entity": [
        "customer", "order", "payment", "product", "session",
        "event", "transaction", "user", "subscription", "lead",
        "invoice", "shipment", "review", "campaign", "contract",
    ],
    "source": [
        "Salesforce", "Stripe", "Shopify", "Segment", "HubSpot",
        "Zendesk", "Jira", "SAP", "Oracle ERP", "legacy CRM",
        "data warehouse", "operational database", "third-party API",
    ],
    "domain": [
        "Sales", "Marketing", "Finance", "Operations", "Product",
        "Customer Success", "Engineering", "BI", "Analytics",
    ],
    "cadence": [
        "hourly", "daily", "weekly", "every 15 minutes", "in near-real-time",
    ],
    "period": [
        "the last 2 years", "FY2024", "Q1-Q4 2025", "the past 90 days",
        "all historical records",
    ],
    "team": [
        "Data Platform", "Analytics Engineering", "Finance Ops", "Growth",
        "Revenue Operations", "Data Science",
    ],
    "partition": [
        "event_date", "created_at", "fiscal_year", "region",
    ],
    "regulation": [
        "GDPR", "CCPA", "HIPAA", "SOX", "PCI-DSS",
    ],
}

# ── Column native type strings (SQL-flavour) ─────────────────────────────────
# Maps to the SDK type class to use.

COLUMN_TYPE_MAP: list[tuple[str, str]] = [
    # (nativeDataType, sdk_type_class_name)
    ("VARCHAR(255)",   "string"),
    ("TEXT",           "string"),
    ("VARCHAR(64)",    "string"),
    ("CHAR(2)",        "string"),
    ("BIGINT",         "number"),
    ("INTEGER",        "number"),
    ("SMALLINT",       "number"),
    ("FLOAT",          "number"),
    ("DOUBLE",         "number"),
    ("DECIMAL(18,2)",  "number"),
    ("BOOLEAN",        "boolean"),
    ("DATE",           "date"),
    ("TIMESTAMP",      "date"),
    ("TIMESTAMP_NTZ",  "date"),
    ("TIMESTAMP_LTZ",  "date"),
]

# ── Native type → SDK class name lookup ──────────────────────────────────────
TYPE_CLASS_MAP: dict[str, str] = {
    "string":  "StringTypeClass",
    "number":  "NumberTypeClass",
    "boolean": "BooleanTypeClass",
    "date":    "DateTypeClass",
}
