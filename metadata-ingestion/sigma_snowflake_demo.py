"""
Demo: Sigma Analytics connected to Snowflake data warehouse.

Scenario: "AcrylMart" e-commerce company uses Snowflake as their data warehouse
and Sigma for business intelligence dashboards. This emits realistic metadata
reflecting what the DataHub Sigma + Snowflake connectors would produce.

Run with:
  venv/bin/python sigma_snowflake_demo.py
"""

import datetime
import logging
import os
import sys

import yaml

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatahubKey, DatabaseKey, SchemaKey, gen_containers
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.container import ContainerProperties
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ContainerClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DateTypeClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    NullTypeClass,
    InputFieldClass,
    InputFieldsClass,
    NumberTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TimeStampClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

_FALLBACK_GMS_URL = "http://localhost:8080"
_FALLBACK_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjFiY2U1MGViLThkZWMtNGYyZi1iMTZhLTJlODY3ZmNiMWYwMCIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3ODM2ODEyMDQsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.CcwrdIWIfQSmb00ECJUYIF82mH6si3eqTsocxQwNXfc"


def _resolve_gms_config() -> tuple[str, str | None]:
    """Resolve GMS server + token from the active ~/.datahubenv block.

    The active (uncommented) ``gms:`` entry wins so the demo follows whichever
    instance `datahub init` last selected. Falls back to a local quickstart.
    """
    env_path = os.path.expanduser("~/.datahubenv")
    try:
        with open(env_path) as f:
            cfg = yaml.safe_load(f) or {}
    except (FileNotFoundError, yaml.YAMLError):
        return _FALLBACK_GMS_URL, _FALLBACK_TOKEN
    gms = cfg.get("gms") or {}
    server = gms.get("server")
    if server:
        return server.rstrip("/"), gms.get("token")
    return _FALLBACK_GMS_URL, _FALLBACK_TOKEN


DATAHUB_GMS_URL, DATAHUB_TOKEN = _resolve_gms_config()
PLATFORM_SNOWFLAKE = make_data_platform_urn("snowflake")
PLATFORM_SIGMA = make_data_platform_urn("sigma")
ENV = "PROD"

# Milliseconds since epoch for demo timestamps
_NOW_MS = int(datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000)
_CREATED_MS = int(
    datetime.datetime(2024, 1, 15, tzinfo=datetime.timezone.utc).timestamp() * 1000
)

_AUDIT_STAMP = ChangeAuditStampsClass(
    created=AuditStampClass(time=_CREATED_MS, actor="urn:li:corpuser:datahub"),
    lastModified=AuditStampClass(time=_NOW_MS, actor="urn:li:corpuser:datahub"),
)


# ---------------------------------------------------------------------------
# Snowflake container key helpers
# ---------------------------------------------------------------------------

DB_KEY = DatabaseKey(
    platform="snowflake",
    database="ACRYLMART_DB",
    env=ENV,
)

SALES_SCHEMA_KEY = SchemaKey.model_validate(
    {"platform": "snowflake", "database": "ACRYLMART_DB", "schema": "SALES", "env": ENV}
)

MARKETING_SCHEMA_KEY = SchemaKey.model_validate(
    {"platform": "snowflake", "database": "ACRYLMART_DB", "schema": "MARKETING", "env": ENV}
)


# ---------------------------------------------------------------------------
# Sigma identity constants  (realistic-looking base62 IDs / UUIDs)
# ---------------------------------------------------------------------------

SIGMA_WORKSPACE_ID = "acrylmart-ws-00000001"
SIGMA_WORKSPACE_CONTAINER_KEY_CLASS = None  # built below via datahub_guid

# Sigma Datasets (urlId = short base62 token from Sigma URL)
SIG_DS_ORDERS_ID = "4nKwLp9rXtqMsV2bCfJhAe"
SIG_DS_CUSTOMERS_ID = "7mRtQnZxVwPuKj3aDgEhBc"
SIG_DS_PRODUCTS_ID = "2pYsNmFvLxRqTk8cWbGiDo"
SIG_DS_ORDER_SUMMARY_ID = "9aJhXcVbMnPqLs5eRwKtYu"
SIG_DS_CAMPAIGNS_ID = "6tWdEfGhIjKlMnOpQrStUv"
SIG_DS_CONVERSIONS_ID = "3xZyAbCdEfGhIjKlMnOpQr"

# Sigma Workbooks (full UUIDs)
WORKBOOK_SALES_ID = "a1b2c3d4-0001-0002-0003-sales000001"
WORKBOOK_EXEC_ID = "e5f6g7h8-0001-0002-0003-exec0000002"

# Pages (short base62)
PAGE_REVENUE_ID = "pgRevOvw01"
PAGE_MARKETING_ID = "pgMktAna02"
PAGE_KPI_ID = "pgExecKpi03"

# Elements/Charts (short base62)
CHART_MONTHLY_REVENUE_ID = "chMonRev001"
CHART_REVENUE_BY_CAT_ID = "chRevCat002"
CHART_TOP_CUSTOMERS_ID = "chTopCust03"
CHART_CAMPAIGN_ROI_ID = "chCampROI04"
CHART_CONVERSION_RATE_ID = "chConvRate5"
CHART_TOTAL_REVENUE_KPI = "chTotRevKpi"
CHART_ACTIVE_CUSTOMERS_KPI = "chActCustKp"


def make_sigma_dataset_urn(url_id: str) -> str:
    return f"urn:li:dataset:({PLATFORM_SIGMA},{url_id},{ENV})"


def make_sigma_workbook_urn(workbook_id: str) -> str:
    return f"urn:li:dashboard:(sigma,{workbook_id})"


def make_sigma_page_urn(page_id: str) -> str:
    return f"urn:li:dashboard:(sigma,{page_id})"


def make_sigma_chart_urn(element_id: str) -> str:
    return f"urn:li:chart:(sigma,{element_id})"


def make_snowflake_table_urn(db: str, schema: str, table: str) -> str:
    return make_dataset_urn(
        platform="snowflake",
        name=f"{db}.{schema}.{table}".lower(),
        env=ENV,
    )


def emit_schema_field_statuses(
    emitter: DatahubRestEmitter, parent_urn: str, field_paths: list[str]
) -> None:
    """Materialize schemaField entities so column lineage survives ghost-filtering.

    DataHub's lineage queries drop relationships whose far endpoint is a "ghost"
    schemaField (one with no status). The UI's column-count query then reports
    numUpstream/numDownstream = 0 and greys the column out, so no column-level
    edge is drawn — even though the inputFields/fineGrained aspects are correct.
    Emitting Status(removed=False) on each participating schemaField makes it a
    real entity, so the edges are kept regardless of the
    schemaFieldLineageIgnoreStatus flag or which lineage view is used.
    """
    for field_path in field_paths:
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=f"urn:li:schemaField:({parent_urn},{field_path})",
                aspect=StatusClass(removed=False),
            )
        )


# ---------------------------------------------------------------------------
# Snowflake platform + containers
# ---------------------------------------------------------------------------


def emit_snowflake_platform_instance(emitter: DatahubRestEmitter) -> None:
    mcp = MetadataChangeProposalWrapper(
        entityUrn=f"urn:li:dataPlatformInstance:({PLATFORM_SNOWFLAKE},ACRYLMART_ACCOUNT)",
        aspect=DataPlatformInstanceClass(
            platform=PLATFORM_SNOWFLAKE,
            instance="ACRYLMART_ACCOUNT",
        ),
    )
    emitter.emit_mcp(mcp)


def emit_snowflake_containers(
    emitter: DatahubRestEmitter,
) -> list[MetadataChangeProposalWrapper]:
    mcps: list[MetadataChangeProposalWrapper] = []

    # Database container
    for wu in gen_containers(
        container_key=DB_KEY,
        name="ACRYLMART_DB",
        sub_types=["Database"],
        description="AcrylMart e-commerce data warehouse database",
        extra_properties={
            "platform": "snowflake",
            "env": ENV,
            "database": "ACRYLMART_DB",
        },
    ):
        emitter.emit(wu.metadata)
        mcps.append(wu.metadata)

    # SALES schema container
    for wu in gen_containers(
        container_key=SALES_SCHEMA_KEY,
        name="SALES",
        sub_types=["Schema"],
        description="Transactional sales data — orders, customers, products",
        parent_container_key=DB_KEY,
        extra_properties={
            "platform": "snowflake",
            "env": ENV,
            "database": "ACRYLMART_DB",
            "schema": "SALES",
        },
    ):
        emitter.emit(wu.metadata)
        mcps.append(wu.metadata)

    # MARKETING schema container
    for wu in gen_containers(
        container_key=MARKETING_SCHEMA_KEY,
        name="MARKETING",
        sub_types=["Schema"],
        description="Marketing campaigns and conversion tracking",
        parent_container_key=DB_KEY,
        extra_properties={
            "platform": "snowflake",
            "env": ENV,
            "database": "ACRYLMART_DB",
            "schema": "MARKETING",
        },
    ):
        emitter.emit(wu.metadata)
        mcps.append(wu.metadata)

    return mcps


# ---------------------------------------------------------------------------
# Snowflake tables
# ---------------------------------------------------------------------------

_SF_FIELDS = {
    "orders": [
        ("ORDER_ID", "NUMBER(38,0)", NumberTypeClass(), True, "Unique order identifier"),
        ("CUSTOMER_ID", "NUMBER(38,0)", NumberTypeClass(), False, "FK to CUSTOMERS"),
        ("PRODUCT_ID", "NUMBER(38,0)", NumberTypeClass(), False, "FK to PRODUCTS"),
        ("AMOUNT", "NUMBER(18,2)", NumberTypeClass(), False, "Order total in USD"),
        ("ORDER_DATE", "DATE", DateTypeClass(), False, "Date the order was placed"),
        ("STATUS", "VARCHAR(32)", StringTypeClass(), False, "pending/shipped/delivered/cancelled"),
        ("REGION", "VARCHAR(64)", StringTypeClass(), False, "Shipping destination region"),
        ("DISCOUNT_PCT", "NUMBER(5,2)", NumberTypeClass(), True, "Discount applied, if any"),
    ],
    "customers": [
        ("CUSTOMER_ID", "NUMBER(38,0)", NumberTypeClass(), True, "Unique customer identifier"),
        ("FULL_NAME", "VARCHAR(256)", StringTypeClass(), False, "Customer full name"),
        ("EMAIL", "VARCHAR(256)", StringTypeClass(), False, "Contact email address"),
        ("SIGNUP_DATE", "DATE", DateTypeClass(), False, "Date the account was created"),
        ("COUNTRY", "VARCHAR(64)", StringTypeClass(), False, "Country of residence"),
        ("SEGMENT", "VARCHAR(32)", StringTypeClass(), False, "bronze/silver/gold/platinum"),
        ("LIFETIME_VALUE", "NUMBER(18,2)", NumberTypeClass(), True, "Total spend to date"),
        ("IS_ACTIVE", "BOOLEAN", BooleanTypeClass(), False, "Whether the account is active"),
    ],
    "products": [
        ("PRODUCT_ID", "NUMBER(38,0)", NumberTypeClass(), True, "Unique product identifier"),
        ("PRODUCT_NAME", "VARCHAR(256)", StringTypeClass(), False, "Display name"),
        ("CATEGORY", "VARCHAR(128)", StringTypeClass(), False, "Top-level product category"),
        ("SUBCATEGORY", "VARCHAR(128)", StringTypeClass(), True, "Product subcategory"),
        ("PRICE_USD", "NUMBER(10,2)", NumberTypeClass(), False, "List price in USD"),
        ("COST_USD", "NUMBER(10,2)", NumberTypeClass(), False, "Unit cost in USD"),
        ("SUPPLIER_ID", "NUMBER(38,0)", NumberTypeClass(), True, "FK to SUPPLIERS"),
    ],
    "campaigns": [
        ("CAMPAIGN_ID", "NUMBER(38,0)", NumberTypeClass(), True, "Unique campaign identifier"),
        ("CAMPAIGN_NAME", "VARCHAR(256)", StringTypeClass(), False, "Campaign display name"),
        ("CHANNEL", "VARCHAR(64)", StringTypeClass(), False, "email/paid_search/social/display"),
        ("START_DATE", "DATE", DateTypeClass(), False, "Campaign start date"),
        ("END_DATE", "DATE", DateTypeClass(), True, "Campaign end date (null = ongoing)"),
        ("BUDGET_USD", "NUMBER(18,2)", NumberTypeClass(), False, "Allocated budget in USD"),
        ("TARGET_SEGMENT", "VARCHAR(64)", StringTypeClass(), True, "Target customer segment"),
    ],
    "conversions": [
        ("CONVERSION_ID", "NUMBER(38,0)", NumberTypeClass(), True, "Unique conversion identifier"),
        ("CAMPAIGN_ID", "NUMBER(38,0)", NumberTypeClass(), False, "FK to CAMPAIGNS"),
        ("CUSTOMER_ID", "NUMBER(38,0)", NumberTypeClass(), False, "FK to CUSTOMERS"),
        ("CONVERSION_DATE", "DATE", DateTypeClass(), False, "Date the conversion occurred"),
        ("REVENUE_USD", "NUMBER(18,2)", NumberTypeClass(), False, "Revenue attributed to campaign"),
        ("CONVERSION_TYPE", "VARCHAR(32)", StringTypeClass(), False, "purchase/signup/trial"),
    ],
}

_SF_VIEW_FIELDS = {
    "order_summary": [
        ("MONTH", "DATE", DateTypeClass(), False, "First day of the reporting month"),
        ("REGION", "VARCHAR(64)", StringTypeClass(), False, "Shipping region"),
        ("TOTAL_REVENUE_USD", "NUMBER(18,2)", NumberTypeClass(), False, "Sum of order amounts"),
        ("ORDER_COUNT", "NUMBER(38,0)", NumberTypeClass(), False, "Number of orders placed"),
        ("AVG_ORDER_VALUE_USD", "NUMBER(18,2)", NumberTypeClass(), False, "Average order value"),
        ("UNIQUE_CUSTOMERS", "NUMBER(38,0)", NumberTypeClass(), False, "Distinct buying customers"),
    ],
}


def _make_schema_fields(
    field_defs: list,
) -> list[SchemaFieldClass]:
    fields = []
    for name, native_type, type_obj, nullable, description in field_defs:
        fields.append(
            SchemaFieldClass(
                fieldPath=name,
                type=SchemaFieldDataTypeClass(type=type_obj),
                nativeDataType=native_type,
                nullable=nullable,
                description=description,
            )
        )
    return fields


def emit_snowflake_tables(emitter: DatahubRestEmitter) -> None:
    tables = [
        # (db, schema, table_name, sub_type, description, field_key, upstream_view_of)
        (
            "acrylmart_db",
            "sales",
            "orders",
            "Table",
            "Raw order transactions from the storefront",
            "orders",
            None,
        ),
        (
            "acrylmart_db",
            "sales",
            "customers",
            "Table",
            "Registered customer accounts and segmentation",
            "customers",
            None,
        ),
        (
            "acrylmart_db",
            "sales",
            "products",
            "Table",
            "Product catalog with pricing and category hierarchy",
            "products",
            None,
        ),
        (
            "acrylmart_db",
            "marketing",
            "campaigns",
            "Table",
            "Paid and organic marketing campaign definitions",
            "campaigns",
            None,
        ),
        (
            "acrylmart_db",
            "marketing",
            "conversions",
            "Table",
            "Attribution events linking campaigns to customer conversions",
            "conversions",
            None,
        ),
    ]
    views = [
        (
            "acrylmart_db",
            "sales",
            "order_summary",
            "View",
            "Monthly revenue summary aggregated from ORDERS — refreshed nightly",
            "order_summary",
            "orders",  # this view's upstream
        ),
    ]

    for db, schema, table, sub_type, description, field_key, upstream_of in [
        *tables,
        *views,
    ]:
        dataset_urn = make_snowflake_table_urn(db, schema, table)
        schema_key = SALES_SCHEMA_KEY if schema == "sales" else MARKETING_SCHEMA_KEY

        field_defs = _SF_VIEW_FIELDS.get(field_key) or _SF_FIELDS.get(field_key) or []
        fields = _make_schema_fields(field_defs)

        # status
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=StatusClass(removed=False),
            )
        )

        # datasetProperties
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=table.upper(),
                    qualifiedName=f"{db.upper()}.{schema.upper()}.{table.upper()}",
                    description=description,
                    externalUrl=f"https://app.snowflake.com/acrylmart/#/data/databases/{db.upper()}/schemas/{schema.upper()}/table/{table.upper()}",
                    created=TimeStampClass(time=_CREATED_MS),
                    lastModified=TimeStampClass(time=_NOW_MS),
                    customProperties={
                        "IS_ICEBERG": "false",
                        "RETENTION_TIME": "1",
                    },
                ),
            )
        )

        # schemaMetadata
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=f"{db}.{schema}.{table}",
                    platform=PLATFORM_SNOWFLAKE,
                    version=0,
                    hash="",
                    platformSchema={"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
                    fields=fields,
                ),
            )
        )
        emit_schema_field_statuses(
            emitter, dataset_urn, [f.fieldPath for f in fields]
        )

        # subTypes
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[sub_type]),
            )
        )

        # container
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ContainerClass(container=schema_key.as_urn()),
            )
        )

        # browsePathsV2
        db_label = "ACRYLMART_DB"
        schema_label = schema.upper()
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(
                            id=DB_KEY.as_urn(), urn=DB_KEY.as_urn()
                        ),
                        BrowsePathEntryClass(
                            id=schema_key.as_urn(), urn=schema_key.as_urn()
                        ),
                    ]
                ),
            )
        )

        # view lineage — ORDER_SUMMARY derives all its columns from ORDERS.
        # Add fine-grained column mappings so the full column path is traceable in the UI.
        if sub_type == "View" and upstream_of:
            upstream_urn = make_snowflake_table_urn(db, schema, upstream_of)
            view_field_defs = _SF_VIEW_FIELDS.get(field_key, [])
            # ORDER_SUMMARY columns map to the matching ORDERS columns by name.
            upstream_field_defs = _SF_FIELDS.get(upstream_of, [])
            upstream_col_names = {fd[0] for fd in upstream_field_defs}
            view_fgl = [
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[f"urn:li:schemaField:({upstream_urn},{vfd[0]})"]
                    if vfd[0] in upstream_col_names
                    else [f"urn:li:schemaField:({upstream_urn},AMOUNT)"],  # aggregated columns map to source
                    downstreams=[f"urn:li:schemaField:({dataset_urn},{vfd[0]})"],
                    confidenceScore=1.0,
                )
                for vfd in view_field_defs
            ]
            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=upstream_urn,
                                type="VIEW",
                            )
                        ],
                        fineGrainedLineages=view_fgl,
                    ),
                )
            )

        log.info("  snowflake %s: %s", sub_type.lower(), dataset_urn)


# ---------------------------------------------------------------------------
# Sigma workspace container
# ---------------------------------------------------------------------------


def _sigma_workspace_container_urn() -> str:
    from datahub.emitter.mce_builder import datahub_guid

    return f"urn:li:container:{datahub_guid({'platform': 'sigma', 'workspaceId': SIGMA_WORKSPACE_ID})}"


def emit_sigma_workspace(emitter: DatahubRestEmitter) -> None:
    workspace_urn = _sigma_workspace_container_urn()

    # status, containerProperties, subTypes, dataPlatformInstance, browsePathsV2.
    # Real connector does NOT include description or externalUrl on the workspace container.
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=workspace_urn,
            aspect=StatusClass(removed=False),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=workspace_urn,
            aspect=ContainerProperties(
                name="AcrylMart Analytics",
                created=TimeStampClass(time=_CREATED_MS),
                lastModified=TimeStampClass(time=_NOW_MS),
                customProperties={
                    "platform": "sigma",
                    "workspaceId": SIGMA_WORKSPACE_ID,
                },
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=workspace_urn,
            aspect=SubTypesClass(typeNames=["Sigma Workspace"]),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=workspace_urn,
            aspect=DataPlatformInstanceClass(platform=PLATFORM_SIGMA),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=workspace_urn,
            aspect=BrowsePathsV2Class(path=[]),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=workspace_urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:analytics-team",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            ),
        )
    )
    log.info("  sigma workspace: %s", workspace_urn)


# ---------------------------------------------------------------------------
# Sigma Datasets
# ---------------------------------------------------------------------------

# Sigma Dataset columns mirror the upstream Snowflake table columns.
# Typed StringType — the native representation Sigma uses for all fields.
_SIGMA_DATASET_COLUMNS: dict[str, list[str]] = {
    SIG_DS_ORDERS_ID: ["ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "AMOUNT", "ORDER_DATE", "STATUS", "REGION", "DISCOUNT_PCT"],
    SIG_DS_CUSTOMERS_ID: ["CUSTOMER_ID", "FULL_NAME", "EMAIL", "SIGNUP_DATE", "COUNTRY", "SEGMENT", "LIFETIME_VALUE", "IS_ACTIVE"],
    SIG_DS_PRODUCTS_ID: ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "SUBCATEGORY", "PRICE_USD", "COST_USD", "SUPPLIER_ID"],
    SIG_DS_ORDER_SUMMARY_ID: ["MONTH", "REGION", "TOTAL_REVENUE_USD", "ORDER_COUNT", "AVG_ORDER_VALUE_USD", "UNIQUE_CUSTOMERS"],
    SIG_DS_CAMPAIGNS_ID: ["CAMPAIGN_ID", "CAMPAIGN_NAME", "CHANNEL", "START_DATE", "END_DATE", "BUDGET_USD", "TARGET_SEGMENT"],
    SIG_DS_CONVERSIONS_ID: ["CONVERSION_ID", "CAMPAIGN_ID", "CUSTOMER_ID", "CONVERSION_DATE", "REVENUE_USD", "CONVERSION_TYPE"],
}

_SIGMA_DATASETS = [
    (
        SIG_DS_ORDERS_ID,
        "Orders",
        "Sigma dataset sourced from ACRYLMART_DB.SALES.ORDERS",
        make_snowflake_table_urn("acrylmart_db", "sales", "orders"),
    ),
    (
        SIG_DS_CUSTOMERS_ID,
        "Customers",
        "Sigma dataset sourced from ACRYLMART_DB.SALES.CUSTOMERS",
        make_snowflake_table_urn("acrylmart_db", "sales", "customers"),
    ),
    (
        SIG_DS_PRODUCTS_ID,
        "Products",
        "Sigma dataset sourced from ACRYLMART_DB.SALES.PRODUCTS",
        make_snowflake_table_urn("acrylmart_db", "sales", "products"),
    ),
    (
        SIG_DS_ORDER_SUMMARY_ID,
        "Order Summary",
        "Sigma dataset sourced from ACRYLMART_DB.SALES.ORDER_SUMMARY (monthly aggregations)",
        make_snowflake_table_urn("acrylmart_db", "sales", "order_summary"),
    ),
    (
        SIG_DS_CAMPAIGNS_ID,
        "Campaigns",
        "Sigma dataset sourced from ACRYLMART_DB.MARKETING.CAMPAIGNS",
        make_snowflake_table_urn("acrylmart_db", "marketing", "campaigns"),
    ),
    (
        SIG_DS_CONVERSIONS_ID,
        "Conversions",
        "Sigma dataset sourced from ACRYLMART_DB.MARKETING.CONVERSIONS",
        make_snowflake_table_urn("acrylmart_db", "marketing", "conversions"),
    ),
]


def emit_sigma_datasets(emitter: DatahubRestEmitter) -> None:
    workspace_urn = _sigma_workspace_container_urn()

    for url_id, name, description, snowflake_upstream_urn in _SIGMA_DATASETS:
        dataset_urn = make_sigma_dataset_urn(url_id)

        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=StatusClass(removed=False),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=name,
                    qualifiedName=name,
                    description=description,
                    externalUrl=f"https://app.sigmacomputing.com/acrylmart/d/{url_id}",
                    created=TimeStampClass(time=_CREATED_MS),
                    lastModified=TimeStampClass(time=_NOW_MS),
                    customProperties={
                        "datasetId": url_id,
                        "path": f"/AcrylMart Analytics/Datasets/{name}",
                    },
                ),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=["Sigma Dataset"]),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ContainerClass(container=workspace_urn),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(id=workspace_urn, urn=workspace_urn),
                    ]
                ),
            )
        )
        # Schema: Sigma datasets surface the same columns as their upstream Snowflake table.
        # The real connector does not auto-generate this, but the relationship is real and
        # required for column-level lineage to resolve in the DataHub UI.
        columns = _SIGMA_DATASET_COLUMNS.get(url_id, [])
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=f"sigma.{url_id}",
                    platform=PLATFORM_SIGMA,
                    version=0,
                    hash="",
                    platformSchema={"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
                    fields=[
                        SchemaFieldClass(
                            fieldPath=col,
                            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                            nativeDataType="String",
                            nullable=False,
                            recursive=False,
                            isPartOfKey=False,
                        )
                        for col in columns
                    ],
                ),
            )
        )
        emit_schema_field_statuses(emitter, dataset_urn, columns)

        # Fine-grained column lineage: Snowflake column → Sigma Dataset column (1:1 COPY).
        fine_grained = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[f"urn:li:schemaField:({snowflake_upstream_urn},{col})"],
                downstreams=[f"urn:li:schemaField:({dataset_urn},{col})"],
                confidenceScore=1.0,
            )
            for col in columns
        ]
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=snowflake_upstream_urn,
                            type="COPY",
                        )
                    ],
                    fineGrainedLineages=fine_grained,
                ),
            )
        )
        # Ownership — `ingest_owner=True` in the real connector; DATAOWNER type.
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner="urn:li:corpuser:analytics-team",
                            type=OwnershipTypeClass.DATAOWNER,
                        )
                    ]
                ),
            )
        )
        # globalTags: Sigma "badge" field maps to tags. Order Summary and Customers are "Trusted".
        if url_id in {SIG_DS_ORDER_SUMMARY_ID, SIG_DS_CUSTOMERS_ID}:
            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(
                        tags=[TagAssociationClass(tag="urn:li:tag:Trusted")]
                    ),
                )
            )
        log.info("  sigma dataset: %s → %s", name, dataset_urn)


# ---------------------------------------------------------------------------
# Column fields used by each chart.
#
# For column-level lineage to appear in the UI for charts, inputFields.schemaFieldUrn
# must point at the IMMEDIATE upstream column — i.e. the Sigma Dataset column.
# The Sigma Dataset already carries fineGrainedLineages wiring its own columns back
# to Snowflake, so the full two-hop path resolves automatically:
#   Snowflake column → Sigma Dataset column → Chart field
#
# The old broken pattern emitted (chart_urn, col) — self-referential, silently dropped.
# ---------------------------------------------------------------------------

# (chart_id → (sigma_dataset_url_id, [column_names]))
_CHART_COLUMNS: dict[str, tuple[str, list[str]]] = {
    CHART_MONTHLY_REVENUE_ID: (
        SIG_DS_ORDER_SUMMARY_ID,
        ["MONTH", "REGION", "TOTAL_REVENUE_USD", "ORDER_COUNT", "AVG_ORDER_VALUE_USD"],
    ),
    CHART_REVENUE_BY_CAT_ID: (
        SIG_DS_ORDERS_ID,
        ["ORDER_DATE", "PRODUCT_ID", "AMOUNT", "REGION", "STATUS"],
    ),
    CHART_TOP_CUSTOMERS_ID: (
        SIG_DS_CUSTOMERS_ID,
        ["CUSTOMER_ID", "FULL_NAME", "EMAIL", "SEGMENT", "LIFETIME_VALUE", "COUNTRY"],
    ),
    CHART_CAMPAIGN_ROI_ID: (
        SIG_DS_CAMPAIGNS_ID,
        ["CAMPAIGN_NAME", "CHANNEL", "BUDGET_USD", "START_DATE", "TARGET_SEGMENT"],
    ),
    CHART_CONVERSION_RATE_ID: (
        SIG_DS_CONVERSIONS_ID,
        ["CONVERSION_DATE", "CAMPAIGN_ID", "CONVERSION_TYPE", "REVENUE_USD", "CUSTOMER_ID"],
    ),
    CHART_TOTAL_REVENUE_KPI: (
        SIG_DS_ORDER_SUMMARY_ID,
        ["MONTH", "TOTAL_REVENUE_USD", "UNIQUE_CUSTOMERS"],
    ),
    CHART_ACTIVE_CUSTOMERS_KPI: (
        SIG_DS_CUSTOMERS_ID,
        ["CUSTOMER_ID", "IS_ACTIVE", "SEGMENT", "SIGNUP_DATE"],
    ),
}


def _make_input_fields(element_id: str) -> InputFieldsClass:
    """Build InputFieldsClass for a Sigma chart element.

    schemaFieldUrn points at the Sigma Dataset column (the chart's direct upstream),
    not the chart itself and not the warehouse.  This lets the UI draw column-level
    lineage edges: Snowflake col → Sigma Dataset col → Chart field.
    """
    entry = _CHART_COLUMNS.get(element_id)
    if not entry:
        return InputFieldsClass(fields=[])
    ds_url_id, columns = entry
    sigma_dataset_urn = make_sigma_dataset_urn(ds_url_id)
    fields = [
        InputFieldClass(
            schemaFieldUrn=f"urn:li:schemaField:({sigma_dataset_urn},{col})",
            schemaField=SchemaFieldClass(
                fieldPath=col,
                nullable=False,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="String",
                recursive=False,
                isPartOfKey=False,
            ),
        )
        for col in columns
    ]
    return InputFieldsClass(fields=fields)


# ---------------------------------------------------------------------------
# Sigma Data Models
# ---------------------------------------------------------------------------
# Data Models are a Sigma concept that lets users define reusable metric
# definitions (named formulas/aggregations) on top of warehouse tables.
# In DataHub: the Data Model is a container; each element is a dataset
# with a NullType schema where each field's description holds the formula.
# ---------------------------------------------------------------------------

DM_SALES_ID = "dm-sales-metrics-001"
DM_SALES_URL_ID = "SigDMSalesMet01"

_DM_ELEMENTS = [
    # (element_id, name, formula, upstream_snowflake_urn)
    (
        "dme-monthly-rev-001",
        "Monthly Revenue",
        "SUM([Orders/Amount]) BY [Orders/Order Date].[Month]",
        make_snowflake_table_urn("acrylmart_db", "sales", "orders"),
    ),
    (
        "dme-customer-ltv-002",
        "Customer LTV",
        "SUM([Customers/Lifetime Value])",
        make_snowflake_table_urn("acrylmart_db", "sales", "customers"),
    ),
    (
        "dme-conversion-rate-003",
        "Conversion Rate",
        "COUNT([Conversions/Conversion Id]) / COUNT_DISTINCT([Customers/Customer Id])",
        make_snowflake_table_urn("acrylmart_db", "marketing", "conversions"),
    ),
    (
        "dme-avg-order-value-004",
        "Avg Order Value",
        "SUM([Orders/Amount]) / COUNT([Orders/Order Id])",
        make_snowflake_table_urn("acrylmart_db", "sales", "orders"),
    ),
]


def _make_dm_container_urn() -> str:
    from datahub.emitter.mce_builder import datahub_guid

    return f"urn:li:container:{datahub_guid({'platform': 'sigma', 'workspaceId': SIGMA_WORKSPACE_ID, 'dataModelId': DM_SALES_ID})}"


def _make_dm_element_urn(element_id: str) -> str:
    return f"urn:li:dataset:({PLATFORM_SIGMA},{element_id},{ENV})"


def emit_sigma_data_models(emitter: DatahubRestEmitter) -> None:
    workspace_urn = _sigma_workspace_container_urn()
    dm_container_urn = _make_dm_container_urn()

    # --- Data Model Container ---
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=dm_container_urn,
            aspect=StatusClass(removed=False),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=dm_container_urn,
            aspect=ContainerProperties(
                name="Sales Metrics",
                description="Reusable metric definitions for the sales domain",
                externalUrl=f"https://app.sigmacomputing.com/acrylmart/data-model/{DM_SALES_URL_ID}",
                created=TimeStampClass(time=_CREATED_MS),
                lastModified=TimeStampClass(time=_NOW_MS),
                customProperties={
                    "dataModelId": DM_SALES_ID,
                    "dataModelUrlId": DM_SALES_URL_ID,
                    "latestVersion": "v1.2",
                    "isPersonalDataModel": "false",
                },
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=dm_container_urn,
            aspect=SubTypesClass(typeNames=["Sigma Data Model"]),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=dm_container_urn,
            aspect=ContainerClass(container=workspace_urn),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=dm_container_urn,
            aspect=BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(id=workspace_urn, urn=workspace_urn),
                ]
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=dm_container_urn,
            aspect=GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:Trusted")]
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=dm_container_urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:analytics-team",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            ),
        )
    )
    log.info("  sigma data model container: %s", dm_container_urn)

    # --- Data Model Elements ---
    for element_id, name, formula, upstream_urn in _DM_ELEMENTS:
        element_urn = _make_dm_element_urn(element_id)

        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=element_urn,
                aspect=StatusClass(removed=False),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=element_urn,
                aspect=DatasetPropertiesClass(
                    name=name,
                    qualifiedName=f"Sales Metrics/{name}",
                    externalUrl=f"https://app.sigmacomputing.com/acrylmart/data-model/{DM_SALES_URL_ID}",
                    customProperties={
                        "dataModelId": DM_SALES_ID,
                        "dataModelUrlId": DM_SALES_URL_ID,
                        "elementId": element_id,
                        "type": "metric",
                    },
                ),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=element_urn,
                aspect=SubTypesClass(typeNames=["Sigma Data Model Element"]),
            )
        )
        # Schema: NullType columns; formula stored in the field description.
        # This matches exactly how the real connector emits DM element schemas.
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=element_urn,
                aspect=SchemaMetadataClass(
                    schemaName=f"sigma_dm.{element_id}",
                    platform=PLATFORM_SIGMA,
                    version=0,
                    hash="",
                    platformSchema={"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
                    fields=[
                        SchemaFieldClass(
                            fieldPath=name.upper().replace(" ", "_"),
                            type=SchemaFieldDataTypeClass(type=NullTypeClass()),
                            nativeDataType="metric",
                            nullable=True,
                            description=formula,
                            recursive=False,
                            isPartOfKey=False,
                        )
                    ],
                ),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=element_urn,
                aspect=ContainerClass(container=dm_container_urn),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=element_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(id=workspace_urn, urn=workspace_urn),
                        BrowsePathEntryClass(id=dm_container_urn, urn=dm_container_urn),
                    ]
                ),
            )
        )
        # Lineage: DM element → warehouse table (the table the formula draws from).
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=element_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=upstream_urn,
                            type="TRANSFORMED",
                        )
                    ]
                ),
            )
        )
        log.info("  sigma dm element: %s (%s)", name, element_urn)


# ---------------------------------------------------------------------------
# Sigma Charts / Elements
# ---------------------------------------------------------------------------

_SIGMA_CHARTS = [
    # (element_id, title, viz_type, sigma_dataset_url_id, workbook_id, page_id)
    (
        CHART_MONTHLY_REVENUE_ID,
        "Monthly Revenue Trend",
        "LINE",
        SIG_DS_ORDER_SUMMARY_ID,
        WORKBOOK_SALES_ID,
        PAGE_REVENUE_ID,
    ),
    (
        CHART_REVENUE_BY_CAT_ID,
        "Revenue by Product Category",
        "BAR",
        SIG_DS_ORDERS_ID,
        WORKBOOK_SALES_ID,
        PAGE_REVENUE_ID,
    ),
    (
        CHART_TOP_CUSTOMERS_ID,
        "Top Customers by Lifetime Value",
        "TABLE",
        SIG_DS_CUSTOMERS_ID,
        WORKBOOK_SALES_ID,
        PAGE_REVENUE_ID,
    ),
    (
        CHART_CAMPAIGN_ROI_ID,
        "Campaign ROI by Channel",
        "BAR",
        SIG_DS_CAMPAIGNS_ID,
        WORKBOOK_SALES_ID,
        PAGE_MARKETING_ID,
    ),
    (
        CHART_CONVERSION_RATE_ID,
        "Conversion Rate Over Time",
        "LINE",
        SIG_DS_CONVERSIONS_ID,
        WORKBOOK_SALES_ID,
        PAGE_MARKETING_ID,
    ),
    (
        CHART_TOTAL_REVENUE_KPI,
        "Total Revenue (MTD)",
        "KPI",
        SIG_DS_ORDER_SUMMARY_ID,
        WORKBOOK_EXEC_ID,
        PAGE_KPI_ID,
    ),
    (
        CHART_ACTIVE_CUSTOMERS_KPI,
        "Active Customers (MTD)",
        "KPI",
        SIG_DS_CUSTOMERS_ID,
        WORKBOOK_EXEC_ID,
        PAGE_KPI_ID,
    ),
]


def emit_sigma_charts(emitter: DatahubRestEmitter) -> None:
    workspace_urn = _sigma_workspace_container_urn()

    for element_id, title, viz_type, ds_url_id, workbook_id, page_id in _SIGMA_CHARTS:
        chart_urn = make_sigma_chart_urn(element_id)
        dataset_urn = make_sigma_dataset_urn(ds_url_id)
        page_urn = make_sigma_page_urn(page_id)
        workbook_urn = make_sigma_workbook_urn(workbook_id)

        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=StatusClass(removed=False),
            )
        )
        # Real connector: lastModified is an empty ChangeAuditStampsClass (no timestamps on elements).
        # customProperties only has VizualizationType + type; workbookId/pageId are not present.
        # inputEdges mirrors inputs as EdgeClass list.
        workbook_title = next(
            t for wid, t, *_ in _SIGMA_WORKBOOKS if wid == workbook_id
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=ChartInfoClass(
                    title=title,
                    description="",
                    externalUrl=f"https://app.sigmacomputing.com/acrylmart/workbook/{workbook_id}",
                    inputs=[dataset_urn],
                    inputEdges=[EdgeClass(destinationUrn=dataset_urn, sourceUrn=chart_urn)],
                    lastModified=ChangeAuditStampsClass(),
                    customProperties={
                        "VizualizationType": viz_type,
                        "type": "chart",
                    },
                ),
            )
        )
        # Real connector browse path: workspace URN + workbook title string (no page, no URN for workbook).
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(id=workspace_urn, urn=workspace_urn),
                        BrowsePathEntryClass(id=workbook_title),
                    ]
                ),
            )
        )
        # Column-level fields: one InputFieldClass per column the chart references.
        # schemaFieldUrn points to the Sigma Dataset column (the chart's immediate upstream),
        # enabling two-hop CLL: Snowflake col → Sigma Dataset col → Chart field.
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=_make_input_fields(element_id),
            )
        )
        chart_entry = _CHART_COLUMNS.get(element_id)
        if chart_entry:
            emit_schema_field_statuses(emitter, chart_urn, chart_entry[1])
        log.info("  sigma chart: %s (%s)", title, chart_urn)


# ---------------------------------------------------------------------------
# Sigma Pages (modelled as dashboards)
# ---------------------------------------------------------------------------

_SIGMA_PAGES = [
    (
        PAGE_REVENUE_ID,
        "Revenue Overview",
        WORKBOOK_SALES_ID,
        [CHART_MONTHLY_REVENUE_ID, CHART_REVENUE_BY_CAT_ID, CHART_TOP_CUSTOMERS_ID],
    ),
    (
        PAGE_MARKETING_ID,
        "Marketing Analytics",
        WORKBOOK_SALES_ID,
        [CHART_CAMPAIGN_ROI_ID, CHART_CONVERSION_RATE_ID],
    ),
    (
        PAGE_KPI_ID,
        "KPI Summary",
        WORKBOOK_EXEC_ID,
        [CHART_TOTAL_REVENUE_KPI, CHART_ACTIVE_CUSTOMERS_KPI],
    ),
]


def emit_sigma_pages(emitter: DatahubRestEmitter) -> None:
    workspace_urn = _sigma_workspace_container_urn()

    for page_id, title, workbook_id, element_ids in _SIGMA_PAGES:
        page_urn = make_sigma_page_urn(page_id)
        workbook_urn = make_sigma_workbook_urn(workbook_id)
        chart_urns = [make_sigma_chart_urn(eid) for eid in element_ids]

        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=page_urn,
                aspect=StatusClass(removed=False),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=page_urn,
                # Real connector: empty lastModified (zeroed), customProperties only has ElementsCount.
                aspect=DashboardInfoClass(
                    title=title,
                    description="",
                    charts=chart_urns,
                    lastModified=ChangeAuditStampsClass(),
                    customProperties={
                        "ElementsCount": str(len(element_ids)),
                    },
                ),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=page_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(id=workspace_urn, urn=workspace_urn),
                        BrowsePathEntryClass(id=workbook_urn, urn=workbook_urn),
                    ]
                ),
            )
        )
        # Page-level inputFields = union of all chart fields on this page.
        all_fields: list[InputFieldClass] = []
        seen_urns: set[str] = set()
        for eid in element_ids:
            for field in _make_input_fields(eid).fields:
                if field.schemaFieldUrn not in seen_urns:
                    all_fields.append(field)
                    seen_urns.add(field.schemaFieldUrn)
        if all_fields:
            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=page_urn,
                    aspect=InputFieldsClass(fields=all_fields),
                )
            )
            emit_schema_field_statuses(
                emitter, page_urn, [f.schemaField.fieldPath for f in all_fields]
            )
        log.info("  sigma page: %s (%s)", title, page_urn)


# ---------------------------------------------------------------------------
# Sigma Workbooks (modelled as dashboards)
# ---------------------------------------------------------------------------

_SIGMA_WORKBOOKS = [
    (
        WORKBOOK_SALES_ID,
        "Sales Performance Dashboard",
        "Comprehensive sales analytics covering revenue trends, product performance, and customer insights",
        [PAGE_REVENUE_ID, PAGE_MARKETING_ID],
        "sales-v2.1",
    ),
    (
        WORKBOOK_EXEC_ID,
        "Executive KPIs",
        "Executive-level KPI dashboard for C-suite review — updated daily",
        [PAGE_KPI_ID],
        "exec-v1.4",
    ),
]


def emit_sigma_workbooks(emitter: DatahubRestEmitter) -> None:
    workspace_urn = _sigma_workspace_container_urn()

    for workbook_id, title, description, page_ids, version in _SIGMA_WORKBOOKS:
        workbook_urn = make_sigma_workbook_urn(workbook_id)
        page_urns = [make_sigma_page_urn(pid) for pid in page_ids]

        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=workbook_urn,
                aspect=StatusClass(removed=False),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=workbook_urn,
                aspect=DashboardInfoClass(
                    title=title,
                    description=description,
                    dashboards=[EdgeClass(destinationUrn=u) for u in page_urns],
                    charts=[],
                    externalUrl=f"https://app.sigmacomputing.com/acrylmart/workbook/{workbook_id}",
                    lastModified=_AUDIT_STAMP,
                    customProperties={
                        "path": f"/AcrylMart Analytics/{title}",
                        "latestVersion": version,
                    },
                ),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=workbook_urn,
                aspect=SubTypesClass(typeNames=["Sigma Workbook"]),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=workbook_urn,
                aspect=ContainerClass(container=workspace_urn),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=workbook_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(id=workspace_urn, urn=workspace_urn),
                    ]
                ),
            )
        )
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=workbook_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner="urn:li:corpuser:analytics-team",
                            type=OwnershipTypeClass.DATAOWNER,
                        )
                    ]
                ),
            )
        )
        # globalTags: "Sales Performance Dashboard" carries the "Trusted" badge.
        if workbook_id == WORKBOOK_SALES_ID:
            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=workbook_urn,
                    aspect=GlobalTagsClass(
                        tags=[TagAssociationClass(tag="urn:li:tag:Trusted")]
                    ),
                )
            )
        log.info("  sigma workbook: %s (%s)", title, workbook_urn)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_URL, token=DATAHUB_TOKEN)
    try:
        emitter.test_connection()
        log.info("Connected to DataHub at %s", DATAHUB_GMS_URL)
    except Exception as exc:
        log.error("Cannot connect to DataHub at %s: %s", DATAHUB_GMS_URL, exc)
        sys.exit(1)

    log.info("=== Emitting Snowflake containers ===")
    emit_snowflake_containers(emitter)

    log.info("=== Emitting Snowflake tables/views ===")
    emit_snowflake_tables(emitter)

    log.info("=== Emitting Sigma workspace ===")
    emit_sigma_workspace(emitter)

    log.info("=== Emitting Sigma datasets ===")
    emit_sigma_datasets(emitter)

    log.info("=== Emitting Sigma data models ===")
    emit_sigma_data_models(emitter)

    log.info("=== Emitting Sigma charts ===")
    emit_sigma_charts(emitter)

    log.info("=== Emitting Sigma pages ===")
    emit_sigma_pages(emitter)

    log.info("=== Emitting Sigma workbooks ===")
    emit_sigma_workbooks(emitter)

    # Derive the UI base URL: strip a trailing /gms for cloud, or map 8080→9002 locally.
    if DATAHUB_GMS_URL.endswith("/gms"):
        ui_base = DATAHUB_GMS_URL[: -len("/gms")]
    elif "8080" in DATAHUB_GMS_URL:
        ui_base = DATAHUB_GMS_URL.replace("8080", "9002")
    else:
        ui_base = DATAHUB_GMS_URL

    log.info("")
    log.info("Done. Demo entities emitted to %s", DATAHUB_GMS_URL)
    log.info("")
    log.info("Snowflake → %s/browse/dataset?platform=snowflake", ui_base)
    log.info("Sigma     → %s/browse/dashboard?platform=sigma", ui_base)


if __name__ == "__main__":
    main()
