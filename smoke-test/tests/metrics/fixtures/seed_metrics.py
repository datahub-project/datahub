"""
Dev-only seed script for the Metrics UI feature (CAT-2567).

Emits 3 semantic models and ~15 metrics via DataHubGraph.emit_mcp.
Idempotent: re-running upserts all entities without creating duplicates.

Usage:
    smoke-test/venv/bin/python smoke-test/tests/metrics/fixtures/seed_metrics.py
    smoke-test/venv/bin/python smoke-test/tests/metrics/fixtures/seed_metrics.py --dry-run
    smoke-test/venv/bin/python smoke-test/tests/metrics/fixtures/seed_metrics.py --wipe

See README.md in this directory for full usage guide.
"""

import argparse
import json
import logging
import os
from typing import List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AiContextClass,
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DerivedMetricInputClass,
    DialectClass,
    DialectExpressionClass,
    DimensionClass,
    DomainsClass,
    EdgeClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricKeyClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    ModelDatasetClass,
    NumberTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SemanticFieldClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelKeyClass,
    SemanticModelRelationshipClass,
    StatusClass,
    StringTypeClass,
    TagAssociationClass,
    _Aspect,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ACTOR_URN = "urn:li:corpuser:datahub"

# Timestamps ~Jan 2025 and ~Mar 2025 for plausible-looking audit stamps
TS_CREATED_MS = 1735689600000  # 2025-01-01 00:00:00 UTC
TS_MODIFIED_MS = 1741824000000  # 2025-03-13 00:00:00 UTC


def _ts(millis: int = TS_CREATED_MS) -> AuditStampClass:
    return AuditStampClass(time=millis, actor=ACTOR_URN)


def _platform(name: str) -> str:
    return f"urn:li:dataPlatform:{name}"


def _model_urn(platform: str, path: str, model_id: str) -> str:
    return f"urn:li:semanticModel:({_platform(platform)},{path},{model_id})"


def _metric_urn(platform: str, path: str, metric_id: str) -> str:
    return f"urn:li:metric:({_platform(platform)},{path},{metric_id})"


def _dataset_urn(platform: str, name: str, env: str = "PROD") -> str:
    return f"urn:li:dataset:({_platform(platform)},{name},{env})"


def _edge(dest_urn: str) -> EdgeClass:
    return EdgeClass(destinationUrn=dest_urn, created=_ts(), lastModified=_ts())


def _ownership(owner_urn: str = ACTOR_URN) -> OwnershipClass:
    return OwnershipClass(
        owners=[OwnerClass(owner=owner_urn, type=OwnershipTypeClass.DATAOWNER)],
        lastModified=_ts(),
    )


def _browse_paths(entries: List[str]) -> BrowsePathsV2Class:
    return BrowsePathsV2Class(path=[BrowsePathEntryClass(id=e) for e in entries])


def _tags(*tag_ids: str) -> GlobalTagsClass:
    return GlobalTagsClass(
        tags=[TagAssociationClass(tag=f"urn:li:tag:{tid}") for tid in tag_ids]
    )


def _terms(*term_ids: str) -> GlossaryTermsClass:
    return GlossaryTermsClass(
        terms=[
            GlossaryTermAssociationClass(urn=f"urn:li:glossaryTerm:{tid}")
            for tid in term_ids
        ],
        auditStamp=_ts(),
    )


def _domain(domain_id: str) -> DomainsClass:
    return DomainsClass(domains=[f"urn:li:domain:{domain_id}"])


def _expression(dialect: str, sql: str) -> MetricExpressionClass:
    return MetricExpressionClass(
        dialects=[DialectExpressionClass(dialect=dialect, expression=sql)]
    )


def _ai_context(
    synonyms: Optional[List[str]] = None,
    instructions: Optional[str] = None,
    examples: Optional[List[str]] = None,
) -> AiContextClass:
    return AiContextClass(
        synonyms=synonyms,
        instructions=instructions,
        examples=examples,
    )


def _schema_field(name: str, native_type: str = "FLOAT") -> SchemaFieldClass:
    return SchemaFieldClass(
        fieldPath=name,
        nativeDataType=native_type,
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        description="",
        nullable=True,
    )


def _string_field(name: str) -> SchemaFieldClass:
    return SchemaFieldClass(
        fieldPath=name,
        nativeDataType="VARCHAR",
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        description="",
        nullable=True,
    )


def _mcp(urn: str, aspect: _Aspect) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)


# ---------------------------------------------------------------------------
# URN constants
# ---------------------------------------------------------------------------

# --- Semantic models ---
SM_B2B_URN = _model_urn("snowflake", "b2b_sales", "b2b_sales_bookings")
SM_WEB_URN = _model_urn("databricks", "web_analytics", "web_performance_funnel")
SM_ACCT_URN = _model_urn("tableau", "accounts", "accounts_semantic_layer")

# --- Dataset upstreams ---
DS_OPP_URN = _dataset_urn("snowflake", "salesforce.dim_opportunity")
DS_ACCT_URN = _dataset_urn("snowflake", "salesforce.dim_account")
DS_DATE_URN = _dataset_urn("snowflake", "shared.dim_date")
DS_SESSION_URN = _dataset_urn("databricks", "web.dim_session")
DS_PAGE_URN = _dataset_urn("databricks", "web.fact_pageview")

# --- Snowflake / B2B metrics ---
MTR_BOOKINGS_URN = _metric_urn("snowflake", "b2b_sales", "bookings")
MTR_WON_OPP_URN = _metric_urn("snowflake", "b2b_sales", "won_opportunities")
MTR_CLOSED_OPP_URN = _metric_urn("snowflake", "b2b_sales", "closed_opportunities")
MTR_AVG_DEAL_URN = _metric_urn("snowflake", "b2b_sales", "avg_deal_size_won")
MTR_AVG_CYCLE_URN = _metric_urn("snowflake", "b2b_sales", "avg_deal_cycle_days")
MTR_BOOKINGS_TARGET_URN = _metric_urn("snowflake", "b2b_sales", "bookings_target")
MTR_PIPELINE_URN = _metric_urn("snowflake", "b2b_sales", "pipeline_generated")
MTR_WIN_RATE_URN = _metric_urn("snowflake", "b2b_sales", "win_rate")

# --- Parent/child chain for breadcrumb testing ---
MTR_BOOKINGS_BY_REGION_URN = _metric_urn("snowflake", "b2b_sales", "bookings_by_region")
MTR_BOOKINGS_BY_REGION_QTR_URN = _metric_urn(
    "snowflake", "b2b_sales", "bookings_by_region_by_quarter"
)

# --- Deep chain (6 levels) for 5-level truncation testing ---
MTR_DEEP_1_URN = _metric_urn("snowflake", "b2b_sales", "deep_root_metric")
MTR_DEEP_2_URN = _metric_urn("snowflake", "b2b_sales", "deep_level_2")
MTR_DEEP_3_URN = _metric_urn("snowflake", "b2b_sales", "deep_level_3")
MTR_DEEP_4_URN = _metric_urn("snowflake", "b2b_sales", "deep_level_4")
MTR_DEEP_5_URN = _metric_urn("snowflake", "b2b_sales", "deep_level_5")
MTR_DEEP_6_URN = _metric_urn("snowflake", "b2b_sales", "deep_leaf_metric")

# --- Databricks / web metrics ---
MTR_SESSIONS_URN = _metric_urn("databricks", "web_analytics", "total_sessions")
MTR_BOUNCE_RATE_URN = _metric_urn("databricks", "web_analytics", "bounce_rate")

# --- Sparse metric (only MetricInfo.name set) ---
MTR_SPARSE_URN = _metric_urn("tableau", "accounts", "tableau_metric_sparse")

ALL_URNS: List[str] = [
    SM_B2B_URN,
    SM_WEB_URN,
    SM_ACCT_URN,
    DS_OPP_URN,
    DS_ACCT_URN,
    DS_DATE_URN,
    DS_SESSION_URN,
    DS_PAGE_URN,
    MTR_BOOKINGS_URN,
    MTR_WON_OPP_URN,
    MTR_CLOSED_OPP_URN,
    MTR_AVG_DEAL_URN,
    MTR_AVG_CYCLE_URN,
    MTR_BOOKINGS_TARGET_URN,
    MTR_PIPELINE_URN,
    MTR_WIN_RATE_URN,
    MTR_BOOKINGS_BY_REGION_URN,
    MTR_BOOKINGS_BY_REGION_QTR_URN,
    MTR_DEEP_1_URN,
    MTR_DEEP_2_URN,
    MTR_DEEP_3_URN,
    MTR_DEEP_4_URN,
    MTR_DEEP_5_URN,
    MTR_DEEP_6_URN,
    MTR_SESSIONS_URN,
    MTR_BOUNCE_RATE_URN,
    MTR_SPARSE_URN,
]

# ---------------------------------------------------------------------------
# MCPs: upstream datasets (minimal SchemaMetadata so lineage has something to hop to)
# ---------------------------------------------------------------------------

_DATASET_CONFIGS = [
    (
        DS_OPP_URN,
        "snowflake",
        "dim_opportunity",
        ["opportunity_id", "account_id", "stage", "amount", "close_date"],
    ),
    (
        DS_ACCT_URN,
        "snowflake",
        "dim_account",
        ["account_id", "account_name", "industry", "region"],
    ),
    (
        DS_DATE_URN,
        "snowflake",
        "dim_date",
        ["date_key", "calendar_date", "quarter", "year"],
    ),
    (
        DS_SESSION_URN,
        "databricks",
        "dim_session",
        ["session_id", "user_id", "referrer", "device_type"],
    ),
    (
        DS_PAGE_URN,
        "databricks",
        "fact_pageview",
        ["session_id", "page_url", "time_on_page_secs", "bounced"],
    ),
]


def _dataset_mcps() -> List[MetadataChangeProposalWrapper]:
    from datahub.metadata.schema_classes import (
        DatasetPropertiesClass,
        OtherSchemaClass,
        SchemaMetadataClass,
    )

    mcps: List[MetadataChangeProposalWrapper] = []
    for urn, platform, name, fields in _DATASET_CONFIGS:
        mcps.append(
            _mcp(
                urn,
                DatasetPropertiesClass(
                    name=name, description=f"Upstream table: {name}"
                ),
            )
        )
        schema_fields = [_string_field(f) for f in fields]
        schema_fields[0] = SchemaFieldClass(
            fieldPath=fields[0],
            nativeDataType="VARCHAR",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nullable=False,
            description="",
        )
        mcps.append(
            _mcp(
                urn,
                SchemaMetadataClass(
                    schemaName=name,
                    platform=_platform(platform),
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    fields=schema_fields,
                ),
            )
        )
    return mcps


# ---------------------------------------------------------------------------
# MCPs: B2B Sales Bookings & Pipelines (fully-populated semantic model)
# ---------------------------------------------------------------------------

_B2B_NATIVE_DEF = """\
semantic_model:
  name: b2b_sales_bookings
  description: "B2B sales bookings and pipeline metrics"
  model: ref('fct_bookings')
  entities:
    - name: opportunity
      type: primary
      expr: opportunity_id
  measures:
    - name: bookings
      agg: sum
      expr: amount
    - name: won_opportunities
      agg: count_distinct
      expr: CASE WHEN stage = 'Closed Won' THEN opportunity_id END
"""

_B2B_DATASETS = [
    ModelDatasetClass(
        name="fct_opportunity",
        source=DS_OPP_URN,
        fields=[
            SemanticFieldClass(
                schemaField=_schema_field("amount"),
                type=SemanticFieldTypeClass.MEASURE,
                expression=_expression(DialectClass.SNOWFLAKE, "SUM(amount)"),
            ),
            SemanticFieldClass(
                schemaField=_schema_field("opportunity_id"),
                type=SemanticFieldTypeClass.DIMENSION,
                expression=_expression(DialectClass.SNOWFLAKE, "opportunity_id"),
            ),
            SemanticFieldClass(
                schemaField=_string_field("stage"),
                type=SemanticFieldTypeClass.FILTER,
                expression=_expression(DialectClass.SNOWFLAKE, "stage"),
            ),
            SemanticFieldClass(
                schemaField=_string_field("region"),
                type=SemanticFieldTypeClass.DIMENSION,
                expression=_expression(DialectClass.SNOWFLAKE, "region"),
            ),
        ],
    ),
    ModelDatasetClass(
        name="dim_account",
        source=DS_ACCT_URN,
        fields=[
            SemanticFieldClass(
                schemaField=_string_field("account_name"),
                type=SemanticFieldTypeClass.DIMENSION,
                expression=_expression(DialectClass.SNOWFLAKE, "account_name"),
            ),
            SemanticFieldClass(
                schemaField=_string_field("industry"),
                type=SemanticFieldTypeClass.DIMENSION,
                expression=_expression(DialectClass.SNOWFLAKE, "industry"),
            ),
        ],
    ),
    ModelDatasetClass(
        name="dim_date",
        source=DS_DATE_URN,
        fields=[
            SemanticFieldClass(
                schemaField=_string_field("calendar_date"),
                type=SemanticFieldTypeClass.DIMENSION,
                expression=_expression(DialectClass.SNOWFLAKE, "calendar_date"),
                dimension=DimensionClass(isTime=True),
            ),
            SemanticFieldClass(
                schemaField=_string_field("quarter"),
                type=SemanticFieldTypeClass.DIMENSION,
                expression=_expression(DialectClass.SNOWFLAKE, "quarter"),
                dimension=DimensionClass(isTime=True),
            ),
        ],
    ),
]

_B2B_RELATIONSHIPS = [
    SemanticModelRelationshipClass(
        name="opportunity_to_account",
        from_="fct_opportunity",
        fromColumns=["account_id"],
        to="dim_account",
        toColumns=["account_id"],
        cardinality="N_ONE",
    ),
    SemanticModelRelationshipClass(
        name="opportunity_to_date",
        from_="fct_opportunity",
        fromColumns=["close_date"],
        to="dim_date",
        toColumns=["date_key"],
        cardinality="N_ONE",
    ),
]


def _b2b_model_mcps() -> List[MetadataChangeProposalWrapper]:
    urn = SM_B2B_URN
    mcps: List[MetadataChangeProposalWrapper] = [
        _mcp(
            urn,
            SemanticModelKeyClass(
                platform=_platform("snowflake"),
                path="b2b_sales",
                id="b2b_sales_bookings",
            ),
        ),
        _mcp(
            urn,
            SemanticModelInfoClass(
                name="B2B Sales Bookings & Pipelines",
                description=(
                    "Covers all B2B sales bookings, pipeline generation, and win-rate metrics. "
                    "Sourced from Salesforce CRM via dbt."
                ),
                created=_ts(TS_CREATED_MS),
                lastModified=_ts(TS_MODIFIED_MS),
                nativeDefinition=_B2B_NATIVE_DEF,
                aiContext=_ai_context(
                    synonyms=["sales bookings", "B2B pipeline", "CRM metrics"],
                    instructions="Use when answering questions about closed deals, sales performance, or pipeline health.",
                    examples=[
                        "What was our bookings last quarter?",
                        "Show win rate by region",
                    ],
                ),
                datasets=_B2B_DATASETS,
                relationships=_B2B_RELATIONSHIPS,
            ),
        ),
        _mcp(urn, _ownership()),
        _mcp(urn, _tags("sales", "crm")),
        _mcp(urn, _terms("Revenue", "Pipeline")),
        _mcp(urn, _domain("sales")),
        _mcp(urn, _browse_paths(["b2b_sales"])),
    ]
    return mcps


# ---------------------------------------------------------------------------
# MCPs: Web Performance & Funnel (medium semantic model)
# ---------------------------------------------------------------------------

_WEB_NATIVE_DEF = """\
-- Databricks metric view definition
CREATE METRIC VIEW web_performance_funnel
  METRICS (
    total_sessions LONG COMMENT 'Total browsing sessions',
    bounce_rate DOUBLE COMMENT 'Sessions with a single pageview / total sessions'
  )
  DIMENSIONS (
    device_type STRING,
    referrer   STRING
  )
  SOURCE (SELECT * FROM web.dim_session JOIN web.fact_pageview USING (session_id));
"""


def _web_model_mcps() -> List[MetadataChangeProposalWrapper]:
    urn = SM_WEB_URN
    datasets = [
        ModelDatasetClass(
            name="dim_session",
            source=DS_SESSION_URN,
            fields=[
                SemanticFieldClass(
                    schemaField=_string_field("device_type"),
                    type=SemanticFieldTypeClass.DIMENSION,
                    expression=_expression(DialectClass.DATABRICKS, "device_type"),
                ),
                SemanticFieldClass(
                    schemaField=_string_field("referrer"),
                    type=SemanticFieldTypeClass.DIMENSION,
                    expression=_expression(DialectClass.DATABRICKS, "referrer"),
                ),
            ],
        ),
        ModelDatasetClass(
            name="fact_pageview",
            source=DS_PAGE_URN,
            fields=[
                SemanticFieldClass(
                    schemaField=_schema_field("time_on_page_secs"),
                    type=SemanticFieldTypeClass.MEASURE,
                    expression=_expression(
                        DialectClass.DATABRICKS, "AVG(time_on_page_secs)"
                    ),
                ),
                SemanticFieldClass(
                    schemaField=_schema_field("bounced"),
                    type=SemanticFieldTypeClass.FILTER,
                    expression=_expression(DialectClass.DATABRICKS, "bounced = TRUE"),
                ),
            ],
        ),
    ]
    return [
        _mcp(
            urn,
            SemanticModelKeyClass(
                platform=_platform("databricks"),
                path="web_analytics",
                id="web_performance_funnel",
            ),
        ),
        _mcp(
            urn,
            SemanticModelInfoClass(
                name="Web Performance & Funnel",
                description="Tracks user sessions, funnel drop-off, and bounce rate across the marketing site.",
                created=_ts(TS_CREATED_MS),
                lastModified=_ts(TS_CREATED_MS),
                nativeDefinition=_WEB_NATIVE_DEF,
                aiContext=_ai_context(
                    synonyms=["web analytics", "funnel metrics"],
                    instructions="Use for questions about site traffic, session quality, or conversion funnel.",
                ),
                datasets=datasets,
                relationships=[
                    SemanticModelRelationshipClass(
                        from_="dim_session",
                        fromColumns=["session_id"],
                        to="fact_pageview",
                        toColumns=["session_id"],
                        cardinality="ONE_N",
                    )
                ],
            ),
        ),
        _mcp(urn, _ownership()),
        _mcp(urn, _tags("web", "analytics")),
        _mcp(urn, _domain("product")),
        _mcp(urn, _browse_paths(["web_analytics"])),
    ]


# ---------------------------------------------------------------------------
# MCPs: Accounts Semantic Layer (sparse — only info.name set)
# ---------------------------------------------------------------------------


def _accounts_model_mcps() -> List[MetadataChangeProposalWrapper]:
    urn = SM_ACCT_URN
    return [
        _mcp(
            urn,
            SemanticModelKeyClass(
                platform=_platform("tableau"),
                path="accounts",
                id="accounts_semantic_layer",
            ),
        ),
        _mcp(
            urn,
            SemanticModelInfoClass(name="Accounts Semantic Layer"),
        ),
    ]


# ---------------------------------------------------------------------------
# MCPs: B2B leaf / simple metrics
# ---------------------------------------------------------------------------


def _simple_metric(
    urn: str,
    name: str,
    description: str,
    dialect: str,
    sql: str,
    model_urn: str = SM_B2B_URN,
    modified_ms: int = TS_MODIFIED_MS,
    upstream_urns: Optional[List[str]] = None,
    ai_ctx: Optional[AiContextClass] = None,
    tags: Optional[List[str]] = None,
) -> List[MetadataChangeProposalWrapper]:
    parts = urn.split("(")[1].rstrip(")").split(",")
    platform_urn = parts[0].strip()
    path = parts[1].strip()
    metric_id = parts[2].strip()

    mcps: List[MetadataChangeProposalWrapper] = [
        _mcp(urn, MetricKeyClass(platform=platform_urn, path=path, id=metric_id)),
        _mcp(
            urn,
            MetricInfoClass(
                name=name,
                description=description,
                created=_ts(TS_CREATED_MS),
                lastModified=_ts(modified_ms),
                semanticModel=model_urn,
                expression=_expression(dialect, sql),
                aiContext=ai_ctx,
            ),
        ),
        _mcp(urn, _ownership()),
        _mcp(urn, _browse_paths([path])),
        # Emit MetricRelationships even with no parent so `hasParentMetric` is
        # explicitly indexed as false — getRootMetrics filters on that field,
        # and an absent aspect leaves it unindexed (excluded from both root
        # and child listings).
        _mcp(urn, MetricRelationshipsClass()),
    ]
    if tags:
        mcps.append(_mcp(urn, _tags(*tags)))
    if upstream_urns:
        mcps.append(
            _mcp(
                urn,
                MetricUpstreamsClass(
                    datasetUpstreams=[_edge(u) for u in upstream_urns]
                ),
            )
        )
    return mcps


def _b2b_simple_metrics_mcps() -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []

    mcps.extend(
        _simple_metric(
            MTR_BOOKINGS_URN,
            "bookings",
            "Total value of closed-won opportunities in the period.",
            DialectClass.SNOWFLAKE,
            "SUM(CASE WHEN stage = 'Closed Won' THEN amount ELSE 0 END)",
            upstream_urns=[DS_OPP_URN],
            ai_ctx=_ai_context(
                synonyms=["closed bookings", "won revenue", "ARR bookings"],
                instructions="Report in USD. Exclude internal test accounts.",
                examples=[
                    "What were total bookings last quarter?",
                    "Show bookings by region",
                ],
            ),
            tags=["revenue", "kpi"],
        )
    )
    mcps.extend(
        _simple_metric(
            MTR_WON_OPP_URN,
            "won_opportunities",
            "Count of distinct opportunities closed as won.",
            DialectClass.SNOWFLAKE,
            "COUNT(DISTINCT CASE WHEN stage = 'Closed Won' THEN opportunity_id END)",
            upstream_urns=[DS_OPP_URN],
        )
    )
    mcps.extend(
        _simple_metric(
            MTR_CLOSED_OPP_URN,
            "closed_opportunities",
            "Count of all closed opportunities (won + lost).",
            DialectClass.SNOWFLAKE,
            "COUNT(DISTINCT CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN opportunity_id END)",
            upstream_urns=[DS_OPP_URN],
        )
    )
    mcps.extend(
        _simple_metric(
            MTR_AVG_DEAL_URN,
            "avg_deal_size_won",
            "Average deal size across closed-won opportunities.",
            DialectClass.ANSI_SQL,
            "SUM(CASE WHEN stage = 'Closed Won' THEN amount END) / NULLIF(COUNT(DISTINCT CASE WHEN stage = 'Closed Won' THEN opportunity_id END), 0)",
            upstream_urns=[DS_OPP_URN],
        )
    )
    mcps.extend(
        _simple_metric(
            MTR_AVG_CYCLE_URN,
            "avg_deal_cycle_days",
            "Average number of days from opportunity creation to close.",
            DialectClass.SNOWFLAKE,
            "AVG(DATEDIFF('day', created_date, close_date))",
            upstream_urns=[DS_OPP_URN],
        )
    )
    mcps.extend(
        _simple_metric(
            MTR_BOOKINGS_TARGET_URN,
            "bookings_target",
            "Target bookings figure for the current period.",
            DialectClass.SNOWFLAKE,
            "SUM(quarterly_target)",
            modified_ms=TS_CREATED_MS,
        )
    )
    mcps.extend(
        _simple_metric(
            MTR_PIPELINE_URN,
            "pipeline_generated",
            "Total pipeline value of all open opportunities created in the period.",
            DialectClass.SNOWFLAKE,
            "SUM(CASE WHEN stage NOT IN ('Closed Won', 'Closed Lost') THEN amount ELSE 0 END)",
            upstream_urns=[DS_OPP_URN],
        )
    )
    return mcps


# ---------------------------------------------------------------------------
# MCPs: derived metric (win_rate)
# ---------------------------------------------------------------------------


def _win_rate_mcps() -> List[MetadataChangeProposalWrapper]:
    urn = MTR_WIN_RATE_URN
    parts = urn.split("(")[1].rstrip(")").split(",")
    platform_urn = parts[0].strip()
    path = parts[1].strip()
    metric_id = parts[2].strip()

    return [
        _mcp(urn, MetricKeyClass(platform=platform_urn, path=path, id=metric_id)),
        _mcp(
            urn,
            MetricInfoClass(
                name="win_rate",
                description="Ratio of won opportunities to all closed opportunities.",
                created=_ts(TS_CREATED_MS),
                lastModified=_ts(TS_MODIFIED_MS),
                semanticModel=SM_B2B_URN,
                expression=_expression(
                    DialectClass.SNOWFLAKE,
                    "DIV0(won_opportunities, closed_opportunities)",
                ),
                aiContext=_ai_context(
                    synonyms=["close rate", "conversion rate"],
                    instructions="Expressed as a decimal (0–1); multiply by 100 for percentage.",
                    examples=[
                        "What is our current win rate?",
                        "Compare win rate by segment",
                    ],
                ),
            ),
        ),
        _mcp(
            urn,
            MetricRelationshipsClass(
                derivedFrom=[
                    DerivedMetricInputClass(destinationUrn=MTR_WON_OPP_URN),
                    DerivedMetricInputClass(destinationUrn=MTR_CLOSED_OPP_URN),
                ]
            ),
        ),
        _mcp(urn, _ownership()),
        _mcp(urn, _browse_paths(["b2b_sales"])),
        _mcp(urn, _tags("kpi")),
    ]


# ---------------------------------------------------------------------------
# MCPs: parent/child chain (bookings → by_region → by_region_by_quarter)
# ---------------------------------------------------------------------------


def _hierarchy_chain_mcps() -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []

    def _child(
        urn: str,
        name: str,
        description: str,
        parent_urn: str,
        sql: str,
    ) -> List[MetadataChangeProposalWrapper]:
        parts = urn.split("(")[1].rstrip(")").split(",")
        platform_urn = parts[0].strip()
        path = parts[1].strip()
        metric_id = parts[2].strip()
        return [
            _mcp(urn, MetricKeyClass(platform=platform_urn, path=path, id=metric_id)),
            _mcp(
                urn,
                MetricInfoClass(
                    name=name,
                    description=description,
                    created=_ts(TS_CREATED_MS),
                    lastModified=_ts(TS_MODIFIED_MS),
                    semanticModel=SM_B2B_URN,
                    expression=_expression(DialectClass.SNOWFLAKE, sql),
                ),
            ),
            _mcp(urn, MetricRelationshipsClass(parentMetric=parent_urn)),
            _mcp(urn, _ownership()),
            _mcp(urn, _browse_paths(["b2b_sales"])),
        ]

    mcps.extend(
        _child(
            MTR_BOOKINGS_BY_REGION_URN,
            "bookings_by_region",
            "Closed-won bookings segmented by sales region.",
            MTR_BOOKINGS_URN,
            "SUM(CASE WHEN stage = 'Closed Won' THEN amount ELSE 0 END) GROUP BY region",
        )
    )
    mcps.extend(
        _child(
            MTR_BOOKINGS_BY_REGION_QTR_URN,
            "bookings_by_region_by_quarter",
            "Closed-won bookings segmented by region and fiscal quarter.",
            MTR_BOOKINGS_BY_REGION_URN,
            "SUM(CASE WHEN stage = 'Closed Won' THEN amount ELSE 0 END) GROUP BY region, quarter",
        )
    )
    return mcps


# ---------------------------------------------------------------------------
# MCPs: deep chain (6 levels) — tests 5-level header truncation
# ---------------------------------------------------------------------------


def _deep_chain_mcps() -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []
    chain = [
        (MTR_DEEP_1_URN, "deep_root_metric", "Root of a deep metric hierarchy.", None),
        (
            MTR_DEEP_2_URN,
            "deep_level_2",
            "Second level of the deep chain.",
            MTR_DEEP_1_URN,
        ),
        (
            MTR_DEEP_3_URN,
            "deep_level_3",
            "Third level of the deep chain.",
            MTR_DEEP_2_URN,
        ),
        (
            MTR_DEEP_4_URN,
            "deep_level_4",
            "Fourth level of the deep chain.",
            MTR_DEEP_3_URN,
        ),
        (
            MTR_DEEP_5_URN,
            "deep_level_5",
            "Fifth level of the deep chain.",
            MTR_DEEP_4_URN,
        ),
        (
            MTR_DEEP_6_URN,
            "deep_leaf_metric",
            "Leaf metric at depth 6; header should truncate with '…' at level 5.",
            MTR_DEEP_5_URN,
        ),
    ]
    for urn, name, desc, parent_urn in chain:
        parts = urn.split("(")[1].rstrip(")").split(",")
        platform_urn = parts[0].strip()
        path = parts[1].strip()
        metric_id = parts[2].strip()
        mcps.append(
            _mcp(urn, MetricKeyClass(platform=platform_urn, path=path, id=metric_id))
        )
        mcps.append(
            _mcp(
                urn,
                MetricInfoClass(
                    name=name,
                    description=desc,
                    created=_ts(TS_CREATED_MS),
                    lastModified=_ts(TS_MODIFIED_MS),
                    semanticModel=SM_B2B_URN,
                    expression=_expression(
                        DialectClass.ANSI_SQL, f"deep_expr_{metric_id}"
                    ),
                ),
            )
        )
        # Always emit MetricRelationships (even with parentMetric=None for the
        # root) so `hasParentMetric` is explicitly indexed — see note in
        # _simple_metric().
        mcps.append(_mcp(urn, MetricRelationshipsClass(parentMetric=parent_urn)))
        mcps.append(_mcp(urn, _ownership()))
        mcps.append(_mcp(urn, _browse_paths(["b2b_sales"])))
    return mcps


# ---------------------------------------------------------------------------
# MCPs: Databricks web metrics
# ---------------------------------------------------------------------------


def _web_metrics_mcps() -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []

    mcps.extend(
        _simple_metric(
            MTR_SESSIONS_URN,
            "total_sessions",
            "Total number of browsing sessions in the period.",
            DialectClass.DATABRICKS,
            "COUNT(DISTINCT session_id)",
            model_urn=SM_WEB_URN,
            upstream_urns=[DS_SESSION_URN],
        )
    )
    mcps.extend(
        _simple_metric(
            MTR_BOUNCE_RATE_URN,
            "bounce_rate",
            "Proportion of sessions where the user left after viewing a single page.",
            DialectClass.DATABRICKS,
            "SUM(CAST(bounced AS INT)) / NULLIF(COUNT(DISTINCT session_id), 0)",
            model_urn=SM_WEB_URN,
            upstream_urns=[DS_SESSION_URN, DS_PAGE_URN],
            ai_ctx=_ai_context(
                synonyms=["single-page rate"],
                instructions="Lower is better. Typical range 0.3–0.6.",
            ),
        )
    )
    return mcps


# ---------------------------------------------------------------------------
# MCPs: sparse Tableau metric
# ---------------------------------------------------------------------------


def _sparse_metric_mcps() -> List[MetadataChangeProposalWrapper]:
    urn = MTR_SPARSE_URN
    parts = urn.split("(")[1].rstrip(")").split(",")
    platform_urn = parts[0].strip()
    path = parts[1].strip()
    metric_id = parts[2].strip()
    return [
        _mcp(urn, MetricKeyClass(platform=platform_urn, path=path, id=metric_id)),
        _mcp(urn, MetricInfoClass(name="tableau_metric_sparse")),
        # Explicitly index hasParentMetric=false — see note in _simple_metric().
        _mcp(urn, MetricRelationshipsClass()),
    ]


# ---------------------------------------------------------------------------
# Assemble all MCPs
# ---------------------------------------------------------------------------


def build_all_mcps() -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []
    mcps.extend(_dataset_mcps())
    mcps.extend(_b2b_model_mcps())
    mcps.extend(_web_model_mcps())
    mcps.extend(_accounts_model_mcps())
    mcps.extend(_b2b_simple_metrics_mcps())
    mcps.extend(_win_rate_mcps())
    mcps.extend(_hierarchy_chain_mcps())
    mcps.extend(_deep_chain_mcps())
    mcps.extend(_web_metrics_mcps())
    mcps.extend(_sparse_metric_mcps())
    return mcps


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed metrics fixture data for CAT-2567.")
    p.add_argument(
        "--gms-url",
        default=os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080"),
        help="DataHub GMS URL (default: $DATAHUB_GMS_URL or http://localhost:8080)",
    )
    p.add_argument(
        "--token",
        default=os.environ.get("DATAHUB_GMS_TOKEN"),
        help="DataHub access token (default: $DATAHUB_GMS_TOKEN)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print MCPs to stdout without emitting anything.",
    )
    p.add_argument(
        "--wipe",
        action="store_true",
        help="Delete all seeded URNs instead of upserting.",
    )
    return p.parse_args()


def _make_graph(args: argparse.Namespace) -> DataHubGraph:
    config = DatahubClientConfig(
        server=args.gms_url,
        token=args.token,
    )
    return DataHubGraph(config)


def _do_emit(graph: DataHubGraph, mcps: List[MetadataChangeProposalWrapper]) -> None:
    for mcp in mcps:
        graph.emit_mcp(mcp)
    logger.info("Emitted %d MCPs.", len(mcps))


def _do_wipe(graph: DataHubGraph) -> None:
    logger.info("Soft-deleting %d entities…", len(ALL_URNS))
    for urn in ALL_URNS:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StatusClass(removed=True),
            )
        )
    logger.info("Done. Re-run without --wipe to restore.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()

    if args.dry_run:
        mcps = build_all_mcps()
        print(
            f"[dry-run] Would emit {len(mcps)} MCPs across {len(ALL_URNS)} entities.\n"
        )
        for mcp in mcps:
            print(json.dumps(mcp.to_obj(), indent=2, default=str))
        return

    graph = _make_graph(args)

    if args.wipe:
        _do_wipe(graph)
        return

    mcps = build_all_mcps()
    _do_emit(graph, mcps)
    logger.info(
        "Seed complete. %d semantic models, %d metrics emitted.",
        3,
        len([u for u in ALL_URNS if "urn:li:metric:" in u]),
    )


if __name__ == "__main__":
    main()
