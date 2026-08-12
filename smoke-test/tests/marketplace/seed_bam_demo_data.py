"""Seed BAM/finance marketplace demo data into a running DataHub instance.

Creates ~10 root data products (plus a few children), large asset sets on a
couple of products, nested applications (app-of-apps), Views/Charts/Dashboards
with lineage, and a **side-by-side hierarchy demo**:

1. Native DP nesting via ``parentDataProduct`` (Trade Surveillance BAM → Equity/FX)
2. Application-as-1-level-parent: flat root DPs linked via the ``applications``
   aspect (no ``parentDataProduct``), proving Applications can group DPs without
   native multi-level hierarchy / lineage complexity.

Usage:
  export DATAHUB_GMS_URL=http://localhost:8080
  datahub init --username datahub --password datahub
  python smoke-test/tests/marketplace/seed_bam_demo_data.py
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import yaml

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_chart_urn,
    make_dashboard_urn,
    make_dataset_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ApplicationPropertiesClass,
    ApplicationsClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataProductAssociationClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    DomainsClass,
    EdgeClass,
    GlossaryTermInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.specific.dataproduct import DataProductPatchBuilder
from datahub.utilities.urns.urn import Urn

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("seed_bam_demo")

OWNER = make_user_urn("datahub")

# Stable domain + terms for BAM / capital-markets demo
DOMAIN_CAPITAL_MARKETS = Urn(
    "domain", [datahub_guid({"name": "Capital Markets BAM"})]
)
DOMAIN_RISK = Urn("domain", [datahub_guid({"name": "Enterprise Risk"})])
DOMAIN_PAYMENTS = Urn("domain", [datahub_guid({"name": "Payments & Cash"})])

TERM_TRADE = "urn:li:glossaryTerm:bam_trade_lifecycle"
TERM_SURVEILLANCE = "urn:li:glossaryTerm:bam_market_surveillance"
TERM_REGULATORY = "urn:li:glossaryTerm:bam_regulatory_reporting"
TERM_LIQUIDITY = "urn:li:glossaryTerm:bam_liquidity_risk"

PLATFORM = "snowflake"
ENV = "PROD"


def _cfg() -> DataHubGraph:
    env = yaml.safe_load(Path.home().joinpath(".datahubenv").read_text())
    return DataHubGraph(
        DatahubClientConfig(server=env["gms"]["server"], token=env["gms"]["token"])
    )


def _ds(schema: str, table: str) -> str:
    return make_dataset_urn(PLATFORM, f"{schema}.{table}", ENV)


def _emit_domain(graph: DataHubGraph, urn: Urn, name: str, description: str) -> None:
    graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=str(urn),
            aspect=DomainPropertiesClass(name=name, description=description),
        )
    )


def _emit_term(graph: DataHubGraph, urn: str, name: str, definition: str) -> None:
    graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlossaryTermInfoClass(
                name=name, definition=definition, termSource="INTERNAL"
            ),
        )
    )


def _emit_dataset(
    graph: DataHubGraph,
    urn: str,
    description: str,
    *,
    subtype: Optional[str] = None,
) -> None:
    name = urn.split(",")[1]
    graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetPropertiesClass(name=name, description=description),
        )
    )
    if subtype:
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=SubTypesClass(typeNames=[subtype]),
            )
        )


def _emit_view(graph: DataHubGraph, urn: str, description: str) -> None:
    _emit_dataset(graph, urn, description, subtype="View")


def _emit_chart(
    graph: DataHubGraph,
    urn: str,
    title: str,
    description: str,
    input_urns: Sequence[str],
) -> None:
    graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=ChartInfoClass(
                title=title,
                description=description,
                lastModified=ChangeAuditStampsClass(),
                inputEdges=[EdgeClass(destinationUrn=u) for u in input_urns],
            ),
        )
    )
    graph.emit(
        MetadataChangeProposalWrapper(entityUrn=urn, aspect=StatusClass(removed=False))
    )


def _emit_dashboard(
    graph: DataHubGraph,
    urn: str,
    title: str,
    description: str,
    chart_urns: Sequence[str],
    dataset_urns: Sequence[str],
) -> None:
    graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DashboardInfoClass(
                title=title,
                description=description,
                lastModified=ChangeAuditStampsClass(),
                chartEdges=[EdgeClass(destinationUrn=u) for u in chart_urns],
                datasetEdges=[EdgeClass(destinationUrn=u) for u in dataset_urns],
            ),
        )
    )
    graph.emit(
        MetadataChangeProposalWrapper(entityUrn=urn, aspect=StatusClass(removed=False))
    )


def _emit_dataset_lineage(
    graph: DataHubGraph, downstream_urn: str, upstream_urns: Sequence[str]
) -> None:
    graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=u, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                    for u in upstream_urns
                ]
            ),
        )
    )


def _create_data_product(
    graph: DataHubGraph,
    *,
    id: str,
    name: str,
    description: str,
    domain_urn: str,
    parent_urn: Optional[str] = None,
) -> str:
    props: Dict = {"name": name, "description": description}
    if parent_urn:
        props["parentDataProduct"] = parent_urn
    # Soft-delete prior copy for idempotency
    urn = f"urn:li:dataProduct:{id}"
    try:
        graph.execute_graphql(
            "mutation($urn: String!) { deleteDataProduct(urn: $urn) }", {"urn": urn}
        )
    except Exception:
        pass
    result = graph.execute_graphql(
        """
        mutation($input: CreateDataProductInput!) {
          createDataProduct(input: $input) { urn }
        }
        """,
        {
            "input": {
                "id": id,
                "domainUrn": domain_urn,
                "properties": props,
            }
        },
    )
    return result["createDataProduct"]["urn"]


def _attach_assets(
    graph: DataHubGraph,
    data_product_urn: str,
    asset_urns: Sequence[str],
    output_ports: Sequence[str],
) -> None:
    output_set = set(output_ports)
    associations = [
        DataProductAssociationClass(
            destinationUrn=u, outputPort=u in output_set
        )
        for u in asset_urns
    ]
    for mcp in (
        DataProductPatchBuilder(data_product_urn)
        .set_assets(associations)
        .add_owner(OwnerClass(owner=OWNER, type=OwnershipTypeClass.TECHNICAL_OWNER))
        .build()
    ):
        graph.emit(mcp)


def _add_term(graph: DataHubGraph, resource_urn: str, term_urn: str) -> None:
    graph.execute_graphql(
        "mutation($input: TermAssociationInput!) { addTerm(input: $input) }",
        {"input": {"termUrn": term_urn, "resourceUrn": resource_urn}},
    )


def _make_application(
    graph: DataHubGraph,
    *,
    id: str,
    name: str,
    description: str,
    domain_urn: str,
    parent_urn: Optional[str] = None,
) -> str:
    urn = f"urn:li:application:{id}"
    try:
        graph.execute_graphql(
            "mutation($urn: String!) { deleteApplication(urn: $urn) }", {"urn": urn}
        )
    except Exception:
        pass

    props = ApplicationPropertiesClass(
        name=name,
        description=description,
        parentApplication=parent_urn,
    )
    for aspect in (
        props,
        OwnershipClass(
            owners=[
                OwnerClass(owner=OWNER, type=OwnershipTypeClass.TECHNICAL_OWNER)
            ]
        ),
        DomainsClass(domains=[domain_urn]),
        StatusClass(removed=False),
    ):
        graph.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))
    return urn


def _link_assets_to_applications(
    graph: DataHubGraph, asset_urn: str, application_urns: Sequence[str]
) -> None:
    graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=asset_urn,
            aspect=ApplicationsClass(applications=list(application_urns)),
        )
    )


def seed_supporting_entities(graph: DataHubGraph) -> None:
    _emit_domain(
        graph,
        DOMAIN_CAPITAL_MARKETS,
        "Capital Markets BAM",
        "Business activity monitoring for capital markets: trading, surveillance, and post-trade.",
    )
    _emit_domain(
        graph,
        DOMAIN_RISK,
        "Enterprise Risk",
        "Market, credit, and liquidity risk datasets used by risk and finance.",
    )
    _emit_domain(
        graph,
        DOMAIN_PAYMENTS,
        "Payments & Cash",
        "Payments rails, cash management, and fraud monitoring.",
    )
    _emit_term(
        graph,
        TERM_TRADE,
        "Trade Lifecycle",
        "Order → execution → allocation → settlement events.",
    )
    _emit_term(
        graph,
        TERM_SURVEILLANCE,
        "Market Surveillance",
        "Alerts and evidence for market abuse and spoofing detection.",
    )
    _emit_term(
        graph,
        TERM_REGULATORY,
        "Regulatory Reporting",
        "BCBS 239 / MiFID II / EMIR reportable facts.",
    )
    _emit_term(
        graph,
        TERM_LIQUIDITY,
        "Liquidity Risk",
        "Intraday liquidity, LCR/NSFR inputs, and cash ladder.",
    )


# --- Dataset catalogs (BAM / finance realistic names) ---

TRADE_SURVEILLANCE_TABLES = [
    ("bam_mkt_surv", "trades_raw", "Normalized equity/FX/FI trade blotter feed"),
    ("bam_mkt_surv", "orders_raw", "Parent and child order events from OMS"),
    ("bam_mkt_surv", "quotes_l1", "Top-of-book quote ticks for surveillance windows"),
    ("bam_mkt_surv", "alerts_spoofing", "Spoofing / layering alert candidates"),
    ("bam_mkt_surv", "alerts_wash_trade", "Wash-trade detection alerts"),
    ("bam_mkt_surv", "alerts_front_running", "Front-running / insider trading alerts"),
    ("bam_mkt_surv", "alert_evidence", "Supporting quotes/orders linked to an alert"),
    ("bam_mkt_surv", "trader_profiles", "Trader desk attribution and risk limits"),
    ("bam_mkt_surv", "instrument_ref", "Instrument master used by surveillance rules"),
    ("bam_mkt_surv", "venue_ref", "Exchange / MTF / SI venue reference"),
    ("bam_mkt_surv", "case_management", "Compliance case workflow for BAM alerts"),
    ("bam_mkt_surv", "surveillance_kpis", "Daily BAM KPI aggregates by desk and venue"),
]

PAYMENT_FRAUD_TABLES = [
    ("bam_payments", "wire_payments", "Outgoing/incoming wire payment instructions"),
    ("bam_payments", "ach_payments", "ACH / NACHA payment files"),
    ("bam_payments", "swift_messages", "SWIFT MT/MX message store"),
    ("bam_payments", "fraud_scores", "Real-time payment fraud model scores"),
    ("bam_payments", "fraud_alerts", "High-risk payment alerts for investigators"),
    ("bam_payments", "sanction_hits", "Sanctions / PEP screening hits"),
    ("bam_payments", "beneficiary_profiles", "Known good/bad beneficiary patterns"),
    ("bam_payments", "customer_risk_rating", "Customer AML risk ratings"),
    ("bam_payments", "case_queue", "Investigations queue for BAM payment cases"),
    ("bam_payments", "false_positive_feedback", "Analyst disposition feedback loop"),
    ("bam_payments", "payment_kpis", "STP rates, alert volumes, and SLA metrics"),
]

LIQUIDITY_TABLES = [
    ("risk_liquidity", "cash_ladder", "Intraday cash ladder by currency and legal entity"),
    ("risk_liquidity", "lcr_positions", "LCR HQLA and outflow positions"),
    ("risk_liquidity", "nsfr_positions", "NSFR ASF/RSF positions"),
    ("risk_liquidity", "collateral_inventory", "Eligible collateral inventory"),
    ("risk_liquidity", "funding_forecast", "Short-term funding forecast"),
]

CREDIT_RISK_TABLES = [
    ("risk_credit", "counterparty_master", "Legal entity / counterparty hierarchy"),
    ("risk_credit", "exposures_eod", "End-of-day credit exposures by product"),
    ("risk_credit", "limits_utilization", "Credit limit utilization"),
    ("risk_credit", "collateral_csa", "CSA collateral balances"),
    ("risk_credit", "rating_history", "Internal/external rating history"),
]

OMS_TABLES = [
    ("trading_oms", "orders", "Order management system order book"),
    ("trading_oms", "executions", "Fill / execution reports"),
    ("trading_oms", "allocations", "Post-trade allocations"),
    ("trading_oms", "positions", "Intraday trading positions"),
]

SETTLEMENT_TABLES = [
    ("post_trade", "settlement_instructions", "SSI master"),
    ("post_trade", "fails", "Settlement fails and age"),
    ("post_trade", "clearing_members", "CCP / clearing member map"),
    ("post_trade", "affirmations", "Trade affirmation status"),
]

REFDATA_TABLES = [
    ("refdata", "legal_entities", "LEI-backed legal entity master"),
    ("refdata", "calendars", "Trading and settlement calendars"),
    ("refdata", "fx_rates", "EOD FX rate strip"),
    ("refdata", "holiday_calendar", "Market holiday calendar"),
]

REG_REPORTING_TABLES = [
    ("reg_reporting", "mifid_tsa", "MiFID II transaction reports"),
    ("reg_reporting", "emir_trades", "EMIR trade / position reports"),
    ("reg_reporting", "bcbs239_kpis", "BCBS 239 data quality KPIs"),
    ("reg_reporting", "report_submissions", "Regulator submission audit log"),
]

KYC_TABLES = [
    ("client_onboarding", "kyc_profiles", "KYC / CDD customer profiles"),
    ("client_onboarding", "beneficial_owners", "UBO ownership graph"),
    ("client_onboarding", "documents", "Onboarding document index"),
]

POSITION_TABLES = [
    ("risk_market", "greeks_eod", "EOD option greeks by book"),
    ("risk_market", "var_historical", "Historical VaR by desk"),
    ("risk_market", "stress_scenarios", "Market risk stress scenario results"),
    ("risk_market", "pnl_explain", "Daily PnL explain attributes"),
]


ALL_TABLE_GROUPS: List[List[tuple]] = [
    TRADE_SURVEILLANCE_TABLES,
    PAYMENT_FRAUD_TABLES,
    LIQUIDITY_TABLES,
    CREDIT_RISK_TABLES,
    OMS_TABLES,
    SETTLEMENT_TABLES,
    REFDATA_TABLES,
    REG_REPORTING_TABLES,
    KYC_TABLES,
    POSITION_TABLES,
]


def seed_datasets(graph: DataHubGraph) -> Dict[str, List[str]]:
    """Emit all datasets; return map of product key → asset urns."""
    catalogs = {
        "trade_surveillance": TRADE_SURVEILLANCE_TABLES,
        "payment_fraud": PAYMENT_FRAUD_TABLES,
        "liquidity_risk": LIQUIDITY_TABLES,
        "credit_risk": CREDIT_RISK_TABLES,
        "order_management": OMS_TABLES,
        "settlement_clearing": SETTLEMENT_TABLES,
        "reference_data": REFDATA_TABLES,
        "regulatory_reporting": REG_REPORTING_TABLES,
        "client_kyc": KYC_TABLES,
        "market_risk_positions": POSITION_TABLES,
    }
    result: Dict[str, List[str]] = {}
    for key, tables in catalogs.items():
        urns: List[str] = []
        for schema, table, desc in tables:
            urn = _ds(schema, table)
            _emit_dataset(graph, urn, desc)
            urns.append(urn)
        result[key] = urns
        logger.info("Datasets for %s: %d", key, len(urns))
    return result


def seed_data_products(graph: DataHubGraph, assets: Dict[str, List[str]]) -> Dict[str, str]:
    """Create ~10 root data products (+ a few children). Returns id→urn."""
    roots = [
        (
            "bam_trade_surveillance",
            "Trade Surveillance BAM",
            "Market abuse & spoofing detection feeds powering the BAM surveillance desk.",
            str(DOMAIN_CAPITAL_MARKETS),
            "trade_surveillance",
            TERM_SURVEILLANCE,
            assets["trade_surveillance"][:3],  # output ports
        ),
        (
            "bam_payment_fraud",
            "Payment Fraud Monitoring",
            "Real-time payment fraud scores, sanctions hits, and investigator case queue.",
            str(DOMAIN_PAYMENTS),
            "payment_fraud",
            TERM_SURVEILLANCE,
            assets["payment_fraud"][:2],
        ),
        (
            "liquidity_risk_gold",
            "Liquidity Risk Gold",
            "Cash ladder, LCR/NSFR positions, and funding forecast for treasury BAM views.",
            str(DOMAIN_RISK),
            "liquidity_risk",
            TERM_LIQUIDITY,
            assets["liquidity_risk"][:2],
        ),
        (
            "credit_risk_exposures",
            "Credit Risk Exposures",
            "Counterparty exposures, limit utilization, and CSA collateral.",
            str(DOMAIN_RISK),
            "credit_risk",
            TERM_REGULATORY,
            assets["credit_risk"][:1],
        ),
        (
            "order_management_core",
            "Order Management Core",
            "OMS orders, executions, allocations, and intraday positions.",
            str(DOMAIN_CAPITAL_MARKETS),
            "order_management",
            TERM_TRADE,
            assets["order_management"][:2],
        ),
        (
            "settlement_clearing",
            "Settlement & Clearing",
            "SSI master, fails, affirmations, and clearing member map.",
            str(DOMAIN_CAPITAL_MARKETS),
            "settlement_clearing",
            TERM_TRADE,
            assets["settlement_clearing"][:1],
        ),
        (
            "capital_markets_refdata",
            "Capital Markets Reference Data",
            "Legal entities, calendars, FX rates shared across BAM products.",
            str(DOMAIN_CAPITAL_MARKETS),
            "reference_data",
            TERM_TRADE,
            assets["reference_data"][:1],
        ),
        (
            "regulatory_reporting_hub",
            "Regulatory Reporting Hub",
            "MiFID II / EMIR / BCBS 239 reportable facts and submission audit.",
            str(DOMAIN_RISK),
            "regulatory_reporting",
            TERM_REGULATORY,
            assets["regulatory_reporting"][:2],
        ),
        (
            "client_kyc_cdm",
            "Client KYC / CDD",
            "Onboarding profiles, UBO graph, and document index for client BAM.",
            str(DOMAIN_PAYMENTS),
            "client_kyc",
            TERM_REGULATORY,
            assets["client_kyc"][:1],
        ),
        (
            "market_risk_positions",
            "Market Risk & Positions",
            "Greeks, historical VaR, stress scenarios, and PnL explain.",
            str(DOMAIN_RISK),
            "market_risk_positions",
            TERM_LIQUIDITY,
            assets["market_risk_positions"][:2],
        ),
    ]

    urns: Dict[str, str] = {}
    for (
        id_,
        name,
        description,
        domain,
        asset_key,
        term,
        output_ports,
    ) in roots:
        urn = _create_data_product(
            graph, id=id_, name=name, description=description, domain_urn=domain
        )
        _attach_assets(graph, urn, assets[asset_key], output_ports)
        try:
            _add_term(graph, urn, term)
        except Exception as exc:
            logger.warning("addTerm failed for %s: %s", urn, exc)
        urns[id_] = urn
        logger.info("Data product %s → %s (%d assets)", name, urn, len(assets[asset_key]))

    # Children under Trade Surveillance BAM
    parent = urns["bam_trade_surveillance"]
    for child_id, child_name, child_desc, child_assets, child_ports in [
        (
            "bam_equity_surveillance",
            "Equity Surveillance",
            "Equity-specific spoofing and wash-trade alert packs.",
            assets["trade_surveillance"][3:7],
            assets["trade_surveillance"][3:5],
        ),
        (
            "bam_fx_surveillance",
            "FX Surveillance",
            "FX venue surveillance alerts and trader profiles.",
            assets["trade_surveillance"][7:11],
            assets["trade_surveillance"][7:8],
        ),
    ]:
        child_urn = _create_data_product(
            graph,
            id=child_id,
            name=child_name,
            description=child_desc,
            domain_urn=str(DOMAIN_CAPITAL_MARKETS),
            parent_urn=parent,
        )
        _attach_assets(graph, child_urn, child_assets, child_ports)
        urns[child_id] = child_urn
        logger.info("Child data product %s under %s", child_name, parent)

    return urns


def seed_bi_assets_and_lineage(
    graph: DataHubGraph,
    assets: Dict[str, List[str]],
    data_products: Dict[str, str],
) -> None:
    """Add Views / Charts / Dashboards as DP assets + wire upstream/downstream lineage.

    Lineage shapes (dataset → view → chart → dashboard) for three showcase products.
    """
    # ---- Trade Surveillance BAM ----
    trades_raw = assets["trade_surveillance"][0]
    orders_raw = assets["trade_surveillance"][1]
    quotes_l1 = assets["trade_surveillance"][2]
    alerts_spoofing = assets["trade_surveillance"][3]
    alerts_wash = assets["trade_surveillance"][4]
    trader_profiles = assets["trade_surveillance"][7]
    surv_kpis = assets["trade_surveillance"][11]

    trades_enriched = _ds("bam_mkt_surv", "trades_enriched")
    alert_ready_view = _ds("bam_mkt_surv", "v_alert_ready_trades")
    _emit_dataset(
        graph,
        trades_enriched,
        "Enriched blotter joining trades, orders, and L1 quotes for BAM rules.",
    )
    _emit_view(
        graph,
        alert_ready_view,
        "Curated Snowflake view of alert-ready trades (output port for consumers).",
    )

    chart_spoofing = make_chart_urn("looker", "bam_spoofing_alerts_by_desk")
    chart_wash = make_chart_urn("looker", "bam_wash_trade_heatmap")
    dash_surv = make_dashboard_urn("looker", "bam_trade_surveillance_ops")

    _emit_chart(
        graph,
        chart_spoofing,
        "Spoofing Alerts by Desk",
        "Daily spoofing / layering alert volume by trading desk.",
        [alert_ready_view, alerts_spoofing],
    )
    _emit_chart(
        graph,
        chart_wash,
        "Wash Trade Heatmap",
        "Wash-trade alert intensity across venues and instruments.",
        [alert_ready_view, alerts_wash],
    )
    _emit_dashboard(
        graph,
        dash_surv,
        "Trade Surveillance Ops",
        "Ops dashboard for BAM market surveillance investigators.",
        [chart_spoofing, chart_wash],
        [alert_ready_view, surv_kpis],
    )

    # Dataset lineage chain
    _emit_dataset_lineage(graph, trades_enriched, [trades_raw, orders_raw, quotes_l1])
    _emit_dataset_lineage(graph, alert_ready_view, [trades_enriched, trader_profiles])
    _emit_dataset_lineage(graph, alerts_spoofing, [alert_ready_view])
    _emit_dataset_lineage(graph, alerts_wash, [alert_ready_view])
    _emit_dataset_lineage(
        graph, surv_kpis, [alerts_spoofing, alerts_wash, trader_profiles]
    )

    surv_assets = list(assets["trade_surveillance"]) + [
        trades_enriched,
        alert_ready_view,
        chart_spoofing,
        chart_wash,
        dash_surv,
    ]
    surv_ports = [
        alert_ready_view,
        dash_surv,
        chart_spoofing,
        assets["trade_surveillance"][0],
    ]
    _attach_assets(
        graph, data_products["bam_trade_surveillance"], surv_assets, surv_ports
    )
    logger.info(
        "Enriched Trade Surveillance BAM: %d assets (views/charts/dashboard + lineage)",
        len(surv_assets),
    )

    # ---- Payment Fraud Monitoring ----
    wire = assets["payment_fraud"][0]
    ach = assets["payment_fraud"][1]
    swift = assets["payment_fraud"][2]
    fraud_scores = assets["payment_fraud"][3]
    fraud_alerts = assets["payment_fraud"][4]
    sanction_hits = assets["payment_fraud"][5]
    payment_kpis = assets["payment_fraud"][10]

    payment_enriched = _ds("bam_payments", "payment_enriched")
    high_risk_view = _ds("bam_payments", "v_high_risk_payments")
    _emit_dataset(
        graph,
        payment_enriched,
        "Unified payment instruction store (wire + ACH + SWIFT) with risk features.",
    )
    _emit_view(
        graph,
        high_risk_view,
        "High-risk payment view for investigator consoles (output port).",
    )

    chart_scores = make_chart_urn("tableau", "bam_fraud_score_distribution")
    chart_sanctions = make_chart_urn("tableau", "bam_sanction_hits_trend")
    dash_fraud = make_dashboard_urn("tableau", "bam_payment_fraud_ops")

    _emit_chart(
        graph,
        chart_scores,
        "Fraud Score Distribution",
        "Distribution of real-time payment fraud model scores.",
        [high_risk_view, fraud_scores],
    )
    _emit_chart(
        graph,
        chart_sanctions,
        "Sanctions Hits Trend",
        "7-day trend of sanctions / PEP screening hits.",
        [high_risk_view, sanction_hits],
    )
    _emit_dashboard(
        graph,
        dash_fraud,
        "Payment Fraud Ops",
        "Investigator dashboard for BAM payment fraud and sanctions.",
        [chart_scores, chart_sanctions],
        [high_risk_view, fraud_alerts, payment_kpis],
    )

    _emit_dataset_lineage(graph, payment_enriched, [wire, ach, swift])
    _emit_dataset_lineage(graph, high_risk_view, [payment_enriched, fraud_scores])
    _emit_dataset_lineage(graph, fraud_alerts, [high_risk_view, sanction_hits])
    _emit_dataset_lineage(
        graph, payment_kpis, [fraud_alerts, fraud_scores, sanction_hits]
    )

    pay_assets = list(assets["payment_fraud"]) + [
        payment_enriched,
        high_risk_view,
        chart_scores,
        chart_sanctions,
        dash_fraud,
    ]
    pay_ports = [high_risk_view, dash_fraud, chart_scores, fraud_scores]
    _attach_assets(graph, data_products["bam_payment_fraud"], pay_assets, pay_ports)
    logger.info(
        "Enriched Payment Fraud Monitoring: %d assets",
        len(pay_assets),
    )

    # ---- Liquidity Risk Gold ----
    cash_ladder = assets["liquidity_risk"][0]
    lcr = assets["liquidity_risk"][1]
    nsfr = assets["liquidity_risk"][2]
    funding = assets["liquidity_risk"][4]

    liquidity_view = _ds("risk_liquidity", "v_intraday_liquidity")
    _emit_view(
        graph,
        liquidity_view,
        "Intraday liquidity view combining cash ladder with LCR/NSFR context.",
    )

    chart_ladder = make_chart_urn("looker", "bam_cash_ladder_by_ccy")
    chart_lcr = make_chart_urn("looker", "bam_lcr_buffer")
    dash_liq = make_dashboard_urn("looker", "bam_treasury_liquidity")

    _emit_chart(
        graph,
        chart_ladder,
        "Cash Ladder by Currency",
        "Intraday cash ladder stacked by currency and legal entity.",
        [liquidity_view, cash_ladder],
    )
    _emit_chart(
        graph,
        chart_lcr,
        "LCR Buffer",
        "HQLA buffer vs net outflows for LCR.",
        [liquidity_view, lcr],
    )
    _emit_dashboard(
        graph,
        dash_liq,
        "Treasury Liquidity BAM",
        "Treasury BAM dashboard for intraday liquidity and regulatory buffers.",
        [chart_ladder, chart_lcr],
        [liquidity_view, funding],
    )

    _emit_dataset_lineage(graph, liquidity_view, [cash_ladder, lcr, nsfr])
    _emit_dataset_lineage(graph, funding, [cash_ladder, liquidity_view])

    liq_assets = list(assets["liquidity_risk"]) + [
        liquidity_view,
        chart_ladder,
        chart_lcr,
        dash_liq,
    ]
    liq_ports = [liquidity_view, dash_liq, chart_ladder]
    _attach_assets(graph, data_products["liquidity_risk_gold"], liq_assets, liq_ports)
    logger.info("Enriched Liquidity Risk Gold: %d assets", len(liq_assets))


def seed_applications(graph: DataHubGraph, assets: Dict[str, List[str]]) -> Dict[str, str]:
    """Create nested BAM applications; link some assets into multiple apps."""
    # Root apps
    bam_platform = _make_application(
        graph,
        id="bam_platform",
        name="BAM Platform",
        description="Enterprise Business Activity Monitoring platform spanning trading and payments.",
        domain_urn=str(DOMAIN_CAPITAL_MARKETS),
    )
    trading_suite = _make_application(
        graph,
        id="trading_suite",
        name="Electronic Trading Suite",
        description="Front-office trading applications: OMS, execution, and position keeping.",
        domain_urn=str(DOMAIN_CAPITAL_MARKETS),
    )
    risk_suite = _make_application(
        graph,
        id="risk_control_suite",
        name="Risk & Control Suite",
        description="Enterprise risk and compliance control applications.",
        domain_urn=str(DOMAIN_RISK),
    )

    # Nested under BAM Platform
    surv_app = _make_application(
        graph,
        id="bam_surveillance_app",
        name="Market Surveillance App",
        description="Compliance BAM UI for spoofing, wash trades, and case management.",
        domain_urn=str(DOMAIN_CAPITAL_MARKETS),
        parent_urn=bam_platform,
    )
    pay_fraud_app = _make_application(
        graph,
        id="bam_payment_fraud_app",
        name="Payment Fraud App",
        description="Investigator console for payment fraud and sanctions alerts.",
        domain_urn=str(DOMAIN_PAYMENTS),
        parent_urn=bam_platform,
    )
    # Nested deeper under Market Surveillance App
    case_app = _make_application(
        graph,
        id="bam_case_management_app",
        name="BAM Case Management",
        description="Case workflow nested under the Market Surveillance App.",
        domain_urn=str(DOMAIN_CAPITAL_MARKETS),
        parent_urn=surv_app,
    )

    # Nested under Trading Suite
    oms_app = _make_application(
        graph,
        id="oms_app",
        name="Order Management System",
        description="OMS application producing orders and executions.",
        domain_urn=str(DOMAIN_CAPITAL_MARKETS),
        parent_urn=trading_suite,
    )
    pos_app = _make_application(
        graph,
        id="position_keeper_app",
        name="Position Keeper",
        description="Intraday position and PnL keep.",
        domain_urn=str(DOMAIN_CAPITAL_MARKETS),
        parent_urn=trading_suite,
    )

    # Nested under Risk suite
    liq_app = _make_application(
        graph,
        id="liquidity_dashboard_app",
        name="Liquidity Dashboard",
        description="Treasury BAM views over cash ladder and LCR/NSFR.",
        domain_urn=str(DOMAIN_RISK),
        parent_urn=risk_suite,
    )
    credit_app = _make_application(
        graph,
        id="credit_limit_app",
        name="Credit Limit Monitor",
        description="Credit exposure and limit utilization monitor.",
        domain_urn=str(DOMAIN_RISK),
        parent_urn=risk_suite,
    )

    # Assets nested inside *multiple* applications (shared feeds)
    shared_pairs = [
        (
            assets["trade_surveillance"][0],  # trades_raw
            [surv_app, oms_app, bam_platform],
        ),
        (
            assets["trade_surveillance"][1],  # orders_raw
            [surv_app, oms_app],
        ),
        (
            assets["order_management"][1],  # executions
            [oms_app, pos_app, surv_app],
        ),
        (
            assets["liquidity_risk"][0],  # cash_ladder
            [liq_app, risk_suite, bam_platform],
        ),
        (
            assets["payment_fraud"][3],  # fraud_scores
            [pay_fraud_app, bam_platform],
        ),
        (
            assets["trade_surveillance"][10],  # case_management
            [case_app, surv_app],
        ),
        (
            assets["credit_risk"][1],  # exposures_eod
            [credit_app, risk_suite],
        ),
    ]
    for asset_urn, app_urns in shared_pairs:
        _link_assets_to_applications(graph, asset_urn, app_urns)
        logger.info(
            "Linked %s → %d applications", asset_urn.split(",")[1], len(app_urns)
        )

    return {
        "bam_platform": bam_platform,
        "trading_suite": trading_suite,
        "risk_suite": risk_suite,
        "surv_app": surv_app,
        "pay_fraud_app": pay_fraud_app,
        "case_app": case_app,
        "oms_app": oms_app,
        "pos_app": pos_app,
        "liq_app": liq_app,
        "credit_app": credit_app,
    }


def seed_application_as_dp_parent_demo(graph: DataHubGraph) -> Dict[str, str]:
    """Demo: Applications as 1-level parents for flat (non-nested) data products.

    Creates a portfolio Application and several root-level Data Products that have
    **no** ``parentDataProduct``. Each DP gets ``applications=[portfolio]`` so they
    appear on the Application Assets tab — the pushback narrative against native
    multi-level DP hierarchy (lineage complexity) while still showing 1-level grouping.
    """
    portfolio = _make_application(
        graph,
        id="capital_markets_dp_portfolio",
        name="Capital Markets Data Product Portfolio",
        description=(
            "DEMO — Application used as a 1-level parent for flat data products "
            "(no parentDataProduct). Contrast with native Trade Surveillance BAM hierarchy."
        ),
        domain_urn=str(DOMAIN_CAPITAL_MARKETS),
    )

    # Small dedicated asset sets so these DPs are self-contained in the demo
    flat_catalogs = {
        "dp_post_trade_analytics": [
            (
                "bam_app_parent_demo",
                "post_trade_fills",
                "Post-trade fill summary for portfolio analytics.",
            ),
            (
                "bam_app_parent_demo",
                "post_trade_commissions",
                "Commission & fee breakdown by broker.",
            ),
            (
                "bam_app_parent_demo",
                "v_post_trade_daily",
                "Daily post-trade analytics view.",
            ),
        ],
        "dp_desk_pnl_pack": [
            (
                "bam_app_parent_demo",
                "desk_pnl_raw",
                "Intraday desk PnL feed.",
            ),
            (
                "bam_app_parent_demo",
                "desk_pnl_adjusted",
                "Adjusted desk PnL after allocations.",
            ),
        ],
        "dp_client_mandate_pack": [
            (
                "bam_app_parent_demo",
                "client_mandates",
                "Client investment mandate rules.",
            ),
            (
                "bam_app_parent_demo",
                "mandate_breaches",
                "Mandate breach exceptions.",
            ),
            (
                "bam_app_parent_demo",
                "v_mandate_status",
                "Current mandate compliance status view.",
            ),
        ],
        "dp_broker_scorecard": [
            (
                "bam_app_parent_demo",
                "broker_tca",
                "Transaction cost analysis by broker.",
            ),
            (
                "bam_app_parent_demo",
                "broker_hit_rates",
                "Broker hit rates and fill quality.",
            ),
        ],
    }

    products = [
        (
            "dp_post_trade_analytics",
            "Post-Trade Analytics Pack",
            "Flat DP under the portfolio Application — post-trade fills & commissions.",
            str(DOMAIN_CAPITAL_MARKETS),
            TERM_TRADE,
            ["v_post_trade_daily"],
        ),
        (
            "dp_desk_pnl_pack",
            "Desk PnL Pack",
            "Flat DP under the portfolio Application — desk PnL feeds (no native parent).",
            str(DOMAIN_CAPITAL_MARKETS),
            TERM_TRADE,
            ["desk_pnl_adjusted"],
        ),
        (
            "dp_client_mandate_pack",
            "Client Mandate Pack",
            "Flat DP under the portfolio Application — mandates & breaches.",
            str(DOMAIN_CAPITAL_MARKETS),
            TERM_REGULATORY,
            ["v_mandate_status"],
        ),
        (
            "dp_broker_scorecard",
            "Broker Scorecard Pack",
            "Flat DP under the portfolio Application — TCA and broker quality.",
            str(DOMAIN_CAPITAL_MARKETS),
            TERM_TRADE,
            ["broker_tca"],
        ),
    ]

    dp_urns: Dict[str, str] = {"portfolio_app": portfolio}

    for id_, name, description, domain, term, port_tables in products:
        # Emit datasets (mark views)
        asset_urns: List[str] = []
        port_urns: List[str] = []
        for schema, table, desc in flat_catalogs[id_]:
            urn = _ds(schema, table)
            if table.startswith("v_"):
                _emit_view(graph, urn, desc)
            else:
                _emit_dataset(graph, urn, desc)
            asset_urns.append(urn)
            if table in port_tables:
                port_urns.append(urn)

        # Light lineage inside the pack (still flat at DP level)
        if id_ == "dp_post_trade_analytics" and len(asset_urns) >= 3:
            _emit_dataset_lineage(graph, asset_urns[2], asset_urns[:2])
        if id_ == "dp_desk_pnl_pack" and len(asset_urns) >= 2:
            _emit_dataset_lineage(graph, asset_urns[1], [asset_urns[0]])
        if id_ == "dp_client_mandate_pack" and len(asset_urns) >= 3:
            _emit_dataset_lineage(graph, asset_urns[2], asset_urns[:2])

        # Root DP — explicitly NO parentDataProduct
        dp_urn = _create_data_product(
            graph,
            id=id_,
            name=name,
            description=description,
            domain_urn=domain,
            parent_urn=None,
        )
        _attach_assets(graph, dp_urn, asset_urns, port_urns)
        try:
            _add_term(graph, dp_urn, term)
        except Exception as exc:
            logger.warning("addTerm failed for %s: %s", dp_urn, exc)

        # Associate DP → portfolio Application (1-level grouping)
        graph.emit(
            MetadataChangeProposalWrapper(
                entityUrn=dp_urn,
                aspect=ApplicationsClass(applications=[portfolio]),
            )
        )
        dp_urns[id_] = dp_urn
        logger.info(
            "Flat DP %s → Application %s (no parentDataProduct)",
            name,
            portfolio,
        )

    return dp_urns


def seed_external_lineage_dependencies(
    graph: DataHubGraph,
    assets: Dict[str, List[str]],
) -> None:
    """Wire upstream/downstream assets that sit *outside* data-product membership.

    Covers:
    1. Flat DPs under Capital Markets Data Product Portfolio
    2. Nested Equity Surveillance + FX Surveillance children
    """
    # ---------- 1) Portfolio (Application-grouped) flat DPs ----------
    # External sources (upstream of DP assets) — NOT added to any DP
    ext_oms_bus = _ds("ext_oms", "execution_bus")
    ext_broker_feed = _ds("ext_vendor", "broker_confirmations")
    ext_pnl_engine = _ds("ext_trading", "pnl_engine_output")
    ext_crm_mandates = _ds("ext_crm", "mandate_source_system")
    ext_exchange_stats = _ds("ext_exchange", "venue_quality_stats")

    _emit_dataset(
        graph,
        ext_oms_bus,
        "EXTERNAL — OMS execution bus (outside any data product).",
    )
    _emit_dataset(
        graph,
        ext_broker_feed,
        "EXTERNAL — Vendor broker confirmation feed (outside any data product).",
    )
    _emit_dataset(
        graph,
        ext_pnl_engine,
        "EXTERNAL — Front-office PnL engine output (outside any data product).",
    )
    _emit_dataset(
        graph,
        ext_crm_mandates,
        "EXTERNAL — CRM mandate source system (outside any data product).",
    )
    _emit_dataset(
        graph,
        ext_exchange_stats,
        "EXTERNAL — Exchange venue quality stats (outside any data product).",
    )

    # External consumers (downstream of DP assets) — NOT added to any DP
    ext_post_trade_mart = _ds("ext_analytics", "post_trade_mart")
    ext_gl_desk_pnl = _ds("ext_finance", "gl_desk_pnl_feed")
    ext_compliance_export = _ds("ext_compliance", "mandate_breach_export")
    ext_broker_review = _ds("ext_procurement", "broker_review_pack")
    ext_post_trade_dash = make_dashboard_urn("looker", "ext_post_trade_exec_summary")
    ext_pnl_chart = make_chart_urn("tableau", "ext_desk_pnl_waterfall")

    _emit_dataset(
        graph,
        ext_post_trade_mart,
        "EXTERNAL — Downstream analytics mart consuming post-trade pack outputs.",
    )
    _emit_dataset(
        graph,
        ext_gl_desk_pnl,
        "EXTERNAL — Finance GL feed consuming desk PnL (outside DP scope).",
    )
    _emit_dataset(
        graph,
        ext_compliance_export,
        "EXTERNAL — Compliance export of mandate breaches (outside DP scope).",
    )
    _emit_dataset(
        graph,
        ext_broker_review,
        "EXTERNAL — Procurement broker-review pack (outside DP scope).",
    )

    # In-DP assets for portfolio packs
    post_trade_fills = _ds("bam_app_parent_demo", "post_trade_fills")
    post_trade_commissions = _ds("bam_app_parent_demo", "post_trade_commissions")
    v_post_trade_daily = _ds("bam_app_parent_demo", "v_post_trade_daily")
    desk_pnl_raw = _ds("bam_app_parent_demo", "desk_pnl_raw")
    desk_pnl_adjusted = _ds("bam_app_parent_demo", "desk_pnl_adjusted")
    client_mandates = _ds("bam_app_parent_demo", "client_mandates")
    mandate_breaches = _ds("bam_app_parent_demo", "mandate_breaches")
    v_mandate_status = _ds("bam_app_parent_demo", "v_mandate_status")
    broker_tca = _ds("bam_app_parent_demo", "broker_tca")
    broker_hit_rates = _ds("bam_app_parent_demo", "broker_hit_rates")

    # Upstream: external → in-DP
    _emit_dataset_lineage(graph, post_trade_fills, [ext_oms_bus, ext_broker_feed])
    _emit_dataset_lineage(graph, post_trade_commissions, [ext_broker_feed])
    _emit_dataset_lineage(graph, desk_pnl_raw, [ext_pnl_engine])
    _emit_dataset_lineage(graph, client_mandates, [ext_crm_mandates])
    _emit_dataset_lineage(graph, broker_tca, [ext_exchange_stats, ext_oms_bus])
    _emit_dataset_lineage(graph, broker_hit_rates, [ext_exchange_stats])

    # Downstream: in-DP → external datasets
    _emit_dataset_lineage(graph, ext_post_trade_mart, [v_post_trade_daily, post_trade_fills])
    _emit_dataset_lineage(graph, ext_gl_desk_pnl, [desk_pnl_adjusted])
    _emit_dataset_lineage(graph, ext_compliance_export, [mandate_breaches, v_mandate_status])
    _emit_dataset_lineage(graph, ext_broker_review, [broker_tca, broker_hit_rates])

    # Downstream: in-DP → external BI (charts/dashboards outside the DP)
    _emit_dashboard(
        graph,
        ext_post_trade_dash,
        "EXTERNAL Post-Trade Exec Summary",
        "Outside-DP Looker dashboard consuming the post-trade analytics view.",
        [],
        [v_post_trade_daily, ext_post_trade_mart],
    )
    _emit_chart(
        graph,
        ext_pnl_chart,
        "EXTERNAL Desk PnL Waterfall",
        "Outside-DP Tableau chart consuming adjusted desk PnL.",
        [desk_pnl_adjusted, ext_gl_desk_pnl],
    )

    logger.info(
        "External lineage for portfolio DPs: "
        "upstreams=[oms_bus, broker_feed, pnl_engine, crm, exchange] "
        "downstreams=[post_trade_mart, gl_feed, compliance_export, broker_review, ext BI]"
    )

    # ---------- 2) Nested Equity + FX Surveillance ----------
    # Equity assets (in child DP): alerts_spoofing, alerts_wash, alerts_front_running, alert_evidence
    alerts_spoofing = assets["trade_surveillance"][3]
    alerts_wash = assets["trade_surveillance"][4]
    alerts_front = assets["trade_surveillance"][5]
    alert_evidence = assets["trade_surveillance"][6]

    # FX assets (in child DP): trader_profiles, instrument_ref, venue_ref, case_management
    trader_profiles = assets["trade_surveillance"][7]
    instrument_ref = assets["trade_surveillance"][8]
    venue_ref = assets["trade_surveillance"][9]
    case_management = assets["trade_surveillance"][10]

    # External upstreams (not in Equity/FX DPs, not in parent)
    ext_mkt_data_kafka = _ds("ext_marketdata", "equity_ticks_kafka")
    ext_fix_vendor = _ds("ext_refdata", "lei_instrument_vendor")
    ext_fx_ecb = _ds("ext_marketdata", "fx_ecb_fix_rates")

    _emit_dataset(
        graph,
        ext_mkt_data_kafka,
        "EXTERNAL — Equity tick Kafka topic feeding surveillance (outside nested DPs).",
    )
    _emit_dataset(
        graph,
        ext_fix_vendor,
        "EXTERNAL — LEI / instrument vendor master (outside nested DPs).",
    )
    _emit_dataset(
        graph,
        ext_fx_ecb,
        "EXTERNAL — ECB FX reference rates (outside nested DPs).",
    )

    # External downstreams
    ext_equity_alert_archive = _ds("ext_compliance_lake", "equity_alert_archive")
    ext_fx_case_export = _ds("ext_compliance_lake", "fx_case_export_archer")
    ext_surv_scorecard = make_dashboard_urn("looker", "ext_surveillance_exec_scorecard")

    _emit_dataset(
        graph,
        ext_equity_alert_archive,
        "EXTERNAL — Long-term equity alert archive (outside nested DPs).",
    )
    _emit_dataset(
        graph,
        ext_fx_case_export,
        "EXTERNAL — FX case export to Archer eComms (outside nested DPs).",
    )

    # Equity Surveillance: external up + down (preserve existing in-DP upstreams)
    _emit_dataset_lineage(
        graph,
        alerts_spoofing,
        [
            _ds("bam_mkt_surv", "v_alert_ready_trades"),
            ext_mkt_data_kafka,
            alert_evidence,
        ],
    )
    _emit_dataset_lineage(
        graph,
        alerts_wash,
        [_ds("bam_mkt_surv", "v_alert_ready_trades"), ext_mkt_data_kafka],
    )
    _emit_dataset_lineage(graph, alerts_front, [ext_mkt_data_kafka])
    _emit_dataset_lineage(graph, alert_evidence, [ext_mkt_data_kafka])
    _emit_dataset_lineage(
        graph,
        ext_equity_alert_archive,
        [alerts_spoofing, alerts_wash, alerts_front],
    )

    # FX Surveillance: external up + down
    _emit_dataset_lineage(graph, instrument_ref, [ext_fix_vendor])
    _emit_dataset_lineage(graph, venue_ref, [ext_fx_ecb, ext_fix_vendor])
    _emit_dataset_lineage(graph, trader_profiles, [ext_fix_vendor])
    _emit_dataset_lineage(
        graph, ext_fx_case_export, [case_management, trader_profiles]
    )

    # Shared external exec dashboard consuming both nested DP outputs
    _emit_dashboard(
        graph,
        ext_surv_scorecard,
        "EXTERNAL Surveillance Exec Scorecard",
        "Outside-DP dashboard spanning Equity + FX nested product outputs.",
        [],
        [ext_equity_alert_archive, ext_fx_case_export, alerts_spoofing, case_management],
    )

    logger.info(
        "External lineage for nested DPs: "
        "Equity←kafka ticks →alert_archive; FX←LEI/ECB →Archer export; "
        "shared exec scorecard dashboard outside both DPs"
    )


def verify(graph: DataHubGraph) -> None:
    roots = graph.execute_graphql(
        """
        query {
          getRootDataProducts(input: { start: 0, count: 50, query: "*" }) {
            total
            dataProducts {
              urn
              properties { name numAssets }
              childDataProducts(input: { count: 0, query: "*" }) { total }
            }
          }
        }
        """
    )["getRootDataProducts"]
    logger.info("Root data products total=%s", roots["total"])
    bamish = [
        dp
        for dp in roots["dataProducts"]
        if dp.get("properties")
        and (
            "BAM" in (dp["properties"]["name"] or "")
            or "Risk" in (dp["properties"]["name"] or "")
            or "Order" in (dp["properties"]["name"] or "")
            or "Settlement" in (dp["properties"]["name"] or "")
            or "Regulatory" in (dp["properties"]["name"] or "")
            or "KYC" in (dp["properties"]["name"] or "")
            or "Market Risk" in (dp["properties"]["name"] or "")
            or "Credit" in (dp["properties"]["name"] or "")
            or "Liquidity" in (dp["properties"]["name"] or "")
            or "Payment" in (dp["properties"]["name"] or "")
            or "Capital Markets" in (dp["properties"]["name"] or "")
            or "Customer 360" in (dp["properties"]["name"] or "")
            or "Pack" in (dp["properties"]["name"] or "")
            or "Scorecard" in (dp["properties"]["name"] or "")
        )
    ]
    for dp in sorted(bamish, key=lambda d: d["properties"]["name"]):
        logger.info(
            "  %s  assets=%s  children=%s",
            dp["properties"]["name"],
            dp["properties"].get("numAssets"),
            dp["childDataProducts"]["total"],
        )

    big = graph.execute_graphql(
        """
        query {
          dataProduct(urn: "urn:li:dataProduct:bam_trade_surveillance") {
            properties { name numAssets }
            childDataProducts(input: { count: 10, query: "*" }) {
              total
              searchResults {
                entity { ... on DataProduct { properties { name } } }
              }
            }
          }
        }
        """
    )["dataProduct"]
    logger.info(
        "Trade Surveillance BAM: assets=%s children=%s",
        big["properties"]["numAssets"],
        big["childDataProducts"]["total"],
    )

    # Sample lineage + output-port mix
    lineage_check = graph.execute_graphql(
        """
        query {
          dataset(
            urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,bam_mkt_surv.v_alert_ready_trades,PROD)"
          ) {
            urn
            subTypes { typeNames }
            upstream: lineage(input: { direction: UPSTREAM, start: 0, count: 10 }) {
              total
              relationships { entity { urn type } }
            }
            downstream: lineage(input: { direction: DOWNSTREAM, start: 0, count: 10 }) {
              total
              relationships { entity { urn type } }
            }
          }
          ports: listDataProductAssets(
            urn: "urn:li:dataProduct:bam_trade_surveillance"
            input: {
              query: "*"
              start: 0
              count: 20
              filters: [{ field: "isOutputPort", values: ["true"] }]
            }
          ) {
            total
            searchResults { entity { urn type } }
          }
        }
        """
    )
    ds = lineage_check["dataset"]
    logger.info(
        "View %s subtypes=%s upstream=%s downstream=%s",
        ds["urn"],
        (ds.get("subTypes") or {}).get("typeNames"),
        ds["upstream"]["total"],
        ds["downstream"]["total"],
    )
    for rel in ds["upstream"]["relationships"]:
        logger.info("  ↑ %s %s", rel["entity"]["type"], rel["entity"]["urn"])
    for rel in ds["downstream"]["relationships"]:
        logger.info("  ↓ %s %s", rel["entity"]["type"], rel["entity"]["urn"])
    logger.info(
        "Trade Surveillance output ports (%s):",
        lineage_check["ports"]["total"],
    )
    for r in lineage_check["ports"]["searchResults"]:
        logger.info("  • %s %s", r["entity"]["type"], r["entity"]["urn"])

    # --- Hierarchy contrast ---
    logger.info("--- Hierarchy demo contrast ---")
    native = graph.execute_graphql(
        """
        query {
          dataProduct(urn: "urn:li:dataProduct:bam_trade_surveillance") {
            properties { name parentDataProduct { urn } }
            childDataProducts(input: { count: 10, query: "*" }) {
              total
              searchResults {
                entity {
                  urn
                  ... on DataProduct {
                    properties {
                      name
                      parentDataProduct { urn }
                    }
                  }
                }
              }
            }
          }
        }
        """
    )["dataProduct"]
    logger.info(
        "NATIVE parentDataProduct: %s has %s children",
        native["properties"]["name"],
        native["childDataProducts"]["total"],
    )
    for r in native["childDataProducts"]["searchResults"]:
        props = r["entity"]["properties"]
        logger.info(
            "  child %s parentDataProduct=%s",
            props["name"],
            (props.get("parentDataProduct") or {}).get("urn"),
        )

    app_children = graph.execute_graphql(
        """
        query {
          scrollAcrossEntities(
            input: {
              query: "*"
              types: [DATA_PRODUCT]
              count: 20
              orFilters: [
                {
                  and: [
                    {
                      field: "applications"
                      values: ["urn:li:application:capital_markets_dp_portfolio"]
                    }
                  ]
                }
              ]
              searchFlags: { skipCache: true }
            }
          ) {
            total
            searchResults {
              entity {
                urn
                ... on DataProduct {
                  properties {
                    name
                    parentDataProduct { urn }
                  }
                  applications {
                    application { urn }
                  }
                }
              }
            }
          }
        }
        """
    )["scrollAcrossEntities"]
    logger.info(
        "APPLICATION-as-parent: portfolio has %s flat DPs (expect parentDataProduct=null)",
        app_children["total"],
    )
    for r in app_children["searchResults"]:
        ent = r["entity"]
        props = ent.get("properties") or {}
        logger.info(
            "  flat DP %s parentDataProduct=%s apps=%s",
            props.get("name"),
            (props.get("parentDataProduct") or {}).get("urn"),
            [
                a["application"]["urn"]
                for a in (ent.get("applications") or [])
                if a.get("application")
            ],
        )

    apps = graph.execute_graphql(
        """
        query {
          scrollAcrossEntities(
            input: {
              query: "*"
              types: [APPLICATION]
              count: 25
              searchFlags: { skipCache: true }
            }
          ) {
            total
            searchResults {
              entity {
                urn
                ... on Application {
                  properties { name }
                }
              }
            }
          }
        }
        """
    )["scrollAcrossEntities"]
    logger.info("Applications indexed=%s", apps["total"])
    for r in apps["searchResults"]:
        ent = r["entity"]
        logger.info("  %s (%s)", (ent.get("properties") or {}).get("name"), ent["urn"])

    # External lineage checks (assets outside DP membership)
    logger.info("--- External (outside-DP) lineage ---")
    for label, urn in [
        (
            "portfolio fills (expect EXT oms/broker upstream)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,bam_app_parent_demo.post_trade_fills,PROD)",
        ),
        (
            "equity spoofing alerts (expect EXT kafka + in-DP upstreams)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,bam_mkt_surv.alerts_spoofing,PROD)",
        ),
        (
            "fx instrument_ref (expect EXT LEI vendor upstream)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,bam_mkt_surv.instrument_ref,PROD)",
        ),
    ]:
        res = graph.execute_graphql(
            """
            query($urn: String!) {
              dataset(urn: $urn) {
                urn
                upstream: lineage(input: { direction: UPSTREAM, start: 0, count: 10 }) {
                  total
                  relationships { entity { urn type } }
                }
                downstream: lineage(input: { direction: DOWNSTREAM, start: 0, count: 10 }) {
                  total
                  relationships { entity { urn type } }
                }
              }
            }
            """,
            {"urn": urn},
        )["dataset"]
        logger.info(
            "%s — up=%s down=%s",
            label,
            res["upstream"]["total"],
            res["downstream"]["total"],
        )
        for rel in res["upstream"]["relationships"]:
            marker = "EXT" if "ext_" in rel["entity"]["urn"] else "in"
            logger.info(
                "  ↑ [%s] %s", marker, rel["entity"]["urn"].split(",")[-1][:80]
            )
        for rel in res["downstream"]["relationships"]:
            marker = "EXT" if "ext_" in rel["entity"]["urn"] else "in"
            logger.info(
                "  ↓ [%s] %s", marker, rel["entity"]["urn"].split(",")[-1][:80]
            )


def main() -> None:
    graph = _cfg()
    logger.info("Seeding BAM demo into %s", graph.config.server)
    seed_supporting_entities(graph)
    assets = seed_datasets(graph)
    dps = seed_data_products(graph, assets)
    seed_bi_assets_and_lineage(graph, assets, dps)
    apps = seed_applications(graph, assets)
    flat_demo = seed_application_as_dp_parent_demo(graph)
    seed_external_lineage_dependencies(graph, assets)
    logger.info("Waiting for search index refresh...")
    time.sleep(8)
    verify(graph)
    logger.info(
        "Done. Root DPs: %d  Apps: %d  Flat-under-App DPs: %d",
        len(dps),
        len(apps),
        len(flat_demo) - 1,
    )
    logger.info("Marketplace: %s/marketplace", "http://localhost:9002")
    logger.info(
        "NATIVE hierarchy: http://localhost:9002/dataProduct/urn:li:dataProduct:bam_trade_surveillance"
    )
    logger.info(
        "APP-as-1-level-parent: http://localhost:9002/application/urn:li:application:capital_markets_dp_portfolio"
    )


if __name__ == "__main__":
    main()
