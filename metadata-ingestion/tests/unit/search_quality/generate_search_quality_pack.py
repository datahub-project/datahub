"""Write search-quality judgments and the Cranfield shard into the datapack.

The long-tail catalog is the committed MCP shards. This script does not read
Elasticsearch index snapshots. Judgments stay in judgments.json and are not
listed in index.json. Regeneration is deterministic: audit stamps use one
fixed time.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

PACK_TIME = 1700000000000
ACTOR = "urn:li:corpuser:datahub"
CRANFIELD_PLATFORM = "urn:li:dataPlatform:cranfield"
PROBE_PLATFORM = "urn:li:dataPlatform:catalogprobe"
PROBE_PROPERTY = "urn:li:structuredProperty:catalogProbeTier"
LONG_TAIL_SHARDS = ("03-long-tail.json", "04-long-tail-timeseries.json")

KEYWORD_ENTITY_TYPES = [
    "chart",
    "container",
    "dashboard",
    "dataset",
    "domain",
    "tag",
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def pack_dir(root: Path | None = None) -> Path:
    return (
        root or repo_root()
    ) / "metadata-io/src/test/resources/datapacks/search-quality"


def stamp() -> dict[str, Any]:
    return {"time": PACK_TIME, "actor": ACTOR}


def change_stamps() -> dict[str, Any]:
    return {"created": stamp(), "lastModified": stamp()}


def mcp(entity_urn: str, aspect_name: str, aspect: dict[str, Any]) -> dict[str, Any]:
    entity_type = entity_urn.split(":")[2]
    return {
        "entityType": entity_type,
        "entityUrn": entity_urn,
        "changeType": "UPSERT",
        "aspectName": aspect_name,
        "aspect": {"json": aspect},
    }


def dataset_urn(platform: str, name: str, origin: str = "PROD") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{origin})"


def parse_cranfield_documents(path: Path) -> list[dict[str, str]]:
    documents: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    section: str | None = None
    for raw_line in path.read_text(errors="replace").splitlines():
        if raw_line.startswith(".I "):
            if current:
                documents.append(current)
            current = {
                "id": str(int(raw_line[3:].strip())),
                "title": "",
                "abstract": "",
            }
            section = None
            continue
        if current is None:
            continue
        if raw_line.startswith(".") and len(raw_line) == 2:
            marker = raw_line[1]
            section = {"T": "title", "W": "abstract"}.get(marker)
            continue
        if section:
            current[section] = (current[section] + " " + raw_line.strip()).strip()
    if current:
        documents.append(current)
    return documents


def parse_cranfield_queries(path: Path) -> list[dict[str, str]]:
    """Parse cran.qry, keying each query by its position rather than its label.

    The .I labels in cran.qry are the original Cranfield query numbers. They skip
    values and run up to 365, but cranqrel numbers the same 225 queries 1..225 by
    their order in this file. Position is therefore the only valid join key.
    """
    queries: list[dict[str, str]] = []
    collecting = False
    started = False
    text: list[str] = []
    for raw_line in path.read_text(errors="replace").splitlines():
        if raw_line.startswith(".I "):
            if started:
                queries.append(
                    {"id": str(len(queries) + 1), "text": " ".join(text).strip()}
                )
            started = True
            text = []
            collecting = False
            continue
        if raw_line.startswith(".W"):
            collecting = True
            continue
        if collecting:
            text.append(raw_line.strip())
    if started:
        queries.append({"id": str(len(queries) + 1), "text": " ".join(text).strip()})
    return queries


def cranfield_relevance(grade: int) -> int:
    """Convert a cranqrel grade to a gain level where a larger number is better.

    cranqrel runs the other way: 1 is a complete answer, 2 is highly relevant, 3 is
    a useful reference, 4 is of minimum interest, and -1 is of no interest. Scoring
    treats relevance as an exponential gain, so the scale has to be flipped and the
    -1 sentinel folded into a plain non-relevant 0.
    """
    if grade < 1 or grade > 4:
        return 0
    return 5 - grade


def parse_qrels(path: Path) -> dict[str, list[tuple[str, int]]]:
    grades: dict[str, list[tuple[str, int]]] = {}
    for line in path.read_text().splitlines():
        query_id, doc_id, relevance = line.split()
        grades.setdefault(str(int(query_id)), []).append(
            (str(int(doc_id)), cranfield_relevance(int(relevance)))
        )
    return grades


def cranfield_dataset(document: dict[str, str]) -> tuple[str, list[dict[str, Any]]]:
    name = f"cranfield.{document['id']}"
    urn = dataset_urn("cranfield", name)
    properties = {
        "name": document["title"],
        "qualifiedName": name,
        "description": document["abstract"],
    }
    return urn, [mcp(urn, "datasetProperties", properties)]


def platform_mcp(platform_name: str) -> list[dict[str, Any]]:
    urn = f"urn:li:dataPlatform:{platform_name}"
    display = platform_name
    short_name = platform_name if len(platform_name) <= 15 else platform_name[:15]
    return [
        mcp(
            urn,
            "dataPlatformInfo",
            {
                "name": short_name,
                "displayName": display,
                "type": "OTHERS",
                "datasetNameDelimiter": ".",
            },
        )
    ]


def definitions(platforms: Iterable[str]) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    for platform in sorted(set(platforms)):
        proposals.extend(platform_mcp(platform))
    proposals.append(
        mcp(
            PROBE_PROPERTY,
            "propertyDefinition",
            {
                "qualifiedName": "catalogProbeTier",
                "displayName": "Catalog probe tier",
                "valueType": "urn:li:dataType:datahub.string",
                "entityTypes": ["urn:li:entityType:datahub.dataset"],
                "cardinality": "SINGLE",
                "searchConfiguration": {"queryByDefault": True},
            },
        )
    )
    return proposals


def catalog_probes() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    exact = dataset_urn("catalogprobe", "orders_daily")
    typo = dataset_urn("catalogprobe", "widget_ledger")
    synonym = dataset_urn("catalogprobe", "staging_revenue_ledger")
    quoted = dataset_urn("catalogprobe", "quoted_phrase_alpha")
    prop = dataset_urn("catalogprobe", "tier_gold_asset")
    entities = [
        (
            exact,
            "Catalogprobe Orders Daily",
            "catalogprobe.orders_daily",
            "Daily orders fact table for the catalog probe.",
        ),
        (
            typo,
            "Catalogprobeledger",
            "catalogprobe.widget_ledger",
            "Ledger of widget inventory movements.",
        ),
        (
            synonym,
            "staging catalogprobeledger",
            "catalogprobe.staging_revenue_ledger",
            "Revenue collected in the staging environment.",
        ),
        (
            quoted,
            "catalogprobe quoted phrase",
            "catalogprobe.quoted_phrase_alpha",
            "A dataset whose name is an exact quoted phrase.",
        ),
        (
            prop,
            "Catalogprobe Tier Asset",
            "catalogprobe.tier_gold_asset",
            "Dataset carrying the catalog probe tier property.",
        ),
    ]
    proposals: list[dict[str, Any]] = []
    for urn, name, qualified, description in entities:
        proposals.append(
            mcp(
                urn,
                "datasetProperties",
                {
                    "name": name,
                    "qualifiedName": qualified,
                    "description": description,
                },
            )
        )
    proposals.append(
        mcp(
            prop,
            "structuredProperties",
            {
                "properties": [
                    {
                        "propertyUrn": PROBE_PROPERTY,
                        "values": [{"string": "catalogprobegold"}],
                        "created": stamp(),
                        "lastModified": stamp(),
                    }
                ]
            },
        )
    )
    # Business label on an existing long-tail table. The words are not in its name.
    proposals.append(
        mcp(
            dataset_urn("dbt", "calm-pagoda-323403.jaffle_shop.customers"),
            "structuredProperties",
            {
                "properties": [
                    {
                        "propertyUrn": PROBE_PROPERTY,
                        "values": [{"string": "lifetime value tier"}],
                        "created": stamp(),
                        "lastModified": stamp(),
                    }
                ]
            },
        )
    )
    judgments = [
        probe(
            "catalog-exact-name",
            "Catalogprobe Orders Daily",
            exact,
        ),
        probe("catalog-typo", "catalogprobeledgr", typo),
        probe("catalog-synonym", "stg catalogprobeledger", synonym),
        probe("catalog-quoted", '"catalogprobe quoted phrase"', quoted),
        probe("catalog-property", "catalogprobegold", prop),
    ]
    return proposals, judgments


def catalog_query(
    query_id: str, query: str, grades: list[tuple[str, int]]
) -> dict[str, Any]:
    """One long-tail catalog query.

    Relevance uses the Cranfield gain scale: 4 is the asset whose name, title, or
    description is the query; 3 is that asset on another platform or a close
    variant; 2 is a related asset a user would still open; 1 is a neighbor.
    """
    return {
        "id": query_id,
        "group": "longtail",
        "query": query,
        "surfaces": ["keyword"],
        "entityTypes": KEYWORD_ENTITY_TYPES,
        "grades": [{"urn": urn, "relevance": relevance} for urn, relevance in grades],
    }


def probe(query_id: str, query: str, urn: str) -> dict[str, Any]:
    return {
        "id": query_id,
        "group": "catalog-probe",
        "query": query,
        "surfaces": ["keyword"],
        "entityTypes": ["dataset"],
        "grades": [{"urn": urn, "relevance": 1}],
        "exactTopHit": urn,
    }


def semantic_documents() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    docs = [
        (
            "semantic-getting-started",
            "Getting Started with DataHub",
            "guide",
            """Getting Started with DataHub

Introduction
DataHub is a modern data catalog designed to help you discover, understand, and trust your data.

Quick Start
1. Navigate to the DataHub UI
2. Use the search bar to find datasets
3. Explore metadata, lineage, and documentation

Key Features
- Search: Find data assets across your organization
- Lineage: Understand data dependencies and flow
- Documentation: Add and view rich documentation
- Governance: Manage access and policies
""",
        ),
        (
            "semantic-access-request",
            "Data Access Request Process",
            "process",
            """Data Access Request Process

Overview
This document describes how to request access to data assets in our data platform.

Access Levels

Read Access
- View data in dashboards and reports
- Run queries on approved datasets

Write Access
- Modify existing records
- Requires additional approval

Request Steps
1. Identify the dataset you need access to
2. Submit a request through the data portal
3. Wait for owner approval (typically 24-48 hours)
4. Access will be granted automatically upon approval
""",
        ),
        (
            "semantic-churn",
            "Machine Learning Model: Churn Prediction",
            "model",
            """Churn Prediction Model

Model Overview
This model predicts the probability of customer churn within the next 30 days.

Features
- Days since last login
- Purchase frequency
- Support ticket count

Performance Metrics
- AUC-ROC: 0.87
- Precision: 0.82
- F1 Score: 0.80
""",
        ),
    ]
    proposals = []
    urns = []
    for doc_id, title, sub_type, text in docs:
        urn = f"urn:li:document:{doc_id}"
        urns.append(urn)
        proposals.append(
            mcp(
                urn,
                "documentInfo",
                {
                    "title": title,
                    "status": {"state": "PUBLISHED"},
                    "contents": {"text": text},
                    "created": stamp(),
                    "lastModified": stamp(),
                    "customProperties": {"subType": sub_type},
                },
            )
        )
        proposals.append(mcp(urn, "subTypes", {"typeNames": [sub_type]}))
    judgment = {
        "id": "semantic-access-request",
        "group": "semantic-probe",
        "query": "how to request data access permissions",
        "surfaces": ["semantic"],
        "entityTypes": ["document"],
        "grades": [{"urn": urn, "relevance": 1} for urn in urns],
        "rankAbove": {"urn": urns[1], "above": [urns[0], urns[2]]},
        "documents": [
            {"urn": urn, "title": title, "text": text, "subType": sub_type}
            for (urn, (_doc_id, title, sub_type, text)) in zip(urns, docs, strict=True)
        ],
    }
    return proposals, judgment


def longtail_judgments() -> list[dict[str, Any]]:
    """Catalog searches over the long-tail entities in this datapack."""
    pet_dbt = dataset_urn("dbt", "long_tail_companions.adoption.pet_profiles")
    pet_mongo = dataset_urn("mongodb", "adoption.pet_profiles")
    pet_s3 = dataset_urn("s3", "longtail-core-data/mongo/adoption/pet_profiles")
    pet_snowflake = dataset_urn(
        "snowflake", "long_tail_companions.adoption.pet_profiles"
    )
    pet_dashboard = "urn:li:dashboard:(looker,dashboards.14)"
    orders = [
        dataset_urn("bigquery", "calm-pagoda-323403.jaffle_shop.orders"),
        dataset_urn("dbt", "calm-pagoda-323403.jaffle_shop.orders"),
        dataset_urn("postgres", "calm-pagoda-323403.jaffle_shop.orders"),
    ]
    customers = [
        dataset_urn("bigquery", "calm-pagoda-323403.jaffle_shop.customers"),
        dataset_urn("dbt", "calm-pagoda-323403.jaffle_shop.customers"),
        dataset_urn("postgres", "calm-pagoda-323403.jaffle_shop.customers"),
    ]
    monthly_dashboard = "urn:li:dashboard:(looker,dashboards.15)"
    monthly_ytd = "urn:li:dashboard:(looker,dashboards.34)"
    monthly_explore = dataset_urn(
        "looker", "long_tail_companions.explore.monthly_adoption_projection"
    )
    monthly_view = dataset_urn(
        "looker", "long_tail_companions.view.monthly_adoption_projection"
    )
    monthly_table = dataset_urn(
        "snowflake", "long_tail_companions.finance.monthly_adoption_projection"
    )
    monthly_charts = [
        "urn:li:chart:(looker,dashboard_elements.97)",
        "urn:li:chart:(looker,dashboard_elements.163)",
    ]
    shipping = [
        "urn:li:dashboard:(looker,dashboards.5)",
        "urn:li:dashboard:(looker,dashboards.thelook::shipping_logistics__operations_overview)",
    ]
    adoptions = [
        dataset_urn("dbt", "long_tail_companions.adoption.adoptions"),
        dataset_urn("snowflake", "long_tail_companions.adoption.adoptions"),
    ]
    repeat_charts = [
        "urn:li:chart:(looker,dashboard_elements.10)",
        "urn:li:chart:(looker,dashboard_elements.651bfaac0ef25b45d1d5931e8bb6a8b7)",
    ]
    business_pulse = [
        "urn:li:dashboard:(looker,dashboards.1)",
        "urn:li:dashboard:(looker,dashboards.thelook::business_pulse)",
    ]
    dogs = [
        dataset_urn("dbt", "long_tail_companions.analytics.dogs_in_movies"),
        dataset_urn("snowflake", "long_tail_companions.analytics.dogs_in_movies"),
    ]
    pet_grades = [
        (pet_dbt, 4),
        (pet_mongo, 4),
        (pet_snowflake, 4),
        (pet_s3, 2),
        (pet_dashboard, 2),
    ]
    column_grades = [
        (customers[0], 3),
        (customers[1], 4),
        (customers[2], 4),
    ]
    description_grades = [
        (customers[0], 2),
        (customers[1], 4),
        (customers[2], 4),
    ]
    jaffle_tables = orders + customers
    jaffle_container = "urn:li:container:348c96555971d3f5c1ffd7dd2e7446cb"
    adoptable_charts = [
        "urn:li:chart:(looker,dashboard_elements.126)",
        "urn:li:chart:(looker,dashboard_elements.88)",
    ]
    returns = [
        "urn:li:dashboard:(looker,dashboards.21)",
        "urn:li:dashboard:(looker,dashboards.33)",
    ]
    return [
        catalog_query("longtail-pet-profiles", "pet profiles", pet_grades),
        catalog_query(
            "longtail-qualified-orders",
            "jaffle_shop.orders",
            [(urn, 4) for urn in orders],
        ),
        catalog_query(
            "longtail-lifetime-value",
            "customer lifetime value",
            column_grades,
        ),
        catalog_query(
            "longtail-customer-description",
            "basic information about a customer",
            description_grades,
        ),
        catalog_query(
            "longtail-monthly-adoption",
            "monthly adoption projection",
            [
                (monthly_dashboard, 4),
                (monthly_ytd, 3),
                (monthly_explore, 4),
                (monthly_view, 3),
                (monthly_table, 3),
                (monthly_charts[0], 4),
                (monthly_charts[1], 4),
            ],
        ),
        catalog_query(
            "longtail-shipping",
            "shipping and logistics overview",
            [(urn, 4) for urn in shipping],
        ),
        catalog_query(
            "longtail-pet-adoptions-domain",
            "pet adoptions",
            [
                ("urn:li:domain:094dc54b-0ebc-40a6-a4cf-e1b75e8b8089", 4),
                ("urn:li:dashboard:(looker,dashboards.19)", 2),
                (adoptions[0], 2),
                (adoptions[1], 2),
            ],
        ),
        catalog_query(
            "longtail-repeat-purchase",
            "30 day repeat purchase rate",
            [(urn, 4) for urn in repeat_charts]
            + [(urn, 2) for urn in business_pulse]
            + [
                (
                    dataset_urn("looker", "thelook.explore.order_items"),
                    1,
                )
            ],
        ),
        catalog_query(
            "longtail-dogs-in-movies",
            "dogs in movies",
            [(urn, 4) for urn in dogs],
        ),
        catalog_query(
            "longtail-return-window",
            "items returned within 30 days",
            [(urn, 4) for urn in returns],
        ),
        catalog_query("longtail-identifier", "pet_profiles", pet_grades),
        catalog_query(
            "longtail-column-identifier",
            "customer_lifetime_value",
            column_grades,
        ),
        catalog_query(
            "longtail-pii-tag",
            "pii",
            [
                ("urn:li:tag:pii", 4),
                (customers[1], 2),
                (customers[2], 2),
            ],
        ),
        catalog_query(
            "longtail-campaign-dashboard",
            "campaign driven adoptions",
            [("urn:li:dashboard:(looker,dashboards.20)", 4)],
        ),
        catalog_query(
            "longtail-duplicate-charts",
            "adoptable pets by region",
            [(urn, 4) for urn in adoptable_charts],
        ),
        catalog_query(
            "longtail-schema-name",
            "jaffle shop",
            [(jaffle_container, 4)] + [(urn, 2) for urn in jaffle_tables],
        ),
        catalog_query(
            "longtail-quoted-title",
            '"Shipping Logistics"',
            [(urn, 4) for urn in shipping],
        ),
        catalog_query("longtail-case", "PET PROFILES", pet_grades),
        catalog_query("longtail-quoted-identifier", '"pet_profiles"', pet_grades),
        catalog_query(
            "longtail-quoted-description",
            '"basic information about a customer"',
            description_grades,
        ),
        catalog_query(
            "longtail-typo",
            "dogs_in_movis",
            [(urn, 4) for urn in dogs],
        ),
        catalog_query(
            "longtail-column-prefix",
            "customer_lifetime",
            column_grades,
        ),
        catalog_query(
            "longtail-extra-token",
            "pet profiles watercolor",
            pet_grades,
        ),
        catalog_query(
            "longtail-business-label",
            "lifetime value tier",
            [(customers[1], 4)],
        ),
    ]


def ranking_judgments() -> list[dict[str, Any]]:
    pet_profiles = [
        dataset_urn("dbt", "long_tail_companions.adoption.pet_profiles"),
        dataset_urn("snowflake", "long_tail_companions.adoption.pet_profiles"),
    ]
    return [
        {
            "id": "ranking-pet-profiles",
            "group": "ranking",
            "query": "pet profiles",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            "orderedContains": ["pet_profiles", "pet_profiles"],
        },
        {
            "id": "ranking-pet-profile",
            "group": "ranking",
            "query": "pet profile",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            # Exact title "Pet Profile" may occupy one of these slots. The check is
            # that both tables are still in the top 3, in either order.
            "containsInTop": {"n": 3, "values": ["pet_profiles", "pet_profiles"]},
        },
        {
            "id": "ranking-return-rate",
            "group": "ranking",
            "query": "*",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [],
            "filter": {
                "field": "glossaryTerms_1",
                "condition": "EQUAL",
                "value": "urn:li:glossaryTerm:Ecommerce.ReturnRate",
            },
            "present": [
                "dbt,long_tail_companions.adoption.pet_profiles",
                "looker,thelook.explore.order_items",
                "snowflake,long_tail_companions.analytics.b2c_ecommerce",
            ],
        },
        {
            "id": "ranking-partial-pet-details",
            "group": "ranking",
            "query": "analytics.pet_details",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [
                {
                    "urn": dataset_urn(
                        "snowflake", "long_tail_companions.analytics.pet_details"
                    ),
                    "relevance": 2,
                },
                {
                    "urn": dataset_urn(
                        "dbt", "long_tail_companions.analytics.pet_details"
                    ),
                    "relevance": 1,
                },
            ],
            "orderedContains": [
                "snowflake,long_tail_companions.analytics.pet_details",
            ],
            # Elasticsearch 8 places the dbt copy third, behind a neighboring
            # Snowflake table. Require it in the top 3 rather than a fixed slot.
            "containsInTop": {
                "n": 3,
                "values": ["dbt,long_tail_companions.analytics.pet_details"],
            },
        },
        {
            "id": "ranking-collaborative-actionitems",
            "group": "ranking",
            "query": "collaborative actionitems",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [],
            "orderedContains": [
                "collaborative_actionitems,",
                "collaborative_actionitems_old",
            ],
        },
        {
            "id": "ranking-customer-orders",
            "group": "ranking",
            "query": "customer orders",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [],
            "orderedContains": ["customer_orders,"],
            "scoresStrictlyDescending": True,
        },
        {
            "id": "ranking-row-count",
            "group": "ranking",
            "query": "*",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [
                {
                    "urn": dataset_urn(
                        "dbt", "long_tail_companions.analytics.dogs_in_movies"
                    ),
                    "relevance": 1,
                }
            ],
            "filter": {"field": "rowCount", "condition": "EQUAL", "value": "68"},
            "exactTopHit": dataset_urn(
                "dbt", "long_tail_companions.analytics.dogs_in_movies"
            ),
        },
        {
            "id": "ranking-lineage-pet-profiles",
            "group": "ranking",
            "query": "pet profiles",
            "surfaces": ["lineage"],
            "entityTypes": ["dataset"],
            "candidateUrns": pet_profiles,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            "orderedContains": ["pet_profiles"],
        },
        {
            "id": "ranking-pet-profiles-identifier",
            "group": "ranking",
            "query": "pet_profiles",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            "orderedContains": ["pet_profiles"],
            "present": [
                "dbt,long_tail_companions.adoption.pet_profiles",
                "snowflake,long_tail_companions.adoption.pet_profiles",
            ],
        },
        {
            "id": "ranking-pet-profiles-case",
            "group": "ranking",
            "query": "PET PROFILES",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            "orderedContains": ["pet_profiles"],
        },
        {
            "id": "ranking-pet-profiles-quoted",
            "group": "ranking",
            "query": '"pet_profiles"',
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            "orderedContains": ["pet_profiles"],
        },
        {
            "id": "ranking-pii",
            "group": "ranking",
            "query": "pii",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": "urn:li:tag:pii", "relevance": 1}],
            "exactTopHit": "urn:li:tag:pii",
        },
        {
            "id": "ranking-campaign-adoptions",
            "group": "ranking",
            "query": "campaign driven adoptions",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [
                {
                    "urn": "urn:li:dashboard:(looker,dashboards.20)",
                    "relevance": 1,
                }
            ],
            "exactTopHit": "urn:li:dashboard:(looker,dashboards.20)",
        },
        {
            "id": "ranking-shipping-quoted",
            "group": "ranking",
            "query": '"Shipping Logistics"',
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [
                {"urn": "urn:li:dashboard:(looker,dashboards.5)", "relevance": 1}
            ],
            "exactTopHit": "urn:li:dashboard:(looker,dashboards.5)",
        },
    ]


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")


def platform_name(urn: str) -> str | None:
    marker = "urn:li:dataPlatform:"
    if marker in urn:
        return urn.split(marker, 1)[1].split(",", 1)[0].rstrip(")")
    for entity in ("chart", "dashboard"):
        prefix = f"urn:li:{entity}:("
        if urn.startswith(prefix) and urn.endswith(")"):
            return urn[len(prefix) : -1].split(",", 1)[0]
    return None


def load_mcps(directory: Path) -> dict[str, list[dict[str, Any]]]:
    index = json.loads((directory / "index.json").read_text())
    loaded = {}
    for entry in index["files"]:
        path = entry["path"] if isinstance(entry, dict) else entry
        loaded[path] = json.loads((directory / path).read_text())
    return loaded


def validate_judgments(judgments: dict[str, Any], seen: set[str]) -> None:
    allowed_groups = {
        "benchmark",
        "ranking",
        "catalog-probe",
        "semantic-probe",
        "longtail",
    }
    allowed_surfaces = {"keyword", "semantic", "hybrid", "lineage"}
    for judgment in judgments["judgments"]:
        if judgment["group"] not in allowed_groups:
            raise AssertionError(f"Unknown group {judgment['group']}")
        surfaces = set(judgment["surfaces"])
        if not surfaces <= allowed_surfaces:
            raise AssertionError(f"Unknown surface on {judgment['id']}")
        if judgment["group"] == "benchmark" and surfaces & {"semantic", "hybrid"}:
            raise AssertionError(
                f"Benchmark query {judgment['id']} is not keyword-only"
            )
        for grade in judgment.get("grades") or []:
            if not isinstance(grade["relevance"], int):
                raise AssertionError(f"Malformed grade on {judgment['id']}")
            if grade["urn"] not in seen:
                raise AssertionError(
                    f"Judgment {judgment['id']} references unloaded {grade['urn']}"
                )
        for urn in judgment.get("candidateUrns") or []:
            if urn not in seen:
                raise AssertionError(f"Candidate {urn} is not in the pack")
        exact = judgment.get("exactTopHit")
        if exact and exact not in seen:
            raise AssertionError(f"Top hit {exact} is not in the pack")


def validate_pack(directory: Path) -> None:
    loaded = load_mcps(directory)
    seen: set[str] = set()
    for path, proposals in loaded.items():
        if not isinstance(proposals, list) or not proposals:
            raise AssertionError(f"{path} must be a non-empty MCP array")
        for proposal in proposals:
            if "systemMetadata" in proposal:
                raise AssertionError(f"{path} must not set systemMetadata")
            urn = proposal["entityUrn"]
            seen.add(urn)
            if proposal["aspectName"].endswith("Key"):
                raise AssertionError(
                    "Key aspects are derived from the URN at index time"
                )
            payload = proposal.get("aspect", {}).get("json")
            if not isinstance(payload, dict) or not payload:
                raise AssertionError(
                    f"{path} {proposal.get('aspectName')} on {urn} is empty. "
                    "The file source only accepts an aspect object with fields."
                )
    for name in LONG_TAIL_SHARDS:
        if name not in loaded:
            raise AssertionError(f"{name} is missing from the datapack")
    judgments = json.loads((directory / "judgments.json").read_text())
    validate_judgments(judgments, seen)
    cranfield_urns = {
        proposal["entityUrn"] for proposal in loaded["02-cranfield-datasets.json"]
    }
    if len(cranfield_urns) != 1400:
        raise AssertionError(
            f"Expected 1400 Cranfield datasets, found {len(cranfield_urns)}"
        )
    benchmark = [
        item for item in judgments["judgments"] if item["group"] == "benchmark"
    ]
    if len(benchmark) != 225:
        raise AssertionError(f"Expected 225 benchmark queries, found {len(benchmark)}")


def generate(root: Path | None = None) -> Path:
    root = root or repo_root()
    directory = pack_dir(root)
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / "source/cranfield"
    documents = parse_cranfield_documents(source / "cran.all.1400")
    queries = parse_cranfield_queries(source / "cran.qry")
    qrels = parse_qrels(source / "cranqrel")
    if len(documents) != 1400 or len(queries) != 225:
        raise AssertionError(
            "Cranfield source did not parse to 1400 documents and 225 queries"
        )

    cranfield_mcps: list[dict[str, Any]] = []
    for document in documents:
        _urn, proposals = cranfield_dataset(document)
        cranfield_mcps.extend(proposals)

    platforms = {"cranfield", "catalogprobe"}
    for name in LONG_TAIL_SHARDS:
        path = directory / name
        if not path.is_file():
            raise AssertionError(
                f"{name} is the long-tail catalog in the datapack. "
                "It is not rebuilt from an Elasticsearch snapshot."
            )
        for proposal in json.loads(path.read_text()):
            platform = platform_name(proposal["entityUrn"])
            if platform:
                platforms.add(platform)

    probe_mcps, probe_judgments = catalog_probes()
    semantic_mcps, semantic_judgment = semantic_documents()
    benchmark = []
    for query in queries:
        grades = [
            {
                "urn": dataset_urn("cranfield", f"cranfield.{doc_id}"),
                "relevance": relevance,
            }
            for doc_id, relevance in qrels.get(query["id"], [])
        ]
        benchmark.append(
            {
                "id": f"cranfield-{query['id']}",
                "group": "benchmark",
                "query": query["text"],
                "surfaces": ["keyword"],
                "entityTypes": ["dataset"],
                "grades": grades,
            }
        )

    shards = {
        "01-definitions.json": definitions(platforms),
        "02-cranfield-datasets.json": cranfield_mcps,
        "05-catalog-probes.json": probe_mcps,
        "06-semantic-probes.json": semantic_mcps,
    }
    index = {
        "version": "1",
        "files": [
            {"path": "01-definitions.json", "wait_for_completion": True},
            {"path": "02-cranfield-datasets.json"},
            {"path": "03-long-tail.json"},
            {"path": "04-long-tail-timeseries.json"},
            {"path": "05-catalog-probes.json"},
            {"path": "06-semantic-probes.json"},
        ],
    }
    for name, payload in shards.items():
        write_json(directory / name, payload)
    write_json(directory / "index.json", index)
    judgments: dict[str, Any] = {
        "referenceTimestamp": PACK_TIME,
        "floors": {
            # Template path is the fulltext soft description match. The Java builder
            # floor is lower: with templates skipped, the light-then-full cascade
            # still serves the light query for these questions.
            "elasticsearch": {
                "benchmarkNdcgAt10": 0.31,
                "javaBuilderBenchmarkNdcgAt10": 0.20,
                # Catalog mean. The floors stay separate because the template
                # and the Java builder run different queries.
                "longtailNdcgAt10": 0.67,
                "javaBuilderLongtailNdcgAt10": 0.66,
            },
            "opensearch": {
                "benchmarkNdcgAt10": 0.31,
                "javaBuilderBenchmarkNdcgAt10": 0.20,
                "longtailNdcgAt10": 0.67,
                "javaBuilderLongtailNdcgAt10": 0.66,
            },
        },
        "judgments": benchmark
        + longtail_judgments()
        + ranking_judgments()
        + probe_judgments
        + [semantic_judgment],
    }
    write_json(directory / "judgments.json", judgments)
    kept = sum(
        len(json.loads((directory / name).read_text())) for name in LONG_TAIL_SHARDS
    )
    counts = Counter(item["group"] for item in judgments["judgments"])
    print(
        f"Wrote search-quality pack to {directory} "
        f"mcps={sum(len(items) for items in shards.values()) + kept} judgments={counts}"
    )
    return directory


if __name__ == "__main__":
    generate()
