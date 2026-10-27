"""Write search-quality judgments and the Cranfield shard into the datapack.

The longtail-ecommerce catalog is the committed business shard. Ecommerce
entities in that shard came from showcase-ecommerce and are not rebuilt.
This script does not read Elasticsearch index snapshots. Judgments stay in
judgments.json and are not listed in index.json. Regeneration is
deterministic: audit stamps use one fixed time.
"""

from __future__ import annotations

import gc
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

PACK_TIME = 1700000000000
ACTOR = "urn:li:corpuser:datahub"
CRANFIELD_PLATFORM = "urn:li:dataPlatform:cranfield"
PROBE_PLATFORM = "urn:li:dataPlatform:catalogprobe"
PROBE_PROPERTY = "urn:li:structuredProperty:catalogProbeTier"
# Nonce words. A real word such as "tier" is already a dataset name, and that
# name outranks the property, so the light rung would stop on the wrong hit.
FULLTEXT_PROBE_VALUE = "Zephyrical Quorvane Plinth"
FULLTEXT_PROBE_TOKENS = ("zephyrical", "quorvane", "plinth")
FULLTEXT_PROBE_FIELD = "structuredProperties.catalogProbeTier"
SEARCHABLE_TEXT_FIELDS = frozenset(
    {"name", "qualifiedName", "description", "title", "displayName", "text"}
)
STRING_OPT_IN_PROPERTY = "urn:li:structuredProperty:packStringSearch"
STRING_OPT_OUT_PROPERTY = "urn:li:structuredProperty:packStringFilter"
URN_OPT_IN_PROPERTY = "urn:li:structuredProperty:packUrnSearch"
URN_OPT_OUT_PROPERTY = "urn:li:structuredProperty:packUrnFilter"
BOOSTED_PROPERTY = "urn:li:structuredProperty:packBoostedLabel"
SORT_SET_PROPERTY = "urn:li:structuredProperty:sortProbeSet"
SORT_COUNT_PROPERTY = "urn:li:structuredProperty:sortProbeCount"
SORT_SET_VALUE = "sortprobe"
# Nonce tokens. Each one is absent from the dataset name, qualified name, and
# description, so a hit can come only from the isolated field.
FIELD_PROBE_COLUMN = "brindlepath"
FIELD_PROBE_DESCRIPTION = "brindledesc"
FIELD_PROBE_TAG = "brindletag"
FIELD_PROBE_TERM = "brindleterm"
FIELD_PROBE_OTHER = "brindlebin"
FIELD_PROBE_STEM = "thimble"
FIELD_PROBE_STEM_QUERY = "thimbles"
FIELD_PROBE_TOKENS = (
    FIELD_PROBE_COLUMN,
    FIELD_PROBE_DESCRIPTION,
    FIELD_PROBE_TAG,
    FIELD_PROBE_TERM,
    FIELD_PROBE_OTHER,
    FIELD_PROBE_STEM,
    FIELD_PROBE_STEM_QUERY,
)
BUSINESS_SHARD = "03-longtail-ecommerce.json"
TIMESERIES_SHARD = "04-longtail-ecommerce-timeseries.json"
LONG_TAIL_SHARDS = (BUSINESS_SHARD, TIMESERIES_SHARD)
SHOWCASE_PREFIX = "b2fd91."
# Container ids from the imported catalog. They do not carry the b2fd91 prefix.
IMPORTED_CONTAINER_URNS = frozenset(
    {
        "urn:li:container:0e66a4ce3f0b2777d96fa518517c02fb",
        "urn:li:container:1a138c9fcb5dc4d763aed13939e022f2",
        "urn:li:container:4aa1e0be707b5eb015fd4ae8a0cee4e6",
        "urn:li:container:50c5d36097aca2fe8b0041805c21f13a",
        "urn:li:container:64a5f47928e5c808819f1385be046bc0",
        "urn:li:container:6c6be2267e33f5735445a49ee7a37573",
        "urn:li:container:6f8674b1605468e8622e14df196d84d1",
        "urn:li:container:7ca6ea867f3c1038d55fda4688e26dab",
        "urn:li:container:7eeb369896b78a39b55f38238779bd8b",
        "urn:li:container:82b021ed6470183a388677899ff77153",
        "urn:li:container:a1b63f1e0516d47949181f804bc4ae9b",
        "urn:li:container:d14215d4dacbab259f768b155825550e",
        "urn:li:container:d6b6aada30bc0a4a5a891d06c9c1815a",
        "urn:li:container:d6fc0dbb637e11fe788d53f86f587a96",
    }
)
IMPORTED_PROPOSAL_COUNT = 2826
IMPORTED_PROPERTY_URNS = (
    "urn:li:structuredProperty:showcase.dataFreshnessSla",
    "urn:li:structuredProperty:showcase.dataQualityScore",
    "urn:li:structuredProperty:showcase.retentionPeriod",
    "urn:li:structuredProperty:showcase.costCenter",
    "urn:li:structuredProperty:showcase.escalationContact",
)
ENTITY_BOOST_NAME = "Order Entry Analytics entityboost"
ENTITY_BOOST_TERM = "urn:li:glossaryTerm:entityboost.orderEntry"
ENTITY_BOOST_NODE = "urn:li:glossaryNode:entityboost.orderEntry"
ENTITY_BOOST_PRODUCT = "urn:li:dataProduct:entityboost.orderEntry"
ENTITY_BOOST_DOMAIN = "urn:li:domain:entityboost.orderEntry"
ENTITY_BOOST_DATASET = (
    "urn:li:dataset:(urn:li:dataPlatform:catalogprobe,entityboost.orderEntry,PROD)"
)

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


def mcp(
    entity_urn: str,
    aspect_name: str,
    aspect: dict[str, Any],
    change_type: str = "UPSERT",
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    entity_type = entity_urn.split(":")[2]
    proposal: dict[str, Any] = {
        "entityType": entity_type,
        "entityUrn": entity_urn,
        "changeType": change_type,
        "aspectName": aspect_name,
        "aspect": {"json": aspect},
    }
    if headers:
        proposal["headers"] = headers
    return proposal


def dataset_urn(platform: str, name: str, origin: str = "PROD") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{origin})"


AUTOCOMPLETE_NAME = "zzprefixprobe"
AUTOCOMPLETE_DATASET = dataset_urn("catalogprobe", AUTOCOMPLETE_NAME)

STRING_OPT_IN_DATASET = dataset_urn(
    "dbt", "long_tail_companions.analytics.dogs_in_movies"
)
URN_OPT_IN_DATASET = dataset_urn(
    "snowflake", "long_tail_companions.analytics.b2c_ecommerce"
)
BOOSTED_PROPERTY_DATASET = dataset_urn(
    "dbt", "long_tail_companions.adoption.pet_profiles"
)

# The three tables whose name is exactly pet_profiles.
PET_PROFILE_TABLES = [
    dataset_urn("dbt", "long_tail_companions.adoption.pet_profiles"),
    dataset_urn("mongodb", "adoption.pet_profiles"),
    dataset_urn("snowflake", "long_tail_companions.adoption.pet_profiles"),
]
PET_PROFILE_S3 = dataset_urn("s3", "longtail-core-data/mongo/adoption/pet_profiles")
PET_PROFILE_DASHBOARD = "urn:li:dashboard:(looker,dashboards.14)"
PET_PROFILE_DEEP_DIVE = "urn:li:dashboard:(looker,dashboards.30)"
PET_PROFILE_GRADES = [
    *[(urn, 4) for urn in PET_PROFILE_TABLES],
    (PET_PROFILE_S3, 2),
    (PET_PROFILE_DASHBOARD, 2),
]


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


def _yaml_scalar(value: str) -> Any:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    if value == "true":
        return True
    if value == "false":
        return False
    return value


def bootstrap_platform_aspects(root: Path | None = None) -> dict[str, dict[str, Any]]:
    """dataPlatformInfo aspects from the bootstrap platform registry."""
    path = (
        (root or repo_root())
        / "metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml"
    )
    platforms: dict[str, dict[str, Any]] = {}
    platform_name: str | None = None
    aspect: dict[str, Any] = {}
    in_aspect = False
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.startswith("- entityUrn:"):
            if platform_name and aspect:
                platforms[platform_name] = aspect
            platform_name = line.split("urn:li:dataPlatform:", 1)[1].strip()
            aspect = {}
            in_aspect = False
            continue
        if line.startswith("  aspect:"):
            in_aspect = True
            continue
        if in_aspect and line.startswith("    "):
            key, value = line.strip().split(":", 1)
            aspect[key] = _yaml_scalar(value)
            continue
        in_aspect = False
    if platform_name and aspect:
        platforms[platform_name] = aspect
    return platforms


def platform_mcp(
    platform_name: str, bootstrap: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    urn = f"urn:li:dataPlatform:{platform_name}"
    aspect = bootstrap.get(platform_name)
    if aspect is None:
        # Fixture platforms (cranfield, catalogprobe, ranking, longtail_schemas)
        # are not in the bootstrap registry.
        short_name = platform_name if len(platform_name) <= 15 else platform_name[:15]
        aspect = {
            "name": short_name,
            "displayName": platform_name,
            "type": "OTHERS",
            "datasetNameDelimiter": ".",
        }
    else:
        aspect = dict(aspect)
        # DataPlatformInfo.name is limited to 15 characters.
        if isinstance(aspect.get("name"), str) and len(aspect["name"]) > 15:
            aspect["name"] = aspect["name"][:15]
    # Create the aspect only when it is missing. If-None-Match drops the write
    # when bootstrap already stored dataPlatformInfo, instead of failing the load.
    return [
        mcp(
            urn,
            "dataPlatformInfo",
            aspect,
            change_type="CREATE",
            headers={"If-None-Match": "*"},
        )
    ]


def definitions(platforms: Iterable[str]) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    bootstrap = bootstrap_platform_aspects()
    for platform in sorted(set(platforms)):
        proposals.extend(platform_mcp(platform, bootstrap))
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
    for urn, qualified_name, value_type, search_configuration in [
        (
            STRING_OPT_IN_PROPERTY,
            "packStringSearch",
            "urn:li:dataType:datahub.string",
            {"queryByDefault": True},
        ),
        (
            STRING_OPT_OUT_PROPERTY,
            "packStringFilter",
            "urn:li:dataType:datahub.string",
            {"queryByDefault": False},
        ),
        (
            URN_OPT_IN_PROPERTY,
            "packUrnSearch",
            "urn:li:dataType:datahub.urn",
            {"queryByDefault": True},
        ),
        (
            URN_OPT_OUT_PROPERTY,
            "packUrnFilter",
            "urn:li:dataType:datahub.urn",
            {"queryByDefault": False},
        ),
        (
            BOOSTED_PROPERTY,
            "packBoostedLabel",
            "urn:li:dataType:datahub.string",
            {"queryByDefault": True, "boostScore": 40.0},
        ),
    ]:
        proposals.append(
            mcp(
                urn,
                "propertyDefinition",
                {
                    "qualifiedName": qualified_name,
                    "displayName": qualified_name,
                    "valueType": value_type,
                    "entityTypes": ["urn:li:entityType:datahub.dataset"],
                    "cardinality": "SINGLE",
                    "searchConfiguration": search_configuration,
                },
            )
        )
    proposals.append(
        mcp(
            SORT_SET_PROPERTY,
            "propertyDefinition",
            {
                "qualifiedName": "sortProbeSet",
                "displayName": "Sort probe set",
                "valueType": "urn:li:dataType:datahub.string",
                "entityTypes": [
                    "urn:li:entityType:datahub.dataset",
                    "urn:li:entityType:datahub.chart",
                ],
                "cardinality": "SINGLE",
                "searchConfiguration": {"queryByDefault": False},
            },
        )
    )
    proposals.append(
        mcp(
            SORT_COUNT_PROPERTY,
            "propertyDefinition",
            {
                "qualifiedName": "sortProbeCount",
                "displayName": "Sort probe count",
                "valueType": "urn:li:dataType:datahub.number",
                "entityTypes": ["urn:li:entityType:datahub.dataset"],
                "cardinality": "SINGLE",
                "searchConfiguration": {"queryByDefault": False},
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
    string_opt_out = dataset_urn("catalogprobe", "string_filter_asset")
    urn_opt_in = URN_OPT_IN_DATASET
    urn_opt_out = dataset_urn("catalogprobe", "urn_filter_asset")
    description_competitor = dataset_urn("catalogprobe", "boosted_description_asset")
    fulltext = dataset_urn("catalogprobe", "analyzer_asset")
    identity_name_only = dataset_urn("catalogprobe", "identity_name_only")
    identity_name_and_qualified = dataset_urn(
        "catalogprobe", "identity_name_and_qualified"
    )
    identity_path = dataset_urn("catalogprobe", "identity_path")
    identity_path_partial = dataset_urn("catalogprobe", "identity_path_partial")
    identity_earlier_segment = dataset_urn("catalogprobe", "identity_earlier_segment")
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
        (
            string_opt_out,
            "Catalogprobe String Filter Asset",
            "catalogprobe.string_filter_asset",
            "A dataset used to test a structured property excluded from full-text search.",
        ),
        (
            urn_opt_in,
            "Catalogprobe URN Search Asset",
            "catalogprobe.urn_search_asset",
            "A dataset used to test a URN structured property included in search configuration.",
        ),
        (
            urn_opt_out,
            "Catalogprobe URN Filter Asset",
            "catalogprobe.urn_filter_asset",
            "A dataset used to test a URN structured property excluded from full-text search.",
        ),
        (
            description_competitor,
            "Catalogprobe Boosted Description Asset",
            "catalogprobe.boosted_description_asset",
            "Dataset whose description contains the boosted property phrase: packboostalpha packboostbravo.",
        ),
        (
            fulltext,
            "Catalogprobe Analyzer Asset",
            "catalogprobe.analyzer_asset",
            "Dataset carrying a mixed-case structured property for analyzer probes.",
        ),
        (
            dataset_urn("catalogprobe", AUTOCOMPLETE_NAME),
            AUTOCOMPLETE_NAME,
            AUTOCOMPLETE_NAME,
            AUTOCOMPLETE_NAME,
        ),
    ]
    proposals: list[dict[str, Any]] = []
    proposals.append(
        mcp(
            identity_name_only,
            "datasetProperties",
            {
                "name": "identityprobe parity",
                "description": "Name-only identity comparison asset.",
            },
        )
    )
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
    proposals.extend(
        [
            mcp(
                identity_name_and_qualified,
                "datasetProperties",
                {
                    "name": "identityprobe parity",
                    "qualifiedName": "catalogprobe.identityprobe.parity",
                    "description": "Name and qualified-name identity comparison asset.",
                },
            ),
            mcp(
                identity_path,
                "datasetProperties",
                {
                    "name": "identityprobe_ledger",
                    "qualifiedName": "catalogprobe.analytics.identityprobe_ledger",
                    "description": "Bounded qualified-name path asset.",
                },
            ),
            mcp(
                identity_path_partial,
                "datasetProperties",
                {
                    "name": "identityprobe_ledger_archive",
                    "qualifiedName": "catalogprobe.analytics.identityprobe_ledger_archive",
                    "description": "Partial qualified-name path distractor.",
                },
            ),
            mcp(
                identity_earlier_segment,
                "datasetProperties",
                {
                    "name": "identityprobe_customer_snapshot",
                    "qualifiedName": "catalogprobe.identityprobe.ancestry.customer_snapshot",
                    "description": "Earlier qualified-name segment asset.",
                },
            ),
        ]
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
    # The phrase below is not this whole value, so the case-sensitive keyword
    # term cannot explain it. A stem of the last word matches only .stemmed.
    proposals.append(
        mcp(
            fulltext,
            "structuredProperties",
            {
                "properties": [
                    {
                        "propertyUrn": PROBE_PROPERTY,
                        "values": [{"string": FULLTEXT_PROBE_VALUE}],
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
    structured_assignments = [
        (
            STRING_OPT_IN_DATASET,
            STRING_OPT_IN_PROPERTY,
            "packstringoptin",
        ),
        (
            URN_OPT_IN_DATASET,
            URN_OPT_IN_PROPERTY,
            "urn:li:dataset:(urn:li:dataPlatform:catalogprobe,packurnoptin,PROD)",
        ),
        (
            BOOSTED_PROPERTY_DATASET,
            BOOSTED_PROPERTY,
            "packboostalpha packboostbravo",
        ),
        (
            string_opt_out,
            STRING_OPT_OUT_PROPERTY,
            "packstringoptout",
        ),
        (
            urn_opt_out,
            URN_OPT_OUT_PROPERTY,
            "urn:li:corpuser:packurnoptout",
        ),
    ]
    for dataset, property_urn, value in structured_assignments:
        proposals.append(
            mcp(
                dataset,
                "structuredProperties",
                {
                    "properties": [
                        {
                            "propertyUrn": property_urn,
                            "values": [{"string": value}],
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
        {
            "id": "identity-name-qualified-parity",
            "group": "keyword-probe",
            "query": "identityprobe parity",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [
                {"urn": identity_name_only, "relevance": 1},
                {"urn": identity_name_and_qualified, "relevance": 1},
            ],
            "containsInTop": {
                "n": 2,
                "values": [identity_name_only, identity_name_and_qualified],
            },
            "scoreRatioAtMost": {
                "numerator": identity_name_and_qualified,
                "denominator": identity_name_only,
                "max": 1.02,
            },
        },
        {
            "id": "identity-bounded-qualified-path",
            "group": "keyword-probe",
            "query": "analytics.identityprobe_ledger",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": identity_path, "relevance": 1}],
            "rankAbove": {
                "urn": identity_path,
                "above": [identity_path_partial],
            },
        },
        {
            "id": "identity-earlier-qualified-segment",
            "group": "keyword-probe",
            "query": "identityprobe.ancestry",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": identity_earlier_segment, "relevance": 1}],
            "exactTopHit": identity_earlier_segment,
        },
        {
            "id": "structured-property-string-opt-in",
            "group": "structured-property",
            "query": "packstringoptin",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": STRING_OPT_IN_DATASET, "relevance": 1}],
            "exactTopHit": STRING_OPT_IN_DATASET,
        },
        {
            "id": "structured-property-urn-opt-in",
            "group": "structured-property",
            "query": "packurnoptin",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": URN_OPT_IN_DATASET, "relevance": 1}],
            "exactTopHit": URN_OPT_IN_DATASET,
        },
        {
            "id": "structured-property-urn-opt-in-identity",
            "group": "structured-property",
            "query": "urn:li:dataset:(urn:li:dataPlatform:catalogprobe,packurnoptin,PROD)",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": URN_OPT_IN_DATASET, "relevance": 1}],
            "exactTopHit": URN_OPT_IN_DATASET,
        },
        {
            "id": "structured-property-string-opt-out-fulltext",
            "group": "structured-property",
            "query": "packstringoptout",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [],
            "expectNoHits": True,
        },
        {
            "id": "structured-property-urn-opt-out-fulltext",
            "group": "structured-property",
            "query": "packurnoptout",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [],
            "expectNoHits": True,
        },
        {
            "id": "structured-property-string-opt-out-filter",
            "group": "structured-property",
            "query": "*",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": string_opt_out, "relevance": 1}],
            "filter": {
                "field": "structuredProperties.packStringFilter",
                "condition": "EQUAL",
                "value": "packstringoptout",
            },
            "exactTopHit": string_opt_out,
        },
        {
            "id": "structured-property-urn-opt-in-filter",
            "group": "structured-property",
            "query": "*",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": URN_OPT_IN_DATASET, "relevance": 1}],
            "filter": {
                "field": "structuredProperties.packUrnSearch",
                "condition": "EQUAL",
                "value": "urn:li:dataset:(urn:li:dataPlatform:catalogprobe,packurnoptin,PROD)",
            },
            "exactTopHit": URN_OPT_IN_DATASET,
        },
        {
            "id": "structured-property-urn-opt-out-filter",
            "group": "structured-property",
            "query": "*",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": urn_opt_out, "relevance": 1}],
            "filter": {
                "field": "structuredProperties.packUrnFilter",
                "condition": "EQUAL",
                "value": "urn:li:corpuser:packurnoptout",
            },
            "exactTopHit": urn_opt_out,
        },
        {
            "id": "structured-property-boosted-ranking",
            "group": "structured-property",
            "query": "packboostalpha packboostbravo",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [{"urn": BOOSTED_PROPERTY_DATASET, "relevance": 1}],
            "exactTopHit": BOOSTED_PROPERTY_DATASET,
        },
        fulltext_property_probe(
            "structured-property-fulltext-token", "zephyrical", fulltext
        ),
        fulltext_property_probe(
            "structured-property-fulltext-stem", "plinths", fulltext
        ),
        fulltext_property_probe(
            "structured-property-fulltext-phrase", '"zephyrical quorvane"', fulltext
        ),
        fulltext_property_probe(
            "structured-property-fulltext-case",
            "zephyrical quorvane plinth",
            fulltext,
        ),
    ]
    proposals.extend(sort_probe_entities())
    judgments.extend(sort_judgments())
    proposals.extend(field_probe_entities())
    judgments.extend(field_probe_judgments())
    judgments.extend(autocomplete_judgments())
    return proposals, judgments


def sort_probe_entities() -> list[dict[str, Any]]:
    """Closed set for caller-sort checks. The marker keeps Cranfield out of the order."""
    alpha = dataset_urn("catalogprobe", "sort_alpha")
    bravo = dataset_urn("catalogprobe", "sort_bravo")
    bravo_tie = dataset_urn("catalogprobe", "sort_bravo_tie")
    charlie = dataset_urn("catalogprobe", "sort_charlie")
    chart = "urn:li:chart:(catalogprobe,sort_unmapped)"
    # Same name on bravo and bravo_tie. ESUtils appends urn ascending, so the tie breaks on the URN.
    # Charlie's description repeats the query so a text match ranks it first; name order still starts at alpha.
    rows = [
        (alpha, "Sort Alpha", "Sort probe member."),
        (bravo, "Sort Bravo", "Sort probe member."),
        (bravo_tie, "Sort Bravo", "Sort probe member."),
        (charlie, "Sort Charlie", "Sort probe member sort sort sort."),
    ]
    proposals = [
        mcp(
            urn,
            "datasetProperties",
            {
                "name": name,
                "qualifiedName": f"catalogprobe.{urn.split(',')[1]}",
                "description": description,
            },
        )
        for urn, name, description in rows
    ]
    counts = {alpha: 1.0, bravo: 2.0, bravo_tie: 2.0}
    for urn in (alpha, bravo, bravo_tie, charlie):
        properties = [sort_property(SORT_SET_PROPERTY, {"string": SORT_SET_VALUE})]
        if urn in counts:
            properties.append(
                sort_property(SORT_COUNT_PROPERTY, {"double": counts[urn]})
            )
        proposals.append(mcp(urn, "structuredProperties", {"properties": properties}))
    proposals.append(
        mcp(
            chart,
            "chartInfo",
            {
                "title": "Sort Unmapped",
                "description": "Chart in the sort probe set with no numeric property.",
                "lastModified": change_stamps(),
            },
        )
    )
    proposals.append(
        mcp(
            chart,
            "structuredProperties",
            {
                "properties": [
                    sort_property(SORT_SET_PROPERTY, {"string": SORT_SET_VALUE})
                ]
            },
        )
    )
    return proposals


def sort_property(property_urn: str, value: dict[str, Any]) -> dict[str, Any]:
    return {
        "propertyUrn": property_urn,
        "values": [value],
        "created": stamp(),
        "lastModified": stamp(),
    }


def sort_judgments() -> list[dict[str, Any]]:
    alpha = dataset_urn("catalogprobe", "sort_alpha")
    bravo = dataset_urn("catalogprobe", "sort_bravo")
    bravo_tie = dataset_urn("catalogprobe", "sort_bravo_tie")
    charlie = dataset_urn("catalogprobe", "sort_charlie")
    chart = "urn:li:chart:(catalogprobe,sort_unmapped)"
    # Comma sorts before underscore, so sort_bravo precedes sort_bravo_tie.
    # Ascending name uses the least value. Both Bravo rows share "Sort Bravo", which
    # sorts before the dataset key, so the tie breaks on the URN.
    # Descending name uses the greatest value. The key is also labeled name, and
    # sort_bravo_tie sorts after sort_bravo, so that row comes first.
    name_ascending = [alpha, bravo, bravo_tie, charlie]
    name_descending = [charlie, bravo_tie, bravo, alpha]
    # The dataset with no number is last in both directions. Engine default is _last.
    count_ascending = [alpha, bravo, bravo_tie, charlie]
    count_descending = [bravo, bravo_tie, alpha, charlie]
    # Chart index does not map the dataset-only number. It sorts with the missing row.
    unmapped_ascending = [alpha, bravo, bravo_tie, chart, charlie]
    marker = {
        "field": "structuredProperties.sortProbeSet",
        "condition": "EQUAL",
        "value": SORT_SET_VALUE,
    }
    specs = [
        (
            "sort-name-ascending",
            "sort",
            ["dataset"],
            "name",
            "ASCENDING",
            name_ascending,
        ),
        (
            "sort-name-descending",
            "*",
            ["dataset"],
            "name",
            "DESCENDING",
            name_descending,
        ),
        (
            "sort-count-ascending",
            "*",
            ["dataset"],
            "structuredProperties.sortProbeCount",
            "ASCENDING",
            count_ascending,
        ),
        (
            "sort-count-descending",
            "*",
            ["dataset"],
            "structuredProperties.sortProbeCount",
            "DESCENDING",
            count_descending,
        ),
        (
            "sort-count-unmapped-chart",
            "*",
            ["dataset", "chart"],
            "structuredProperties.sortProbeCount",
            "ASCENDING",
            unmapped_ascending,
        ),
    ]
    judgments = []
    for query_id, query, entity_types, field, order, expected in specs:
        judgments.append(
            {
                "id": query_id,
                "group": "sort",
                "query": query,
                "surfaces": ["keyword"],
                "entityTypes": entity_types,
                "grades": [],
                "filter": marker,
                "sort": {"field": field, "order": order},
                "expectedOrder": expected,
            }
        )
    return judgments


def field_probe_entities() -> list[dict[str, Any]]:
    """One dataset per field. The token is not the dataset name or description."""
    column = dataset_urn("catalogprobe", "fieldprobe_column")
    described = dataset_urn("catalogprobe", "fieldprobe_description")
    tagged = dataset_urn("catalogprobe", "fieldprobe_tag")
    termed = dataset_urn("catalogprobe", "fieldprobe_term")
    contained = dataset_urn("catalogprobe", "fieldprobe_other")
    stemmed = dataset_urn("catalogprobe", "fieldprobe_stem")
    hosts = [
        (column, "Fieldprobe Column Host"),
        (described, "Fieldprobe Description Host"),
        (tagged, "Fieldprobe Tag Host"),
        (termed, "Fieldprobe Term Host"),
        (contained, "Fieldprobe Other Host"),
        (stemmed, "Fieldprobe Stem Host"),
    ]
    proposals = [
        mcp(
            urn,
            "datasetProperties",
            {
                "name": name,
                "qualifiedName": f"catalogprobe.{urn.split(',')[1]}",
                "description": "Field probe host.",
            },
        )
        for urn, name in hosts
    ]
    proposals.append(
        mcp(
            column,
            "schemaMetadata",
            schema_metadata([schema_field(FIELD_PROBE_COLUMN)]),
        )
    )
    proposals.append(
        mcp(
            described,
            "schemaMetadata",
            schema_metadata(
                [schema_field("col_a", description=FIELD_PROBE_DESCRIPTION)]
            ),
        )
    )
    proposals.append(
        mcp(tagged, "globalTags", {"tags": [{"tag": f"urn:li:tag:{FIELD_PROBE_TAG}"}]})
    )
    proposals.append(
        mcp(
            termed,
            "glossaryTerms",
            {
                "auditStamp": stamp(),
                "terms": [{"urn": f"urn:li:glossaryTerm:{FIELD_PROBE_TERM}"}],
            },
        )
    )
    proposals.append(
        mcp(
            contained,
            "container",
            {"container": f"urn:li:container:{FIELD_PROBE_OTHER}"},
        )
    )
    proposals.append(
        mcp(stemmed, "container", {"container": f"urn:li:container:{FIELD_PROBE_STEM}"})
    )
    return proposals


def schema_metadata(fields: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schemaName": "fieldprobe",
        "platform": PROBE_PLATFORM,
        "version": 0,
        "hash": "fieldprobe",
        "platformSchema": {"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
        "fields": fields,
        "created": stamp(),
        "lastModified": stamp(),
    }


def schema_field(path: str, description: str | None = None) -> dict[str, Any]:
    field: dict[str, Any] = {
        "fieldPath": path,
        "nativeDataType": "string",
        "nullable": False,
        "recursive": False,
        "type": {"type": {"com.linkedin.schema.StringType": {}}},
    }
    if description is not None:
        field["description"] = description
    return field


def field_probe_judgments() -> list[dict[str, Any]]:
    """Pass or miss is locked to a measured run. Degree fields are not queried."""
    column = dataset_urn("catalogprobe", "fieldprobe_column")
    described = dataset_urn("catalogprobe", "fieldprobe_description")
    tagged = dataset_urn("catalogprobe", "fieldprobe_tag")
    termed = dataset_urn("catalogprobe", "fieldprobe_term")
    contained = dataset_urn("catalogprobe", "fieldprobe_other")
    stemmed = dataset_urn("catalogprobe", "fieldprobe_stem")
    specs: list[tuple[str, str, str, str | None]] = [
        ("field-probe-name-1", FIELD_PROBE_COLUMN, column, None),
        ("field-probe-description-1", FIELD_PROBE_DESCRIPTION, described, None),
        ("field-probe-tag", FIELD_PROBE_TAG, tagged, "tags"),
        ("field-probe-glossary", FIELD_PROBE_TERM, termed, "glossaryTerms"),
        ("field-probe-other", FIELD_PROBE_OTHER, contained, "other"),
        ("field-probe-stemmed", FIELD_PROBE_STEM_QUERY, stemmed, "other.stemmed"),
    ]
    judgments = []
    for query_id, query, urn, matched in specs:
        row: dict[str, Any] = {
            "id": query_id,
            "group": "field-probe",
            "query": query,
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [],
        }
        if matched is None:
            row["expectNoHits"] = True
        else:
            row["exactTopHit"] = urn
            row["matchedFieldContains"] = {"value": matched, "hits": 1}
        judgments.append(row)
    return judgments


def autocomplete_judgments() -> list[dict[str, Any]]:
    """Pass/fail prefixes. These stay out of both nDCG floors."""
    return [
        {
            "id": "autocomplete-dataset-two-char",
            "group": "autocomplete",
            "query": "zz",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [],
            "expectedUrns": [AUTOCOMPLETE_DATASET],
            "expectedSuggestion": AUTOCOMPLETE_NAME,
        },
        {
            "id": "autocomplete-corpuser-one-char",
            "group": "autocomplete",
            "query": "r",
            "surfaces": ["keyword"],
            "entityTypes": ["corpuser"],
            "grades": [],
            "expectedUrns": [
                "urn:li:corpuser:rankOwner",
                "urn:li:corpuser:rankOther",
            ],
        },
        {
            "id": "autocomplete-dataset-miss",
            "group": "autocomplete",
            "query": "qxnomatch",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "grades": [],
            "expectedUrns": [],
        },
    ]


def interactive_ranking_probes() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    users = {
        "rankOwner": "urn:li:corpuser:rankOwner",
        "rankOther": "urn:li:corpuser:rankOther",
    }
    groups = {
        "rankTeam": "urn:li:corpGroup:rankTeam",
        "rankOtherTeam": "urn:li:corpGroup:rankOtherTeam",
    }
    datasets = {
        name: dataset_urn("ranking", name)
        for name in (
            "owner_rank_owner",
            "owner_rank_other",
            "group_rank_team",
            "group_rank_other_team",
            "described",
            "bare",
            "usage_high",
            "usage_low",
        )
    }
    proposals: list[dict[str, Any]] = []

    for username, urn in users.items():
        proposals.append(
            mcp(
                urn,
                "corpUserInfo",
                {"active": True, "displayName": username},
            )
        )
    for group_name, urn in groups.items():
        proposals.append(
            mcp(
                urn,
                "corpGroupInfo",
                {
                    "displayName": group_name,
                    "admins": [],
                    "members": [],
                    "groups": [],
                },
            )
        )
    proposals.extend(
        [
            mcp(
                users["rankOwner"],
                "groupMembership",
                {"groups": [groups["rankTeam"]]},
            ),
            mcp(
                users["rankOther"],
                "groupMembership",
                {"groups": [groups["rankOtherTeam"]]},
            ),
        ]
    )

    for name, urn in datasets.items():
        properties: dict[str, Any] = {"name": name}
        if name == "described":
            properties["description"] = "A documented asset"
        proposals.append(mcp(urn, "datasetProperties", properties))

    def ownership(owner: str) -> dict[str, Any]:
        return {
            "owners": [{"owner": owner, "type": "TECHNICAL_OWNER"}],
            "lastModified": {"time": 0, "actor": users["rankOwner"]},
        }

    proposals.extend(
        [
            mcp(
                datasets["owner_rank_owner"],
                "ownership",
                ownership(users["rankOwner"]),
            ),
            mcp(
                datasets["owner_rank_other"],
                "ownership",
                ownership(users["rankOther"]),
            ),
            mcp(
                datasets["group_rank_team"],
                "ownership",
                ownership(groups["rankTeam"]),
            ),
            mcp(
                datasets["group_rank_other_team"],
                "ownership",
                ownership(groups["rankOtherTeam"]),
            ),
            mcp(
                datasets["usage_high"],
                "usageFeatures",
                {"queryCountPercentileLast30Days": 100},
            ),
            mcp(
                datasets["usage_low"],
                "usageFeatures",
                {"queryCountPercentileLast30Days": 0},
            ),
        ]
    )

    def judgment(
        judgment_id: str,
        viewer: str,
        viewer_groups: list[str],
        preferred: str,
        other: str,
    ) -> dict[str, Any]:
        preferred_urn = datasets[preferred]
        return {
            "id": judgment_id,
            "group": "keyword-probe",
            "query": "*",
            "surfaces": ["keyword"],
            "entityTypes": ["dataset"],
            "viewerUrn": viewer,
            "viewerGroups": viewer_groups,
            "candidateUrns": [preferred_urn, datasets[other]],
            "grades": [{"urn": preferred_urn, "relevance": 1}],
            "exactTopHit": preferred_urn,
        }

    judgments = [
        judgment(
            "interactive-list-personal-owner",
            users["rankOwner"],
            [groups["rankTeam"]],
            "owner_rank_owner",
            "owner_rank_other",
        ),
        judgment(
            "interactive-list-personal-owner-other-viewer",
            users["rankOther"],
            [groups["rankOtherTeam"]],
            "owner_rank_other",
            "owner_rank_owner",
        ),
        judgment(
            "interactive-list-group-owner",
            users["rankOwner"],
            [groups["rankTeam"]],
            "group_rank_team",
            "group_rank_other_team",
        ),
        judgment(
            "interactive-list-group-owner-other-viewer",
            users["rankOther"],
            [groups["rankOtherTeam"]],
            "group_rank_other_team",
            "group_rank_team",
        ),
        judgment(
            "interactive-list-catalog-completeness",
            users["rankOwner"],
            [groups["rankTeam"]],
            "described",
            "bare",
        ),
        judgment(
            "interactive-list-usage-percentile",
            users["rankOwner"],
            [groups["rankTeam"]],
            "usage_high",
            "usage_low",
        ),
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
    if query_id.startswith("longtail-") and not query_id.startswith(
        "longtail-ecommerce-"
    ):
        query_id = "longtail-ecommerce-" + query_id[len("longtail-") :]
    return {
        "id": query_id,
        "group": "longtail-ecommerce",
        "query": query,
        "surfaces": ["keyword"],
        "entityTypes": KEYWORD_ENTITY_TYPES,
        "grades": [{"urn": urn, "relevance": relevance} for urn, relevance in grades],
    }


def fulltext_property_probe(query_id: str, query: str, urn: str) -> dict[str, Any]:
    return {
        "id": query_id,
        "group": "structured-property",
        "query": query,
        "surfaces": ["keyword"],
        "entityTypes": ["dataset"],
        "grades": [{"urn": urn, "relevance": 1}],
        "exactTopHit": urn,
        "matchedFieldContains": {"value": FULLTEXT_PROBE_FIELD, "hits": 1},
    }


def probe(query_id: str, query: str, urn: str) -> dict[str, Any]:
    return {
        "id": query_id,
        "group": "keyword-probe",
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
    pet_grades = PET_PROFILE_GRADES
    # The column path is identical on all three platforms and has no field
    # description. BigQuery's missing dataset description applies only to
    # description_grades.
    column_grades = [
        (customers[0], 4),
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
        catalog_query(
            "longtail-structured-property-string",
            "packstringoptin",
            [(STRING_OPT_IN_DATASET, 4)],
        ),
        catalog_query(
            "longtail-structured-property-urn",
            "packurnoptin",
            [(URN_OPT_IN_DATASET, 4)],
        ),
        catalog_query(
            "longtail-structured-property-boost",
            "packboostalpha packboostbravo",
            [
                (BOOSTED_PROPERTY_DATASET, 4),
                (dataset_urn("catalogprobe", "boosted_description_asset"), 2),
            ],
        ),
    ]


def ranking_judgments() -> list[dict[str, Any]]:
    pet_profiles = [
        dataset_urn("dbt", "long_tail_companions.adoption.pet_profiles"),
        dataset_urn("snowflake", "long_tail_companions.adoption.pet_profiles"),
    ]
    pet_profile_tables = [
        "dbt,long_tail_companions.adoption.pet_profiles",
        "mongodb,adoption.pet_profiles",
        "snowflake,long_tail_companions.adoption.pet_profiles",
    ]
    return [
        {
            "id": "ranking-pet-profiles",
            "group": "keyword-probe",
            "query": "pet profiles",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [
                {"urn": urn, "relevance": relevance}
                for urn, relevance in [
                    *PET_PROFILE_GRADES,
                    (PET_PROFILE_DEEP_DIVE, 1),
                ]
            ],
            # The plural query puts a table named pet_profiles first. The
            # "Pet Profile" and "Pet Profile Deep Dive" dashboards match the
            # phrase too and may sit between the tables, so the three
            # exact-name tables only have to fill the top 5 with them.
            "orderedContains": ["pet_profiles"],
            "containsInTop": {"n": 5, "values": pet_profile_tables},
        },
        {
            "id": "ranking-pet-profile",
            "group": "keyword-probe",
            "query": "pet profile",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            # Looker dashboards.14 ("Pet Profile") and dashboards.30 ("Pet Profile
            # Deep Dive") can occupy slots ahead of the tables. Both pet_profiles
            # tables still have to appear in the top 5, in either order.
            "containsInTop": {"n": 5, "values": ["pet_profiles", "pet_profiles"]},
        },
        {
            "id": "ranking-return-rate",
            "group": "keyword-probe",
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
            "group": "keyword-probe",
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
            # Snowflake qualifiedName is long_tail_companions.analytics.pet_details.
            # The dbt copy's name is only pet_details and it has no qualified name,
            # which is why Snowflake is graded 2 and dbt is graded 1. The dbt and
            # Snowflake order_details siblings are the same table and can tie ahead
            # of pet_details, so this probe does not pin a first hit. The dbt
            # pet_details copy must remain in the top five. The Looker namesake
            # long_tail_companions.view.pet_details has no analytics segment and
            # is not promoted.
            "containsInTop": {
                "n": 5,
                "values": ["dbt,long_tail_companions.analytics.pet_details"],
            },
        },
        {
            "id": "ranking-collaborative-actionitems",
            "group": "keyword-probe",
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
            "group": "keyword-probe",
            "query": "customer orders",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [],
            "orderedContains": ["customer_orders,"],
            "scoresStrictlyDescending": True,
        },
        {
            "id": "ranking-row-count",
            "group": "keyword-probe",
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
            "group": "keyword-probe",
            "query": "pet profiles",
            "surfaces": ["lineage"],
            "entityTypes": ["dataset"],
            "candidateUrns": pet_profiles,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            "orderedContains": ["pet_profiles"],
        },
        {
            "id": "ranking-pet-profiles-identifier",
            "group": "keyword-probe",
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
            "group": "keyword-probe",
            "query": "PET PROFILES",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": urn, "relevance": 1} for urn in pet_profiles],
            "orderedContains": ["pet_profiles"],
        },
        {
            "id": "ranking-pet-profiles-quoted",
            "group": "keyword-probe",
            "query": '"pet_profiles"',
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [
                {"urn": urn, "relevance": relevance}
                for urn, relevance in PET_PROFILE_GRADES
                if urn != PET_PROFILE_DASHBOARD
            ],
            # Only the three tables are named exactly pet_profiles. The Pet
            # Adoptions domain that owns them can still lead on entity weight,
            # so the tables have to fill the top 4 behind at most one other hit.
            "containsInTop": {"n": 4, "values": pet_profile_tables},
        },
        {
            "id": "ranking-pii",
            "group": "keyword-probe",
            "query": "pii",
            "surfaces": ["keyword"],
            "entityTypes": KEYWORD_ENTITY_TYPES,
            "grades": [{"urn": "urn:li:tag:pii", "relevance": 1}],
            "exactTopHit": "urn:li:tag:pii",
        },
        {
            "id": "ranking-campaign-adoptions",
            "group": "keyword-probe",
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
            "group": "keyword-probe",
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


def write_catalog(path: Path, proposals: list[dict[str, Any]]) -> None:
    # Leave record key order alone. Sorting would rewrite the committed catalog.
    path.write_text(json.dumps(proposals, separators=(",", ":")) + "\n")


def take_property_definitions(
    directory: Path, proposals: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Definitions found in a catalog shard, or the ones already moved."""
    found = [
        proposal
        for proposal in proposals
        if proposal["aspectName"] == "propertyDefinition"
    ]
    if not found:
        path = directory / "01-definitions.json"
        if path.is_file():
            found = [
                proposal
                for proposal in json.loads(path.read_text())
                if proposal["aspectName"] == "propertyDefinition"
                and proposal["entityUrn"] in IMPORTED_PROPERTY_URNS
            ]
    order = {urn: index for index, urn in enumerate(IMPORTED_PROPERTY_URNS)}
    return sorted(
        found, key=lambda proposal: order.get(proposal["entityUrn"], len(order))
    )


def without_property_definitions(
    proposals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        proposal
        for proposal in proposals
        if proposal["aspectName"] != "propertyDefinition"
    ]


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


def iter_searchable_text(payload: Any) -> Iterable[str]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in SEARCHABLE_TEXT_FIELDS and isinstance(value, str):
                yield value
            else:
                yield from iter_searchable_text(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from iter_searchable_text(item)


def assert_fulltext_probe_tokens_absent(
    loaded: dict[str, list[dict[str, Any]]],
) -> None:
    """Probe tokens must not appear in a name or description.

    Name fields outrank a structured property, so a shared word would make
    exactTopHit a name hit and the light rung would stop there.
    """
    for path, proposals in loaded.items():
        for proposal in proposals:
            payload = proposal.get("aspect", {}).get("json")
            for text in iter_searchable_text(payload):
                folded = text.casefold()
                for token in FULLTEXT_PROBE_TOKENS:
                    if token not in folded:
                        continue
                    raise AssertionError(
                        f"{token} appears in {path} {proposal.get('aspectName')} "
                        f"on {proposal.get('entityUrn')}"
                    )


def validate_judgments(judgments: dict[str, Any], seen: set[str]) -> None:
    allowed_groups = {
        "benchmark",
        "keyword-probe",
        "semantic-probe",
        "longtail-ecommerce",
        "entity-boost",
        "entity-search",
        "structured-property",
        "sort",
        "field-probe",
        "autocomplete",
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
        for urn in judgment.get("expectedOrder") or []:
            if urn not in seen:
                raise AssertionError(
                    f"Judgment {judgment['id']} expects unloaded {urn}"
                )
        assert_probe_judgment(judgment, seen)


def assert_probe_judgment(judgment: dict[str, Any], seen: set[str]) -> None:
    """Pass/fail groups stay out of the nDCG floors and name every expected URN."""
    if judgment["group"] == "sort":
        if judgment.get("grades"):
            raise AssertionError(f"{judgment['id']} must not carry nDCG grades")
        order = (judgment.get("sort") or {}).get("order")
        if order not in {"ASCENDING", "DESCENDING"}:
            raise AssertionError(f"{judgment['id']} has no sort order")
        if not judgment.get("expectedOrder"):
            raise AssertionError(f"{judgment['id']} has no expected order")
    if judgment["group"] == "autocomplete":
        if judgment.get("grades"):
            raise AssertionError(f"{judgment['id']} must not carry nDCG grades")
        expected = judgment.get("expectedUrns")
        if not isinstance(expected, list):
            raise AssertionError(f"{judgment['id']} has no expected URN list")
        for urn in expected:
            if urn not in seen:
                raise AssertionError(
                    f"Judgment {judgment['id']} expects unloaded {urn}"
                )
    if judgment["group"] == "field-probe":
        if judgment.get("grades"):
            raise AssertionError(f"{judgment['id']} must not carry nDCG grades")
        has_hit = bool(judgment.get("exactTopHit"))
        no_hits = bool(judgment.get("expectNoHits"))
        if has_hit == no_hits:
            raise AssertionError(f"{judgment['id']} needs a top hit or an empty result")


def assert_autocomplete_token_isolated(
    loaded: dict[str, list[dict[str, Any]]],
) -> None:
    """The prefix token stays off every document except its own dataset."""
    for path, proposals in loaded.items():
        for proposal in proposals:
            if proposal.get("entityUrn") == AUTOCOMPLETE_DATASET:
                continue
            payload = proposal.get("aspect", {}).get("json")
            for text in iter_searchable_text(payload):
                if AUTOCOMPLETE_NAME.casefold() not in text.casefold():
                    continue
                raise AssertionError(
                    f"{AUTOCOMPLETE_NAME} appears in {path} "
                    f"{proposal.get('aspectName')} on {proposal.get('entityUrn')}"
                )


def assert_field_probe_tokens_absent(
    loaded: dict[str, list[dict[str, Any]]],
) -> None:
    """The token stays off the dataset name and off every other shard."""
    for path, proposals in loaded.items():
        for proposal in proposals:
            urn = proposal.get("entityUrn") or ""
            aspect = proposal.get("aspectName")
            if (
                path == "05-catalog-probes.json"
                and "fieldprobe_" in urn
                and aspect
                in {"schemaMetadata", "globalTags", "glossaryTerms", "container"}
            ):
                continue
            payload = proposal.get("aspect", {}).get("json")
            for text in iter_searchable_text(payload):
                folded = text.casefold()
                for token in FIELD_PROBE_TOKENS:
                    if token not in folded:
                        continue
                    raise AssertionError(f"{token} appears in {path} {aspect} on {urn}")


def validate_pack(directory: Path) -> None:
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        _validate_pack(directory)
    finally:
        if was_enabled:
            gc.enable()


def _validate_pack(directory: Path) -> None:
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
    field_probes = [
        item for item in judgments["judgments"] if item["group"] == "field-probe"
    ]
    if len(field_probes) != 6:
        raise AssertionError(f"Expected 6 field probes, found {len(field_probes)}")
    autocomplete = [
        item for item in judgments["judgments"] if item["group"] == "autocomplete"
    ]
    if len(autocomplete) != 3:
        raise AssertionError(
            f"Expected 3 autocomplete probes, found {len(autocomplete)}"
        )
    assert_fulltext_probe_tokens_absent(loaded)
    assert_field_probe_tokens_absent(loaded)
    assert_autocomplete_token_isolated(loaded)
    assert_imported_catalog(loaded)
    assert_entity_boost_isolated(loaded)
    counts = Counter(item["group"] for item in judgments["judgments"])
    if counts["entity-boost"] != 3 or counts["entity-search"] != 2:
        raise AssertionError(
            "Expected 3 entity-boost probes and 2 entity-search probes, "
            f"found {counts['entity-boost']} and {counts['entity-search']}"
        )


def is_imported_catalog_urn(urn: str) -> bool:
    return (
        SHOWCASE_PREFIX in urn
        or urn.startswith("urn:li:document:ecomm-")
        or urn.startswith("urn:li:structuredProperty:showcase.")
        or urn in IMPORTED_CONTAINER_URNS
    )


def imported_proposals(proposals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        proposal
        for proposal in proposals
        if is_imported_catalog_urn(proposal["entityUrn"])
    ]


def assert_imported_catalog(loaded: dict[str, list[dict[str, Any]]]) -> None:
    """The ecommerce section stays in the committed catalog.

    Every property definition is in the first shard, which waits for completion
    before the business shard assigns those properties.
    """
    business = loaded[BUSINESS_SHARD]
    imported = imported_proposals(business)
    if len(imported) != IMPORTED_PROPOSAL_COUNT:
        raise AssertionError(
            f"Expected {IMPORTED_PROPOSAL_COUNT} imported catalog proposals, "
            f"found {len(imported)}"
        )
    stray = [
        (path, proposal["entityUrn"])
        for path, proposals in loaded.items()
        if path != "01-definitions.json"
        for proposal in proposals
        if proposal["aspectName"] == "propertyDefinition"
    ]
    if stray:
        raise AssertionError(
            f"Property definitions belong in 01-definitions.json, found {stray[:8]}"
        )
    leading = [
        proposal["entityUrn"]
        for proposal in loaded["01-definitions.json"]
        if proposal["aspectName"] == "propertyDefinition"
        and proposal["entityUrn"].startswith("urn:li:structuredProperty:showcase.")
    ]
    if leading != list(IMPORTED_PROPERTY_URNS):
        raise AssertionError(
            "Imported property definitions must be in 01-definitions.json, "
            f"found {leading}"
        )


def assert_entity_boost_isolated(loaded: dict[str, list[dict[str, Any]]]) -> None:
    for path, proposals in loaded.items():
        for proposal in proposals:
            if "entityboost" in proposal["entityUrn"]:
                continue
            payload = proposal.get("aspect", {}).get("json")
            for text in iter_searchable_text(payload):
                if "entityboost" in text.casefold():
                    raise AssertionError(
                        f"entityboost appears in {path} {proposal.get('aspectName')} "
                        f"on {proposal.get('entityUrn')}"
                    )


def unique_display(
    proposals: list[dict[str, Any]], entity_type: str, aspect: str, field: str
) -> tuple[str, str]:
    names: dict[str, set[str]] = {}
    for proposal in proposals:
        if proposal["entityType"] != entity_type or proposal["aspectName"] != aspect:
            continue
        name = proposal["aspect"]["json"].get(field)
        if isinstance(name, str) and name.strip():
            names.setdefault(name, set()).add(proposal["entityUrn"])
    for name, urns in names.items():
        if len(urns) == 1:
            return name, next(iter(urns))
    raise AssertionError(f"No unique {entity_type} {field}")


def entity_weight_probes(
    ecommerce: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    assets = [
        proposal["entityUrn"]
        for proposal in ecommerce
        if proposal["entityType"] == "dataset"
        and proposal["aspectName"] == "datasetProperties"
    ]
    if not assets:
        raise AssertionError("Imported catalog has no dataset to place in the product")
    mcps = [
        mcp(
            ENTITY_BOOST_NODE,
            "glossaryNodeInfo",
            {"name": ENTITY_BOOST_NAME, "definition": ENTITY_BOOST_NAME},
        ),
        mcp(
            ENTITY_BOOST_TERM,
            "glossaryTermInfo",
            {
                "name": ENTITY_BOOST_NAME,
                "definition": ENTITY_BOOST_NAME,
                "termSource": "INTERNAL",
                "parentNode": ENTITY_BOOST_NODE,
            },
        ),
        mcp(
            ENTITY_BOOST_DOMAIN,
            "domainProperties",
            {"name": ENTITY_BOOST_NAME, "description": ENTITY_BOOST_NAME},
        ),
        mcp(
            ENTITY_BOOST_PRODUCT,
            "dataProductProperties",
            {
                "name": ENTITY_BOOST_NAME,
                "description": ENTITY_BOOST_NAME,
                "assets": [{"destinationUrn": assets[0], "outputPort": False}],
            },
        ),
        mcp(ENTITY_BOOST_PRODUCT, "domains", {"domains": [ENTITY_BOOST_DOMAIN]}),
        mcp(
            ENTITY_BOOST_DATASET,
            "datasetProperties",
            {"name": ENTITY_BOOST_NAME, "description": ENTITY_BOOST_NAME},
        ),
    ]
    judgments: list[dict[str, Any]] = [
        {
            "id": "entity-boost-glossary-term",
            "group": "entity-boost",
            "query": ENTITY_BOOST_NAME,
            "entityTypes": ["glossaryTerm", "dataProduct", "dataset"],
            "surfaces": ["keyword"],
            "orderedContains": [
                "dataProduct:entityboost",
            ],
            # Entity-type weights multiply each entity's lexical score. The data product
            # weight is above the glossary term, so an equal name and description ranks
            # the data product first. The dataset keeps the same name and stays in the
            # top three.
            "containsInTop": {
                "n": 3,
                "values": [
                    "dataProduct:entityboost",
                    "catalogprobe,entityboost",
                ],
            },
            "grades": [
                {"urn": ENTITY_BOOST_TERM, "relevance": 1},
                {"urn": ENTITY_BOOST_PRODUCT, "relevance": 2},
                {"urn": ENTITY_BOOST_DATASET, "relevance": 1},
            ],
        },
        {
            "id": "entity-boost-glossary-node",
            "group": "entity-boost",
            "query": ENTITY_BOOST_NAME,
            "entityTypes": ["glossaryNode", "dataProduct"],
            "surfaces": ["keyword"],
            "orderedContains": [
                "dataProduct:entityboost",
                "glossaryNode:entityboost",
            ],
            # The data product weight is above the glossary node, so an equal name
            # ranks the data product first.
            "grades": [
                {"urn": ENTITY_BOOST_NODE, "relevance": 1},
                {"urn": ENTITY_BOOST_PRODUCT, "relevance": 2},
            ],
        },
        {
            "id": "entity-boost-domain",
            "group": "entity-boost",
            "query": ENTITY_BOOST_NAME,
            "entityTypes": ["domain", "dataProduct"],
            "surfaces": ["keyword"],
            "present": ["domain:entityboost", "dataProduct:entityboost"],
            "grades": [
                {"urn": ENTITY_BOOST_DOMAIN, "relevance": 1},
                {"urn": ENTITY_BOOST_PRODUCT, "relevance": 1},
            ],
        },
    ]
    user_name, user_urn = unique_display(
        ecommerce, "corpuser", "corpUserInfo", "displayName"
    )
    group_name, group_urn = unique_display(
        ecommerce, "corpGroup", "corpGroupInfo", "displayName"
    )
    judgments.extend(
        [
            {
                "id": "entity-search-user",
                "group": "entity-search",
                "query": user_name,
                "entityTypes": ["corpuser"],
                "surfaces": ["keyword"],
                "exactTopHit": user_urn,
                "grades": [{"urn": user_urn, "relevance": 2}],
            },
            {
                "id": "entity-search-group",
                "group": "entity-search",
                "query": group_name,
                "entityTypes": ["corpGroup"],
                "surfaces": ["keyword"],
                "exactTopHit": group_urn,
                "grades": [{"urn": group_urn, "relevance": 2}],
            },
        ]
    )
    return mcps, judgments


def generate(root: Path | None = None) -> Path:
    gc.disable()
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

    timeseries = directory / TIMESERIES_SHARD
    if not timeseries.is_file():
        raise AssertionError(
            f"{TIMESERIES_SHARD} is the longtail-ecommerce profile shard. "
            "It is not rebuilt from an Elasticsearch snapshot."
        )
    business_path = directory / BUSINESS_SHARD
    if not business_path.is_file():
        raise AssertionError(
            f"{BUSINESS_SHARD} is the longtail-ecommerce catalog. "
            "It is not rebuilt from the ecommerce snapshot."
        )
    business = json.loads(business_path.read_text())
    profile = json.loads(timeseries.read_text())
    property_definitions = take_property_definitions(directory, business + profile)
    if any(proposal["aspectName"] == "propertyDefinition" for proposal in business):
        business = without_property_definitions(business)
        write_catalog(business_path, business)
    if any(proposal["aspectName"] == "propertyDefinition" for proposal in profile):
        write_catalog(timeseries, without_property_definitions(profile))
        profile = without_property_definitions(profile)
    imported = imported_proposals(business)

    platforms = {"cranfield", "catalogprobe", "ranking"}
    for proposal in business + profile:
        platform = platform_name(proposal["entityUrn"])
        if platform:
            platforms.add(platform)

    probe_mcps, probe_judgments = catalog_probes()
    ranking_mcps, interactive_ranking_judgments = interactive_ranking_probes()
    boost_mcps, boost_judgments = entity_weight_probes(imported)
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
        # Assignments sit later in the business shard, which is ingested
        # asynchronously. The definitions have to commit first or those writes
        # are rejected as unknown properties.
        "01-definitions.json": definitions(platforms) + property_definitions,
        "02-cranfield-datasets.json": cranfield_mcps,
        "05-catalog-probes.json": probe_mcps + ranking_mcps + boost_mcps,
        "06-semantic-probes.json": semantic_mcps,
    }
    index = {
        "version": "1",
        "files": [
            {"path": "01-definitions.json", "wait_for_completion": True},
            {"path": "02-cranfield-datasets.json"},
            {"path": BUSINESS_SHARD},
            {"path": TIMESERIES_SHARD},
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
                # 0.75 fails when one currently perfect catalog query drops out of
                # the top 10. 0.76 is tight against queries already in the 0.61-0.82 band.
                "longtailEcommerceNdcgAt10": 0.75,
                "javaBuilderLongtailEcommerceNdcgAt10": 0.72,
            },
            "opensearch": {
                "benchmarkNdcgAt10": 0.31,
                "javaBuilderBenchmarkNdcgAt10": 0.20,
                "longtailEcommerceNdcgAt10": 0.75,
                "javaBuilderLongtailEcommerceNdcgAt10": 0.72,
            },
        },
        "judgments": benchmark
        + longtail_judgments()
        + ranking_judgments()
        + interactive_ranking_judgments
        + probe_judgments
        + boost_judgments
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
    validate_pack(directory)
    return directory


if __name__ == "__main__":
    generate()
