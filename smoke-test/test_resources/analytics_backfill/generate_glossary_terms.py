#!/usr/bin/env python3
"""
Generate glossary terms and attach them to datasets.

This script creates business glossary terms and assigns them to existing datasets
to populate the "Data Assets by Term" chart in analytics.
"""

import argparse
import json
import logging
import random
from typing import Dict, List

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Business glossary terms organized by category
GLOSSARY_TERMS = {
    "Customer Data": {
        "terms": [
            ("Customer ID", "Unique identifier for a customer"),
            (
                "Customer Lifetime Value",
                "Total revenue expected from a customer over their lifetime",
            ),
            (
                "Customer Segment",
                "Classification of customers based on behavior or demographics",
            ),
            ("Churn Rate", "Percentage of customers who stop using the service"),
        ]
    },
    "Financial Metrics": {
        "terms": [
            ("Revenue", "Total income generated from business operations"),
            (
                "Annual Recurring Revenue",
                "Predictable revenue stream from subscriptions",
            ),
            ("Gross Margin", "Revenue minus cost of goods sold"),
            ("Operating Expenses", "Costs incurred during normal business operations"),
        ]
    },
    "Product Metrics": {
        "terms": [
            ("Daily Active Users", "Number of unique users active on a given day"),
            ("Monthly Active Users", "Number of unique users active in a month"),
            ("User Engagement", "Measure of user interaction with the product"),
            ("Feature Adoption", "Rate at which users adopt new features"),
        ]
    },
    "Sales & Marketing": {
        "terms": [
            ("Lead", "Potential customer who has shown interest"),
            ("Conversion Rate", "Percentage of leads that become customers"),
            ("Customer Acquisition Cost", "Cost to acquire a new customer"),
            ("Marketing Qualified Lead", "Lead deemed ready for sales engagement"),
        ]
    },
    "Operations": {
        "terms": [
            (
                "Service Level Agreement",
                "Commitment between service provider and customer",
            ),
            ("Incident", "Unplanned interruption or reduction in service quality"),
            ("Mean Time To Resolution", "Average time to resolve an incident"),
            ("Uptime", "Percentage of time a system is operational"),
        ]
    },
}


def create_glossary_term(
    term_name: str,
    definition: str,
    category: str,
    actor_urn: str = "urn:li:corpuser:datahub",
) -> List[MetadataChangeProposalWrapper]:
    """Create a glossary term with its metadata."""
    # Create a URL-friendly term ID
    term_id = term_name.lower().replace(" ", "_")
    term_urn = make_term_urn(term_id)

    mcps = []

    # GlossaryTermInfo aspect
    term_info = GlossaryTermInfoClass(
        definition=definition,
        name=term_name,
        termSource="INTERNAL",
        sourceRef=category,
        sourceUrl="",
        customProperties={
            "category": category,
            "created_by": "analytics_backfill",
        },
    )

    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=term_urn,
            aspect=term_info,
        )
    )

    return mcps, term_urn


def attach_terms_to_dataset(
    dataset_urn: str, term_urns: List[str], actor_urn: str = "urn:li:corpuser:datahub"
) -> MetadataChangeProposalWrapper:
    """Attach glossary terms to a dataset."""
    glossary_terms = GlossaryTermsClass(
        terms=[
            AuditStampClass(
                time=0,
                actor=actor_urn,
            )
            for _ in term_urns
        ],
        auditStamp=AuditStampClass(
            time=0,
            actor=actor_urn,
        ),
    )

    # Create GlossaryTerms aspect with term associations
    from datahub.metadata.schema_classes import GlossaryTermAssociationClass

    glossary_terms = GlossaryTermsClass(
        terms=[
            GlossaryTermAssociationClass(
                urn=term_urn,
                context="Dataset contains data related to this term",
            )
            for term_urn in term_urns
        ],
        auditStamp=AuditStampClass(
            time=0,
            actor=actor_urn,
        ),
    )

    return MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=glossary_terms,
    )


def generate_and_emit_glossary_terms(
    gms_url: str,
    token: str,
    entity_urns_file: str = None,
    output_file: str = None,
) -> Dict:
    """Generate glossary terms and attach them to datasets."""
    logger.info("Generating glossary terms...")

    emitter = DataHubRestEmitter(gms_server=gms_url, token=token)
    created_terms = []
    term_urn_map = {}

    # Create all glossary terms
    for category, data in GLOSSARY_TERMS.items():
        logger.info(f"Creating terms for category: {category}")
        for term_name, definition in data["terms"]:
            mcps, term_urn = create_glossary_term(term_name, definition, category)
            for mcp in mcps:
                emitter.emit_mcp(mcp)

            created_terms.append(
                {
                    "name": term_name,
                    "urn": term_urn,
                    "category": category,
                    "definition": definition,
                }
            )
            term_urn_map[category] = term_urn_map.get(category, []) + [term_urn]

    logger.info(f"Created {len(created_terms)} glossary terms")

    # Load entity URNs (datasets) if provided
    dataset_urns = []
    if entity_urns_file:
        try:
            with open(entity_urns_file, "r") as f:
                all_urns = json.load(f)
                # Filter to only datasets
                dataset_urns = [urn for urn in all_urns if ":dataset:" in urn]
            logger.info(
                f"Loaded {len(dataset_urns)} dataset URNs from {entity_urns_file}"
            )
        except FileNotFoundError:
            logger.warning(f"Entity URNs file not found: {entity_urns_file}")

    # Attach terms to datasets
    attachments = []
    if dataset_urns:
        logger.info("Attaching glossary terms to datasets...")

        # Distribute terms across datasets
        # Some datasets get multiple terms, some get one, some get none
        for dataset_urn in dataset_urns:
            # 70% of datasets get terms
            if random.random() < 0.7:
                # Randomly select 1-3 categories
                num_categories = random.randint(1, min(3, len(term_urn_map)))
                selected_categories = random.sample(
                    list(term_urn_map.keys()), num_categories
                )

                # Pick one random term from each selected category
                selected_term_urns = []
                for category in selected_categories:
                    selected_term_urns.append(random.choice(term_urn_map[category]))

                # Attach terms to dataset
                mcp = attach_terms_to_dataset(dataset_urn, selected_term_urns)
                emitter.emit_mcp(mcp)

                attachments.append(
                    {
                        "dataset_urn": dataset_urn,
                        "term_urns": selected_term_urns,
                        "num_terms": len(selected_term_urns),
                    }
                )

        logger.info(f"Attached terms to {len(attachments)} datasets")

    # Save results if requested
    result = {"terms": created_terms, "attachments": attachments}

    if output_file:
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)
        logger.info(f"Saved glossary term info to {output_file}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Generate glossary terms and attach them to datasets"
    )
    parser.add_argument(
        "--gms-url",
        default="http://localhost:8080",
        help="DataHub GMS URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--token",
        required=True,
        help="DataHub authentication token",
    )
    parser.add_argument(
        "--entity-urns-file",
        help="JSON file containing entity URNs to attach terms to",
    )
    parser.add_argument(
        "--output-file",
        help="Optional: Save glossary term info to JSON file",
    )

    args = parser.parse_args()

    result = generate_and_emit_glossary_terms(
        gms_url=args.gms_url,
        token=args.token,
        entity_urns_file=args.entity_urns_file,
        output_file=args.output_file,
    )

    logger.info(f"✅ Created {len(result['terms'])} glossary terms")
    logger.info(f"✅ Attached terms to {len(result['attachments'])} datasets")

    # Show some examples
    if result["terms"]:
        logger.info("\nSample glossary terms:")
        for term in result["terms"][:5]:
            logger.info(f"  - {term['name']} ({term['category']})")
        if len(result["terms"]) > 5:
            logger.info(f"  ... and {len(result['terms']) - 5} more")

    if result["attachments"]:
        logger.info("\nSample term attachments:")
        for att in result["attachments"][:3]:
            logger.info(f"  - Dataset with {att['num_terms']} term(s)")


if __name__ == "__main__":
    main()
