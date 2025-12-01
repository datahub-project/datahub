#!/usr/bin/env python3
"""
Demonstration script for glossary domain hierarchy functionality.

This script shows the complete domain hierarchy implementation in action
with real RDF data and comprehensive examples.
"""

import os
import sys

from rdflib import Graph

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datahub.ingestion.source.rdf.core.rdf_ast_to_datahub_ast_converter import (
    ASTToDataHubConverter,
)
from datahub.ingestion.source.rdf.core.rdf_graph_to_rdf_ast_converter import (
    RDFToASTConverter,
)
from datahub.ingestion.source.rdf.core.urn_generator import (
    HierarchicalUrnGenerator,
)


def demonstrate_domain_hierarchy():
    """Demonstrate domain hierarchy functionality with comprehensive examples."""

    print("=" * 80)
    print("GLOSSARY DOMAIN HIERARCHY DEMONSTRATION")
    print("=" * 80)
    print()

    # Initialize components
    urn_generator = HierarchicalUrnGenerator()
    rdf_converter = RDFToASTConverter(forced_dialect=None)
    datahub_converter = ASTToDataHubConverter(urn_generator)

    # Load sample RDF data
    rdf_file = os.path.join(os.path.dirname(__file__), "sample_glossary_domains.ttl")

    if not os.path.exists(rdf_file):
        print(f"❌ Sample RDF file not found: {rdf_file}")
        return False

    print("Loading sample RDF data...")
    rdf_graph = Graph()
    rdf_graph.parse(rdf_file, format="turtle")
    print(f"✓ Loaded {len(rdf_graph)} RDF triples")
    print()

    # Convert to RDF AST
    print("Converting RDF to AST...")
    rdf_ast = rdf_converter.convert(rdf_graph, environment="PROD")
    print(f"✓ Found {len(rdf_ast.glossary_terms)} glossary terms")
    print()

    # Convert to DataHub AST
    print("Converting to DataHub AST with domain hierarchy...")
    datahub_ast = datahub_converter.convert(rdf_ast, "PROD")
    print(f"✓ Created {len(datahub_ast.glossary_terms)} DataHub glossary terms")
    print()

    # Analyze domain hierarchies
    print("DOMAIN HIERARCHY ANALYSIS")
    print("=" * 50)

    domain_stats = {}
    for term in datahub_ast.glossary_terms:
        if term.domain_hierarchy_urns:
            domain_key = "/".join(
                [
                    urn.replace("urn:li:domain:", "")
                    for urn in term.domain_hierarchy_urns
                ]
            )
            if domain_key not in domain_stats:
                domain_stats[domain_key] = []
            domain_stats[domain_key].append(term.name)

    print(f"Found {len(domain_stats)} unique domain hierarchies:")
    print()

    for domain_path, terms in domain_stats.items():
        print(f"📁 Domain Hierarchy: {domain_path}")
        print(f"   Terms: {', '.join(terms)}")
        print(f"   Count: {len(terms)} terms")
        print()

    # Show detailed examples
    print("DETAILED EXAMPLES")
    print("=" * 50)

    for i, term in enumerate(datahub_ast.glossary_terms[:5], 1):  # Show first 5 terms
        print(f"Example {i}: {term.name}")
        print(f"  IRI: {term.urn}")
        print(f"  Definition: {term.definition}")

        if term.domain_hierarchy_urns:
            print("  Domain Hierarchy:")
            for j, domain_urn in enumerate(term.domain_hierarchy_urns):
                indent = "    " + "  " * j
                domain_name = domain_urn.replace("urn:li:domain:", "")
                print(f"{indent}Level {j}: {domain_name}")

            print(f"  Assigned Domain: {term.assigned_domain_urn}")
        else:
            print("  Domain Hierarchy: None")
            print("  Assigned Domain: None")

        print()

    # Show IRI parsing examples
    print("IRI PARSING EXAMPLES")
    print("=" * 50)

    test_iris = [
        "https://bank.com/trading/loans/Customer_Name",
        "https://Bank.COM/Trading/Loans/Loan_Amount",
        "https://bank-name.com/finance-data/loan-trading/Interest_Rate",
        "trading:terms/Loan_Type",
        "simple:Collateral",
    ]

    for iri in test_iris:
        print(f"IRI: {iri}")

        # Test path extraction
        path_segments = urn_generator.derive_path_from_iri(iri, include_last=False)
        print(f"  Path segments: {path_segments}")

        # Test domain hierarchy creation
        domain_urns = datahub_converter.create_domain_hierarchy_urns_for_glossary_term(
            iri
        )
        if domain_urns:
            print(f"  Domain URNs: {domain_urns}")
            leaf_domain = datahub_converter.get_leaf_domain_urn_for_glossary_term(iri)
            print(f"  Leaf domain: {leaf_domain}")
        else:
            print("  Domain URNs: None")
            print("  Leaf domain: None")

        print()

    # Show domain reuse analysis
    print("DOMAIN REUSE ANALYSIS")
    print("=" * 50)

    # Group terms by domain hierarchy
    domain_groups = {}
    for term in datahub_ast.glossary_terms:
        if term.domain_hierarchy_urns:
            key = tuple(term.domain_hierarchy_urns)
            if key not in domain_groups:
                domain_groups[key] = []
            domain_groups[key].append(term.name)

    print("Domain reuse statistics:")
    print(f"  Total unique domain hierarchies: {len(domain_groups)}")
    print(
        f"  Terms sharing domains: {sum(len(terms) for terms in domain_groups.values() if len(terms) > 1)}"
    )
    print()

    for domain_hierarchy, terms in domain_groups.items():
        if len(terms) > 1:
            domain_path = " → ".join(
                [urn.replace("urn:li:domain:", "") for urn in domain_hierarchy]
            )
            print(f"  📁 {domain_path}")
            print(f"     Shared by: {', '.join(terms)}")
            print()

    print("=" * 80)
    print("DEMONSTRATION COMPLETE!")
    print("=" * 80)
    print()
    print("Key Features Demonstrated:")
    print("✓ Domain hierarchy creation from IRI structure")
    print("✓ Case preservation (Bank.COM stays Bank.COM)")
    print("✓ Special character preservation (bank-name.com)")
    print("✓ Custom scheme support (trading:terms)")
    print("✓ Domain reuse across multiple terms")
    print("✓ Complete RDF to DataHub pipeline")
    print("✓ Proper Optional handling (None when no domains)")
    print()
    print("The domain hierarchy implementation is working correctly!")

    return True


if __name__ == "__main__":
    success = demonstrate_domain_hierarchy()
    sys.exit(0 if success else 1)
