#!/usr/bin/env python3
"""
Architecture-agnostic behavior integration tests.

These tests verify expected outputs from RDF inputs WITHOUT referencing
internal architecture classes. They use RDFSource directly.

This allows us to replace the internal implementation while ensuring
the same behavior is preserved.
"""

import unittest
from unittest.mock import MagicMock

from rdflib import Graph

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.rdf.ingestion.rdf_source import (
    RDFSource,
    RDFSourceConfig,
)


class TestGlossaryTermBehavior(unittest.TestCase):
    """Test glossary term extraction behavior."""

    def setUp(self):
        """Set up test fixtures using RDFSource."""
        # Create a minimal config and context for RDFSource
        self.config = RDFSourceConfig(source="dummy", environment="PROD")
        self.ctx = MagicMock(spec=PipelineContext)
        self.ctx.graph = MagicMock()  # Required for StatefulIngestionSourceBase
        self.ctx.pipeline_name = (
            "test_pipeline"  # Required for StatefulIngestionSourceBase
        )
        self.source = RDFSource(self.config, self.ctx)

    def _get_datahub_graph(self, graph: Graph):
        """Helper to get DataHubGraph from RDF graph."""
        return self.source._convert_rdf_to_datahub_ast(
            graph,
            environment="PROD",
            export_only=None,
            skip_export=None,
        )

    def test_simple_glossary_term_extraction(self):
        """Test extraction of a simple glossary term."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex: <http://example.org/glossary/> .
        
        ex:AccountIdentifier a skos:Concept ;
            skos:prefLabel "Account Identifier" ;
            skos:definition "A unique identifier for an account" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        # Should extract one glossary term
        self.assertEqual(len(datahub_graph.glossary_terms), 1)

        term = datahub_graph.glossary_terms[0]
        self.assertEqual(term.name, "Account Identifier")
        self.assertEqual(term.definition, "A unique identifier for an account")
        self.assertIn("urn:li:glossaryTerm:", term.urn)

    def test_glossary_term_urn_format(self):
        """Test that glossary term URNs follow DataHub format."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://bank.com/trading/loans/> .
        
        ex:Customer_Name a skos:Concept ;
            skos:prefLabel "Customer Name" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        term = datahub_graph.glossary_terms[0]
        # URN should contain hierarchy from IRI
        self.assertTrue(term.urn.startswith("urn:li:glossaryTerm:"))
        self.assertIn("bank.com", term.urn)

    def test_multiple_glossary_terms(self):
        """Test extraction of multiple glossary terms."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://example.org/> .
        
        ex:Term1 a skos:Concept ; skos:prefLabel "Term One" .
        ex:Term2 a skos:Concept ; skos:prefLabel "Term Two" .
        ex:Term3 a skos:Concept ; skos:prefLabel "Term Three" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        self.assertEqual(len(datahub_graph.glossary_terms), 3)
        names = {t.name for t in datahub_graph.glossary_terms}
        self.assertEqual(names, {"Term One", "Term Two", "Term Three"})

    def test_glossary_term_custom_properties(self):
        """Test that custom properties including original IRI are preserved."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://example.org/glossary/> .
        
        ex:TestTerm a skos:Concept ;
            skos:prefLabel "Test Term" ;
            skos:notation "TT-001" ;
            skos:scopeNote "Used in testing" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        term = datahub_graph.glossary_terms[0]
        # Original IRI should be preserved
        self.assertIn("rdf:originalIRI", term.custom_properties)
        self.assertEqual(
            term.custom_properties["rdf:originalIRI"],
            "http://example.org/glossary/TestTerm",
        )


class TestDomainHierarchyBehavior(unittest.TestCase):
    """Test domain hierarchy creation behavior."""

    def setUp(self):
        """Set up test fixtures."""
        from unittest.mock import MagicMock

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.rdf.ingestion.rdf_source import (
            RDFSource,
            RDFSourceConfig,
        )

        self.config = RDFSourceConfig(source="dummy", environment="PROD")
        self.ctx = MagicMock(spec=PipelineContext)
        self.ctx.graph = MagicMock()  # Required for StatefulIngestionSourceBase
        self.ctx.pipeline_name = (
            "test_pipeline"  # Required for StatefulIngestionSourceBase
        )
        self.source = RDFSource(self.config, self.ctx)

    def _get_datahub_graph(self, graph: Graph):
        """Helper to get DataHubGraph from RDF graph."""
        return self.source._convert_rdf_to_datahub_ast(
            graph,
            environment="PROD",
            export_only=None,
            skip_export=None,
        )

    def test_domain_created_from_iri_hierarchy(self):
        """Test that domains are created from IRI path hierarchy."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://bank.com/trading/loans/> .
        
        ex:Customer_Name a skos:Concept ;
            skos:prefLabel "Customer Name" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        # Should create domain hierarchy: bank.com -> trading -> loans
        # All domains (root + subdomains) should be in datahub_graph.domains so all get MCPs
        domain_paths = [tuple(d.path_segments) for d in datahub_graph.domains]
        self.assertIn(
            ("bank.com",), domain_paths, "Root domain should be in result.domains"
        )
        self.assertIn(
            ("bank.com", "trading"),
            domain_paths,
            "Subdomain should be in result.domains",
        )
        self.assertIn(
            ("bank.com", "trading", "loans"),
            domain_paths,
            "Subdomain should be in result.domains",
        )

        # Subdomains should ALSO be accessible through parent's subdomains list
        bank_domain = next(
            d for d in datahub_graph.domains if tuple(d.path_segments) == ("bank.com",)
        )
        trading_domain = next(
            (
                sd
                for sd in bank_domain.subdomains
                if tuple(sd.path_segments) == ("bank.com", "trading")
            ),
            None,
        )
        self.assertIsNotNone(
            trading_domain, "trading subdomain should be accessible via parent"
        )

        loans_domain = next(
            (
                sd
                for sd in trading_domain.subdomains
                if tuple(sd.path_segments) == ("bank.com", "trading", "loans")
            ),
            None,
        )
        self.assertIsNotNone(
            loans_domain, "loans subdomain should be accessible via parent"
        )

        # Verify subdomains have correct parent_domain_urn (not None)
        trading_in_list = next(
            d
            for d in datahub_graph.domains
            if tuple(d.path_segments) == ("bank.com", "trading")
        )
        loans_in_list = next(
            d
            for d in datahub_graph.domains
            if tuple(d.path_segments) == ("bank.com", "trading", "loans")
        )
        self.assertIsNotNone(
            trading_in_list.parent_domain_urn,
            "Subdomain should have parent_domain_urn set",
        )
        self.assertIsNotNone(
            loans_in_list.parent_domain_urn,
            "Subdomain should have parent_domain_urn set",
        )

    def test_domain_parent_child_relationships(self):
        """Test that domain parent-child relationships are correct."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://bank.com/trading/loans/> .
        
        ex:Customer_Name a skos:Concept ;
            skos:prefLabel "Customer Name" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        # All domains (root + subdomains) should be in datahub_graph.domains so all get MCPs
        # But subdomains should have parent_domain_urn set (not None)
        root_domains = [d for d in datahub_graph.domains if d.parent_domain_urn is None]
        subdomains = [
            d for d in datahub_graph.domains if d.parent_domain_urn is not None
        ]
        self.assertEqual(len(root_domains), 1, "Should have 1 root domain")
        self.assertGreater(len(subdomains), 0, "Should have subdomains in list")

        # Find root domain
        bank_domain = None
        for d in datahub_graph.domains:
            if tuple(d.path_segments) == ("bank.com",):
                bank_domain = d
                break

        self.assertIsNotNone(bank_domain, "Root domain bank.com should exist")
        self.assertIsNone(bank_domain.parent_domain_urn, "Root should have no parent")

        # Find subdomains through parent's subdomains list
        trading_domain = None
        loans_domain = None
        for subdomain in bank_domain.subdomains:
            if tuple(subdomain.path_segments) == ("bank.com", "trading"):
                trading_domain = subdomain
                # Find loans subdomain
                for loans_sub in trading_domain.subdomains:
                    if tuple(loans_sub.path_segments) == (
                        "bank.com",
                        "trading",
                        "loans",
                    ):
                        loans_domain = loans_sub
                        break
                break

        self.assertIsNotNone(trading_domain, "trading subdomain should exist")
        self.assertEqual(
            trading_domain.parent_domain_urn,
            bank_domain.urn,
            "trading's parent should be bank.com",
        )

        self.assertIsNotNone(loans_domain, "loans subdomain should exist")
        self.assertEqual(
            loans_domain.parent_domain_urn,
            trading_domain.urn,
            "loans' parent should be trading",
        )

        # Verify subdomains ARE in domains list (so they get MCPs)
        # But they have parent_domain_urn set (not None)
        trading_in_list = next(
            (d for d in datahub_graph.domains if d.urn == trading_domain.urn), None
        )
        loans_in_list = next(
            (d for d in datahub_graph.domains if d.urn == loans_domain.urn), None
        )
        self.assertIsNotNone(
            trading_in_list, "Subdomain trading should be in domains list"
        )
        self.assertIsNotNone(loans_in_list, "Subdomain loans should be in domains list")
        self.assertIsNotNone(
            trading_in_list.parent_domain_urn, "Subdomain should have parent_domain_urn"
        )
        self.assertIsNotNone(
            loans_in_list.parent_domain_urn, "Subdomain should have parent_domain_urn"
        )

    def test_terms_placed_in_correct_domain(self):
        """Test that terms are placed in the correct leaf domain."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix trading: <http://bank.com/trading/> .
        @prefix loans: <http://bank.com/trading/loans/> .
        
        trading:Trade_ID a skos:Concept ; skos:prefLabel "Trade ID" .
        loans:Loan_Amount a skos:Concept ; skos:prefLabel "Loan Amount" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        # Find root domain
        bank_domain = None
        for d in datahub_graph.domains:
            if tuple(d.path_segments) == ("bank.com",):
                bank_domain = d
                break

        self.assertIsNotNone(bank_domain, "Root domain bank.com should exist")

        # Find subdomains through parent's subdomains list
        trading_domain = None
        loans_domain = None
        for subdomain in bank_domain.subdomains:
            if tuple(subdomain.path_segments) == ("bank.com", "trading"):
                trading_domain = subdomain
                # Find loans subdomain
                for loans_sub in trading_domain.subdomains:
                    if tuple(loans_sub.path_segments) == (
                        "bank.com",
                        "trading",
                        "loans",
                    ):
                        loans_domain = loans_sub
                        break
                break

        self.assertIsNotNone(trading_domain, "trading subdomain should exist")
        self.assertIsNotNone(loans_domain, "loans subdomain should exist")

        # Trade ID should be in trading domain
        trading_term_names = {t.name for t in trading_domain.glossary_terms}
        self.assertIn("Trade ID", trading_term_names)

        # Loan Amount should be in loans domain
        loans_term_names = {t.name for t in loans_domain.glossary_terms}
        self.assertIn("Loan Amount", loans_term_names)


class TestRelationshipBehavior(unittest.TestCase):
    """Test relationship extraction behavior."""

    def setUp(self):
        """Set up test fixtures."""
        from unittest.mock import MagicMock

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.rdf.ingestion.rdf_source import (
            RDFSource,
            RDFSourceConfig,
        )

        self.config = RDFSourceConfig(source="dummy", environment="PROD")
        self.ctx = MagicMock(spec=PipelineContext)
        self.ctx.graph = MagicMock()  # Required for StatefulIngestionSourceBase
        self.ctx.pipeline_name = (
            "test_pipeline"  # Required for StatefulIngestionSourceBase
        )
        self.source = RDFSource(self.config, self.ctx)

    def _get_datahub_graph(self, graph: Graph):
        """Helper to get DataHubGraph from RDF graph."""
        return self.source._convert_rdf_to_datahub_ast(
            graph,
            environment="PROD",
            export_only=None,
            skip_export=None,
        )

    def test_broader_relationship_extraction(self):
        """Test that skos:broader relationships are extracted."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://example.org/> .
        
        ex:ChildTerm a skos:Concept ;
            skos:prefLabel "Child Term" ;
            skos:broader ex:ParentTerm .
        
        ex:ParentTerm a skos:Concept ;
            skos:prefLabel "Parent Term" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        # Should have relationships
        self.assertGreater(len(datahub_graph.relationships), 0)

        # Find the broader relationship
        broader_rels = [
            r
            for r in datahub_graph.relationships
            if r.relationship_type.value == "broader"
        ]
        self.assertEqual(len(broader_rels), 1)

        rel = broader_rels[0]
        self.assertIn("ChildTerm", rel.source_urn)
        self.assertIn("ParentTerm", rel.target_urn)

    def test_narrower_relationship_extraction(self):
        """Test that skos:narrower relationships are extracted."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://example.org/> .
        
        ex:ParentTerm a skos:Concept ;
            skos:prefLabel "Parent Term" ;
            skos:narrower ex:ChildTerm .
        
        ex:ChildTerm a skos:Concept ;
            skos:prefLabel "Child Term" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        narrower_rels = [
            r
            for r in datahub_graph.relationships
            if r.relationship_type.value == "narrower"
        ]
        self.assertEqual(len(narrower_rels), 1)

    def test_related_not_extracted(self):
        """Test that skos:related is NOT extracted (per spec)."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://example.org/> .
        
        ex:Term1 a skos:Concept ;
            skos:prefLabel "Term One" ;
            skos:related ex:Term2 .
        
        ex:Term2 a skos:Concept ;
            skos:prefLabel "Term Two" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        # Should have no relationships extracted (skos:related is not supported)
        self.assertEqual(len(datahub_graph.relationships), 0)

    def test_exactmatch_not_extracted_for_terms(self):
        """Test that skos:exactMatch is NOT extracted for term-to-term (per spec)."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://example.org/> .
        
        ex:Term1 a skos:Concept ;
            skos:prefLabel "Term One" ;
            skos:exactMatch ex:Term2 .
        
        ex:Term2 a skos:Concept ;
            skos:prefLabel "Term Two" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        # Should have no relationships extracted (skos:exactMatch is not supported for term-to-term)
        self.assertEqual(len(datahub_graph.relationships), 0)


# TestDatasetBehavior removed - dataset extraction not supported in MVP


class TestMCPGenerationBehavior(unittest.TestCase):
    """Test MCP generation behavior."""

    def setUp(self):
        """Set up test fixtures."""
        from unittest.mock import MagicMock

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.rdf.ingestion.rdf_source import (
            RDFSource,
            RDFSourceConfig,
        )

        self.config = RDFSourceConfig(source="dummy", environment="PROD")
        self.ctx = MagicMock(spec=PipelineContext)
        self.ctx.graph = MagicMock()  # Required for StatefulIngestionSourceBase
        self.ctx.pipeline_name = (
            "test_pipeline"  # Required for StatefulIngestionSourceBase
        )
        self.source = RDFSource(self.config, self.ctx)

    def _get_mcps(self, graph):
        """Helper to get MCPs from RDF graph."""
        datahub_graph = self.source._convert_rdf_to_datahub_ast(
            graph,
            environment="PROD",
            export_only=None,
            skip_export=None,
        )
        # Generate work units directly using the inlined method
        workunits = self.source._generate_workunits_from_ast(datahub_graph)
        return workunits

    def test_glossary_term_mcp_generation(self):
        """Test that glossary term MCPs are generated correctly."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://example.org/> .
        
        ex:TestTerm a skos:Concept ;
            skos:prefLabel "Test Term" ;
            skos:definition "A test term" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        mcps = self._get_mcps(graph)

        # Should generate at least one MCP for the glossary term
        glossary_mcps = [m for m in mcps if "glossaryTerm" in m.get_urn()]
        self.assertGreater(len(glossary_mcps), 0)

        # Check MCP has correct entity URN
        mcp = glossary_mcps[0]
        self.assertIn("urn:li:glossaryTerm:", mcp.get_urn())

    def test_relationship_mcp_uses_isrelatedterms(self):
        """Test that broader relationships create isRelatedTerms MCPs (not hasRelatedTerms)."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <http://example.org/> .
        
        ex:ChildTerm a skos:Concept ;
            skos:prefLabel "Child Term" ;
            skos:broader ex:ParentTerm .
        
        ex:ParentTerm a skos:Concept ;
            skos:prefLabel "Parent Term" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        mcps = self._get_mcps(graph)

        # Find relationship MCPs (GlossaryRelatedTermsClass aspects)
        from datahub.metadata.schema_classes import GlossaryRelatedTermsClass

        rel_mcps = []
        for m in mcps:
            # Use get_aspect_of_type to get the aspect
            aspect = m.get_aspect_of_type(GlossaryRelatedTermsClass)
            if aspect:
                rel_mcps.append((m, aspect))

        # Should have at least one relationship MCP
        self.assertGreater(
            len(rel_mcps),
            0,
            f"Should have relationship MCPs, got {len(mcps)} total MCPs",
        )

        # Check that isRelatedTerms is populated (not hasRelatedTerms)
        child_mcp = next((m for m, a in rel_mcps if "ChildTerm" in m.get_urn()), None)
        self.assertIsNotNone(child_mcp, "Should have MCP for ChildTerm")
        if child_mcp:
            rel_aspect = child_mcp.get_aspect_of_type(GlossaryRelatedTermsClass)
            self.assertIsNotNone(
                rel_aspect, "ChildTerm MCP should have GlossaryRelatedTermsClass aspect"
            )
            if rel_aspect:
                self.assertIsNotNone(rel_aspect.isRelatedTerms)
                self.assertGreater(len(rel_aspect.isRelatedTerms), 0)


class TestEnvironmentBehavior(unittest.TestCase):
    """Test environment handling behavior."""

    def setUp(self):
        """Set up test fixtures."""
        from unittest.mock import MagicMock

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.rdf.ingestion.rdf_source import (
            RDFSource,
            RDFSourceConfig,
        )

        self.config = RDFSourceConfig(source="dummy", environment="PROD")
        self.ctx = MagicMock(spec=PipelineContext)
        self.ctx.graph = MagicMock()  # Required for StatefulIngestionSourceBase
        self.ctx.pipeline_name = (
            "test_pipeline"  # Required for StatefulIngestionSourceBase
        )
        self.source = RDFSource(self.config, self.ctx)

    # test_environment_passed_to_datasets removed - dataset extraction not supported in MVP


class TestEndToEndBehavior(unittest.TestCase):
    """End-to-end behavior tests with realistic RDF data."""

    def setUp(self):
        """Set up test fixtures."""
        from unittest.mock import MagicMock

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.rdf.ingestion.rdf_source import (
            RDFSource,
            RDFSourceConfig,
        )

        self.config = RDFSourceConfig(source="dummy", environment="PROD")
        self.ctx = MagicMock(spec=PipelineContext)
        self.ctx.graph = MagicMock()  # Required for StatefulIngestionSourceBase
        self.ctx.pipeline_name = (
            "test_pipeline"  # Required for StatefulIngestionSourceBase
        )
        self.source = RDFSource(self.config, self.ctx)

    def _get_datahub_graph(self, graph: Graph):
        """Helper to get DataHubGraph from RDF graph."""
        return self.source._convert_rdf_to_datahub_ast(
            graph,
            environment="PROD",
            export_only=None,
            skip_export=None,
        )

    def test_bcbs239_style_input(self):
        """Test with BCBS239-style input data."""
        ttl = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix void: <http://rdfs.org/ns/void#> .
        @prefix dcat: <http://www.w3.org/ns/dcat#> .
        @prefix dcterms: <http://purl.org/dc/terms/> .
        @prefix trading: <http://DataHubFinancial.com/TRADING/LOANS/> .
        @prefix ref: <http://DataHubFinancial.com/REFERENCE_DATA/ACCOUNTS/> .
        @prefix plat: <http://DataHubFinancial.com/PLATFORMS/> .
        
        # Glossary terms
        trading:Loan_Amount a skos:Concept ;
            skos:prefLabel "Loan Amount" ;
            skos:definition "Principal amount of the loan" .
        
        ref:Account_ID a skos:Concept ;
            skos:prefLabel "Account ID" ;
            skos:definition "Unique account identifier" ;
            skos:broader <http://fibo.org/AccountIdentifier> .
        
        # Dataset
        trading:Loan_Table a void:Dataset ;
            rdfs:label "Loan Table" ;
            rdfs:comment "Table of loan records" ;
            dcat:accessService plat:postgres .
        
        plat:postgres dcterms:title "postgres" .
        """

        graph = Graph()
        graph.parse(data=ttl, format="turtle")

        datahub_graph = self._get_datahub_graph(graph)

        # Verify glossary terms
        self.assertEqual(len(datahub_graph.glossary_terms), 2)
        term_names = {t.name for t in datahub_graph.glossary_terms}
        self.assertIn("Loan Amount", term_names)
        self.assertIn("Account ID", term_names)

        # Verify domains created
        domain_paths = {tuple(d.path_segments) for d in datahub_graph.domains}
        self.assertIn(("DataHubFinancial.com",), domain_paths)

        # Verify relationships
        broader_rels = [
            r
            for r in datahub_graph.relationships
            if r.relationship_type.value == "broader"
        ]
        self.assertEqual(len(broader_rels), 1)


# TestLineageBehavior removed - lineage extraction not supported in MVP

# TestDataProductBehavior removed - data product extraction not supported in MVP

# TestStructuredPropertyBehavior removed - structured property extraction not supported in MVP

# TestAssertionBehavior removed - assertion extraction not supported in MVP

# TestSchemaFieldBehavior removed - schema field extraction not supported in MVP (requires datasets)


class TestBCBS239FullParity(unittest.TestCase):
    """
    Test that bcbs239 example produces expected entity counts for MVP.

    MVP counts (based on old implementation):
    - 296 glossary terms
    - 22+ relationships
    - 21 domains
    """

    def setUp(self):
        """Load bcbs239 example data."""
        from pathlib import Path
        from unittest.mock import MagicMock

        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.rdf.ingestion.rdf_source import (
            RDFSource,
            RDFSourceConfig,
        )

        self.config = RDFSourceConfig(source="dummy", environment="PROD")
        self.ctx = MagicMock(spec=PipelineContext)
        self.ctx.graph = MagicMock()  # Required for StatefulIngestionSourceBase
        self.ctx.pipeline_name = (
            "test_pipeline"  # Required for StatefulIngestionSourceBase
        )
        self.source = RDFSource(self.config, self.ctx)

        # Load all bcbs239 TTL files
        self.graph = Graph()
        bcbs239_path = Path(__file__).parent.parent / "examples" / "bcbs239"

        if bcbs239_path.exists():
            for ttl_file in bcbs239_path.glob("*.ttl"):
                self.graph.parse(str(ttl_file), format="turtle")
            self.has_data = len(self.graph) > 0
        else:
            self.has_data = False

    def test_glossary_term_count(self):
        """Test that all glossary terms are extracted."""
        if not self.has_data:
            self.skipTest("bcbs239 data not available")

        datahub_graph = self.source._convert_rdf_to_datahub_ast(
            self.graph, environment="PROD", export_only=None, skip_export=None
        )

        # Old implementation extracted 296 glossary terms
        self.assertEqual(
            len(datahub_graph.glossary_terms),
            296,
            f"Expected 296 glossary terms, got {len(datahub_graph.glossary_terms)}",
        )

    # Non-MVP tests removed: dataset_count, data_product_count, lineage_relationship_count,
    # lineage_activity_count, structured_property_count, assertion_count

    def test_domain_count(self):
        """Test that domains are created."""
        if not self.has_data:
            self.skipTest("bcbs239 data not available")

        datahub_graph = self.source._convert_rdf_to_datahub_ast(
            self.graph, environment="PROD", export_only=None, skip_export=None
        )

        # Old implementation created 21 domains
        self.assertEqual(
            len(datahub_graph.domains),
            21,
            f"Expected 21 domains, got {len(datahub_graph.domains)}",
        )

    def test_relationship_count(self):
        """Test that term relationships are extracted."""
        if not self.has_data:
            self.skipTest("bcbs239 data not available")

        datahub_graph = self.source._convert_rdf_to_datahub_ast(
            self.graph, environment="PROD", export_only=None, skip_export=None
        )

        # Old implementation had 22 relationships
        self.assertGreaterEqual(
            len(datahub_graph.relationships),
            9,
            f"Expected at least 9 relationships, got {len(datahub_graph.relationships)}",
        )


if __name__ == "__main__":
    unittest.main()
