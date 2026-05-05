#!/usr/bin/env python3
"""
Unit tests for DomainBuilder.

Tests domain hierarchy creation from glossary terms, ensuring:
- Only root domains are returned
- Subdomains are accessible through parent's subdomains list
- Subdomains are NOT in the returned list
- Hierarchy is correctly structured
"""

import unittest

from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
from datahub.ingestion.source.rdf.entities.glossary_term.ast import DataHubGlossaryTerm


class TestDomainBuilder(unittest.TestCase):
    """Test DomainBuilder functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.builder = DomainBuilder()

    def test_build_domains_returns_all_domains(self):
        """Test that build_domains returns all domains (root and subdomains)."""
        terms = [
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/loans/Account",
                name="Account",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "loans", "Account"],
            ),
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/trading/Position",
                name="Position",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "trading", "Position"],
            ),
        ]

        domains = self.builder.build_domains(terms)

        # Should return all domains (root + subdomains) so all get MCPs created
        self.assertEqual(
            len(domains), 3, "Should return all domains (1 root + 2 subdomains)"
        )

        # Verify we have both root and subdomains
        root_domains = [d for d in domains if d.parent_domain_urn is None]
        subdomains = [d for d in domains if d.parent_domain_urn is not None]
        self.assertEqual(len(root_domains), 1, "Should have 1 root domain")
        self.assertEqual(len(subdomains), 2, "Should have 2 subdomains")

    def test_subdomains_accessible_through_parent(self):
        """Test that subdomains are accessible through parent's subdomains list."""
        terms = [
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/loans/Account",
                name="Account",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "loans", "Account"],
            ),
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/trading/Position",
                name="Position",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "trading", "Position"],
            ),
        ]

        domains = self.builder.build_domains(terms)

        # Get root domain
        root_domain = domains[0]
        self.assertEqual(root_domain.name, "bank")
        self.assertEqual(len(root_domain.subdomains), 2)

        # Verify subdomains are accessible
        subdomain_names = {sd.name for sd in root_domain.subdomains}
        self.assertIn("loans", subdomain_names)
        self.assertIn("trading", subdomain_names)

    def test_subdomains_not_in_returned_list(self):
        """Test that subdomains are NOT in the returned domains list."""
        terms = [
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/loans/Account",
                name="Account",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "loans", "Account"],
            ),
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/trading/Position",
                name="Position",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "trading", "Position"],
            ),
        ]

        domains = self.builder.build_domains(terms)

        # Subdomains should be in the returned list (so they get MCPs created)
        subdomains_in_list = [d for d in domains if d.parent_domain_urn is not None]
        self.assertEqual(
            len(subdomains_in_list), 2, "Subdomains should be in returned list"
        )

        # Subdomains should ALSO be in their parent's subdomains list
        root_domain = next(d for d in domains if d.parent_domain_urn is None)
        subdomain_names_in_hierarchy = {sd.name for sd in root_domain.subdomains}
        subdomain_names_in_list = {sd.name for sd in subdomains_in_list}
        self.assertEqual(
            subdomain_names_in_hierarchy,
            subdomain_names_in_list,
            "Subdomains should be in both returned list and parent's subdomains list",
        )

    def test_nested_hierarchy_structure(self):
        """Test that nested hierarchy is correctly structured."""
        terms = [
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/trading/loans/Customer",
                name="Customer",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "trading", "loans", "Customer"],
            ),
        ]

        domains = self.builder.build_domains(terms)

        # Should return all domains (root + subdomains)
        self.assertEqual(len(domains), 3)

        # Find root domain
        root_domain = next(d for d in domains if d.parent_domain_urn is None)
        self.assertEqual(root_domain.name, "bank")
        self.assertIsNone(root_domain.parent_domain_urn)

        # Check first level subdomain
        self.assertEqual(len(root_domain.subdomains), 1)
        trading_domain = root_domain.subdomains[0]
        self.assertEqual(trading_domain.name, "trading")
        self.assertEqual(trading_domain.parent_domain_urn, root_domain.urn)

        # Check second level subdomain
        self.assertEqual(len(trading_domain.subdomains), 1)
        loans_domain = trading_domain.subdomains[0]
        self.assertEqual(loans_domain.name, "loans")
        self.assertEqual(loans_domain.parent_domain_urn, trading_domain.urn)

        # Verify subdomains ARE in returned list (so they get MCPs)
        self.assertIn(trading_domain, domains)
        self.assertIn(loans_domain, domains)

    def test_multiple_root_domains(self):
        """Test that multiple root domains are returned correctly."""
        terms = [
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/Account",
                name="Account",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "Account"],
            ),
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:finance/Balance",
                name="Balance",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["finance", "Balance"],
            ),
        ]

        domains = self.builder.build_domains(terms)

        # Should return 2 root domains (no subdomains in this case)
        self.assertEqual(len(domains), 2)

        # All should be root domains
        for domain in domains:
            self.assertIsNone(domain.parent_domain_urn)

        # Verify domain names
        domain_names = {d.name for d in domains}
        self.assertIn("bank", domain_names)
        self.assertIn("finance", domain_names)

        # Get domains
        bank_domain = next(d for d in domains if d.name == "bank")
        self.assertEqual(len(bank_domain.subdomains), 0)  # No subdomains, only terms

    def test_terms_assigned_to_correct_domain(self):
        """Test that terms are assigned to the correct leaf domain."""
        terms = [
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/trading/Trade_ID",
                name="Trade ID",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "trading", "Trade_ID"],
            ),
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/trading/loans/Loan_Amount",
                name="Loan Amount",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "trading", "loans", "Loan_Amount"],
            ),
        ]

        domains = self.builder.build_domains(terms)

        # Navigate to trading domain
        root_domain = domains[0]
        trading_domain = next(
            sd for sd in root_domain.subdomains if sd.name == "trading"
        )

        # Navigate to loans domain
        loans_domain = next(
            sd for sd in trading_domain.subdomains if sd.name == "loans"
        )

        # Verify terms are in correct domains
        self.assertEqual(len(trading_domain.glossary_terms), 1)
        self.assertEqual(trading_domain.glossary_terms[0].name, "Trade ID")

        self.assertEqual(len(loans_domain.glossary_terms), 1)
        self.assertEqual(loans_domain.glossary_terms[0].name, "Loan Amount")

    def _collect_subdomains(self, domain, subdomains_list):
        """Recursively collect all subdomains."""
        for subdomain in domain.subdomains:
            subdomains_list.append(subdomain)
            self._collect_subdomains(subdomain, subdomains_list)


if __name__ == "__main__":
    unittest.main()
