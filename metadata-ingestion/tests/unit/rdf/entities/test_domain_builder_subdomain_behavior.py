#!/usr/bin/env python3
"""
Unit tests to verify subdomains are NOT treated as root domains.

This test ensures that:
- Subdomains are in the returned list (so they get MCPs)
- Subdomains are in their parent's subdomains list (hierarchy)
- Subdomains have parent_domain_urn set (not None)
- Subdomains are NOT treated as root domains anywhere
"""

import unittest

from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
from datahub.ingestion.source.rdf.entities.glossary_term.ast import DataHubGlossaryTerm


class TestDomainBuilderSubdomainBehavior(unittest.TestCase):
    """Test that subdomains are correctly handled and not treated as root domains."""

    def setUp(self):
        """Set up test fixtures."""
        self.builder = DomainBuilder()

    def test_subdomains_have_parent_domain_urn_set(self):
        """Test that subdomains have parent_domain_urn set (not None)."""
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

        # Find subdomains
        root_domain = next(d for d in domains if d.parent_domain_urn is None)
        trading_domain = root_domain.subdomains[0]
        loans_domain = trading_domain.subdomains[0]

        # Verify subdomains have parent_domain_urn set
        self.assertIsNotNone(
            trading_domain.parent_domain_urn,
            "Subdomain trading should have parent_domain_urn set",
        )
        self.assertIsNotNone(
            loans_domain.parent_domain_urn,
            "Subdomain loans should have parent_domain_urn set",
        )

        # Verify they're NOT root domains
        self.assertNotEqual(
            trading_domain.parent_domain_urn,
            None,
            "Subdomain should NOT be a root domain",
        )
        self.assertNotEqual(
            loans_domain.parent_domain_urn,
            None,
            "Subdomain should NOT be a root domain",
        )

    def test_subdomains_in_list_and_hierarchy(self):
        """Test that subdomains are in both the returned list AND parent's subdomains list."""
        terms = [
            DataHubGlossaryTerm(
                urn="urn:li:glossaryTerm:bank/loans/Account",
                name="Account",
                definition="Test",
                source=None,
                custom_properties={},
                path_segments=["bank", "loans", "Account"],
            ),
        ]

        domains = self.builder.build_domains(terms)

        # Subdomains should be in returned list
        subdomains_in_list = [d for d in domains if d.parent_domain_urn is not None]
        self.assertEqual(
            len(subdomains_in_list), 1, "Subdomain should be in returned list"
        )

        # Subdomains should ALSO be in parent's subdomains list
        root_domain = next(d for d in domains if d.parent_domain_urn is None)
        self.assertEqual(
            len(root_domain.subdomains),
            1,
            "Subdomain should be in parent's subdomains list",
        )

        # Verify it's the same domain object
        subdomain_in_list = subdomains_in_list[0]
        subdomain_in_hierarchy = root_domain.subdomains[0]
        self.assertEqual(
            subdomain_in_list.urn,
            subdomain_in_hierarchy.urn,
            "Subdomain should be the same object in both places",
        )

    def test_no_subdomain_treated_as_root(self):
        """Test that no subdomain is treated as a root domain."""
        terms = [
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

        # Count root vs subdomains
        root_domains = [d for d in domains if d.parent_domain_urn is None]
        subdomains = [d for d in domains if d.parent_domain_urn is not None]

        self.assertEqual(len(root_domains), 1, "Should have exactly 1 root domain")
        self.assertEqual(len(subdomains), 1, "Should have exactly 1 subdomain")

        # Verify subdomain is NOT a root domain
        subdomain = subdomains[0]
        self.assertIsNotNone(
            subdomain.parent_domain_urn,
            "Subdomain must have parent_domain_urn set (not None)",
        )
        self.assertNotIn(
            subdomain,
            root_domains,
            "Subdomain should NOT be in root_domains list",
        )


if __name__ == "__main__":
    unittest.main()
