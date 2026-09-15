#!/usr/bin/env python3
"""
Test Fixtures and Mock Data for DataHub RDF Operations

This module provides test fixtures, mock data, and utility functions
for unit testing the modular RDF to DataHub system.
"""

import os
import tempfile
from pathlib import Path
from typing import List

from rdflib import Graph, Literal, Namespace, URIRef

# Test namespaces
TEST_DCAT = Namespace("http://www.w3.org/ns/dcat#")
TEST_DH = Namespace("http://datahub.com/ontology/")
TEST_BCBS = Namespace("http://BCBS239/GOVERNANCE/")
TEST_RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
TEST_RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")


class TestDataFactory:
    """Factory for creating test data and graphs."""

    @staticmethod
    def create_simple_dataset_graph() -> Graph:
        """Create a simple test graph with one dataset."""
        graph = Graph()

        # Add namespaces
        graph.bind("dcat", TEST_DCAT)
        graph.bind("dh", TEST_DH)
        graph.bind("bcbs", TEST_BCBS)

        # Create dataset
        dataset_uri = URIRef("http://TEST/Dataset1")
        graph.add((dataset_uri, TEST_RDF.type, TEST_DCAT.Dataset))
        graph.add((dataset_uri, TEST_DH.platform, Literal("postgres")))
        graph.add((dataset_uri, TEST_BCBS.authorized, TEST_BCBS.Source))

        return graph

    @staticmethod
    def create_multi_dataset_graph() -> Graph:
        """Create a test graph with multiple datasets."""
        graph = Graph()

        # Add namespaces
        graph.bind("dcat", TEST_DCAT)
        graph.bind("dh", TEST_DH)
        graph.bind("bcbs", TEST_BCBS)

        # Dataset 1
        dataset1 = URIRef("http://TEST/Dataset1")
        graph.add((dataset1, TEST_RDF.type, TEST_DCAT.Dataset))
        graph.add((dataset1, TEST_DH.platform, Literal("postgres")))
        graph.add((dataset1, TEST_BCBS.authorized, TEST_BCBS.Source))

        # Dataset 2
        dataset2 = URIRef("http://TEST/Dataset2")
        graph.add((dataset2, TEST_RDF.type, TEST_DCAT.Dataset))
        graph.add((dataset2, TEST_DH.platform, Literal("mysql")))
        graph.add((dataset2, TEST_BCBS.authorized, TEST_BCBS.Distributor))

        # Dataset 3 (no platform)
        dataset3 = URIRef("http://TEST/Dataset3")
        graph.add((dataset3, TEST_RDF.type, TEST_DCAT.Dataset))
        graph.add((dataset3, TEST_BCBS.authorized, TEST_BCBS.Source))

        return graph

    @staticmethod
    def create_property_definition_graph() -> Graph:
        """Create a test graph with structured property definitions."""
        graph = Graph()

        # Add namespaces
        graph.bind("rdf", TEST_RDF)
        graph.bind("rdfs", TEST_RDFS)
        graph.bind("dcat", TEST_DCAT)
        graph.bind("bcbs", TEST_BCBS)

        # Property definition
        property_uri = URIRef("http://BCBS239/GOVERNANCE/authorized")
        graph.add((property_uri, TEST_RDF.type, TEST_RDF.Property))
        graph.add((property_uri, TEST_RDFS.domain, TEST_DCAT.Dataset))
        graph.add((property_uri, TEST_RDFS.range, TEST_BCBS.AuthorizationType))
        graph.add((property_uri, TEST_RDFS.label, Literal("authorized")))
        graph.add(
            (
                property_uri,
                TEST_RDFS.comment,
                Literal("Authorization type for datasets"),
            )
        )

        # Enum values
        graph.add((TEST_BCBS.Source, TEST_RDF.type, TEST_BCBS.AuthorizationType))
        graph.add((TEST_BCBS.Distributor, TEST_RDF.type, TEST_BCBS.AuthorizationType))

        return graph

    @staticmethod
    def create_complex_graph() -> Graph:
        """Create a complex test graph with datasets and property definitions."""
        graph = Graph()

        # Add namespaces
        graph.bind("rdf", TEST_RDF)
        graph.bind("rdfs", TEST_RDFS)
        graph.bind("dcat", TEST_DCAT)
        graph.bind("dh", TEST_DH)
        graph.bind("bcbs", TEST_BCBS)

        # Property definition
        property_uri = URIRef("http://BCBS239/GOVERNANCE/authorized")
        graph.add((property_uri, TEST_RDF.type, TEST_RDF.Property))
        graph.add((property_uri, TEST_RDFS.domain, TEST_DCAT.Dataset))
        graph.add((property_uri, TEST_RDFS.range, TEST_BCBS.AuthorizationType))
        graph.add((property_uri, TEST_RDFS.label, Literal("authorized")))
        graph.add(
            (
                property_uri,
                TEST_RDFS.comment,
                Literal("Authorization type for datasets"),
            )
        )

        # Enum values
        graph.add((TEST_BCBS.Source, TEST_RDF.type, TEST_BCBS.AuthorizationType))
        graph.add((TEST_BCBS.Distributor, TEST_RDF.type, TEST_BCBS.AuthorizationType))

        # Dataset 1
        dataset1 = URIRef("http://TEST/Dataset1")
        graph.add((dataset1, TEST_RDF.type, TEST_DCAT.Dataset))
        graph.add((dataset1, TEST_DH.platform, Literal("postgres")))
        graph.add((dataset1, property_uri, TEST_BCBS.Source))

        # Dataset 2
        dataset2 = URIRef("http://TEST/Dataset2")
        graph.add((dataset2, TEST_RDF.type, TEST_DCAT.Dataset))
        graph.add((dataset2, TEST_DH.platform, Literal("mysql")))
        graph.add((dataset2, property_uri, TEST_BCBS.Distributor))

        return graph


class TempFileManager:
    """Manages temporary test files."""

    def __init__(self):
        self.temp_dir = None
        self.temp_files = []

    def create_temp_file(self, content: str, suffix: str = ".ttl") -> Path:
        """Create a temporary file with given content."""
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp()

        temp_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=suffix, dir=self.temp_dir, delete=False
        )
        temp_file.write(content)
        temp_file.close()

        self.temp_files.append(temp_file.name)
        return Path(temp_file.name)

    def create_temp_directory(self) -> Path:
        """Create a temporary directory."""
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp()

        temp_dir = tempfile.mkdtemp(dir=self.temp_dir)
        return Path(temp_dir)

    def cleanup(self):
        """Clean up all temporary files and directories."""
        for file_path in self.temp_files:
            try:
                os.unlink(file_path)
            except OSError:
                pass

        if self.temp_dir:
            try:
                os.rmdir(self.temp_dir)
            except OSError:
                pass

    # MockDataHubClient removed - CLI-only, not used by ingestion source

    def set_emit_success(self, success: bool) -> None:
        """Set whether MCP emission should succeed."""
        self.emit_success = success

    def set_emit_error(self, error: Exception) -> None:
        """Set error to raise during MCP emission."""
        self.emit_error = error

    def get_emitted_mcps(self) -> List:
        """Get list of emitted MCPs."""
        return self.emitted_mcps.copy()

    def clear_emitted_mcps(self):
        """Clear emitted MCPs list."""
        self.emitted_mcps = []


def create_test_ttl_content() -> str:
    """Create test TTL content."""
    return """
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dh: <http://datahub.com/ontology/> .
@prefix bcbs: <http://BCBS239/GOVERNANCE/> .

<http://TEST/Dataset1> a dcat:Dataset ;
    dh:platform "postgres" ;
    bcbs:authorized bcbs:Source .

<http://TEST/Dataset2> a dcat:Dataset ;
    dh:platform "mysql" ;
    bcbs:authorized bcbs:Distributor .
"""


def create_test_property_ttl_content() -> str:
    """Create test TTL content with property definitions."""
    return """
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix bcbs: <http://BCBS239/GOVERNANCE/> .

bcbs:authorized a rdf:Property ;
    rdfs:domain dcat:Dataset ;
    rdfs:range bcbs:AuthorizationType ;
    rdfs:label "authorized" ;
    rdfs:comment "Authorization type for datasets" .

bcbs:AuthorizationType a rdfs:Class .

bcbs:Source a bcbs:AuthorizationType .
bcbs:Distributor a bcbs:AuthorizationType .
"""
