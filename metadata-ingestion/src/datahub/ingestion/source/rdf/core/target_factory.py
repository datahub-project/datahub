#!/usr/bin/env python3
"""
Target Factory Interface

This module provides a factory interface for creating different types of output targets.
Supports DataHub targets, pretty print targets, and file targets with dependency injection.
"""

import datetime
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from rdflib import Graph
from rdflib.namespace import DCAT, DCTERMS, RDF, RDFS, VOID

from datahub.ingestion.source.rdf.core.ast import DataHubGraph, RDFOwnership

# DataHub imports removed - all DataHub operations now go through DataHubClient
from datahub.ingestion.source.rdf.core.datahub_client import DataHubClient
from datahub.ingestion.source.rdf.entities.dataset.ast import DataHubDataset
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)
from datahub.ingestion.source.rdf.entities.structured_property.ast import (
    DataHubStructuredProperty,
)

logger = logging.getLogger(__name__)


class SimpleReport:
    """Simple report class for DataHubTarget that tracks basic statistics."""

    def __init__(self):
        self.num_entities_emitted = 0
        self.num_workunits_produced = 0

    def report_entity_emitted(self):
        """Report that an entity was emitted."""
        self.num_entities_emitted += 1

    def report_workunit_produced(self):
        """Report that a work unit was produced."""
        self.num_workunits_produced += 1


class TargetInterface(ABC):
    """Abstract interface for output targets."""

    @abstractmethod
    def execute(
        self, datahub_ast: DataHubGraph, rdf_graph: Graph = None
    ) -> Dict[str, Any]:
        """Execute the target with the DataHub AST."""
        pass

    @abstractmethod
    def get_target_info(self) -> dict:
        """Get information about this target."""
        pass


class DataHubTarget(TargetInterface):
    """Target that sends data to DataHub using the ingestion target internally."""

    def __init__(self, datahub_client: DataHubClient, rdf_graph: Graph = None):
        self.datahub_client = datahub_client
        self.rdf_graph = rdf_graph
        self.report = SimpleReport()
        # Lazy import to avoid circular dependency
        self._ingestion_target = None

    @property
    def ingestion_target(self):
        """Lazy load ingestion target to avoid circular imports."""
        if self._ingestion_target is None:
            from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
                DataHubIngestionTarget,
            )

            self._ingestion_target = DataHubIngestionTarget(self.report)
        return self._ingestion_target

    def execute(
        self, datahub_ast: DataHubGraph, rdf_graph: Graph = None
    ) -> Dict[str, Any]:
        """Execute DataHub target by generating work units and emitting them."""
        try:
            logger.info("Executing DataHub target...")

            # Store RDF graph if provided
            if rdf_graph:
                self.rdf_graph = rdf_graph

            # Generate work units using ingestion target
            ingestion_results = self.ingestion_target.execute(datahub_ast, rdf_graph)

            if not ingestion_results.get("success"):
                return {
                    "success": False,
                    "target_type": "datahub",
                    "error": ingestion_results.get("error", "Unknown error"),
                }

            # Emit all work units via DataHubClient
            workunits = self.ingestion_target.get_workunits()
            logger.info(f"Emitting {len(workunits)} work units to DataHub...")

            errors = []
            entities_emitted = 0

            for workunit in workunits:
                try:
                    # Extract MCP from work unit and emit it
                    # MetadataWorkUnit stores MCP in metadata attribute
                    mcp = None
                    if hasattr(workunit, "mcp") and workunit.mcp:
                        mcp = workunit.mcp
                    elif hasattr(workunit, "metadata") and workunit.metadata:
                        # MetadataWorkUnit may store MCP as metadata
                        from datahub.emitter.mcp import MetadataChangeProposalWrapper

                        if isinstance(workunit.metadata, MetadataChangeProposalWrapper):
                            mcp = workunit.metadata
                        elif hasattr(workunit.metadata, "mcp"):
                            mcp = workunit.metadata.mcp

                    if mcp:
                        self.datahub_client._emit_mcp(mcp)
                        entities_emitted += 1
                except Exception as e:
                    error_msg = f"Failed to emit work unit {workunit.id}: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            logger.info(
                f"✅ DataHub execution completed: {entities_emitted} entities emitted"
            )

            return {
                "success": True,
                "target_type": "datahub",
                "results": {
                    "strategy": "live_datahub",
                    "workunits_generated": len(workunits),
                    "entities_emitted": entities_emitted,
                    "errors": errors,
                },
            }
        except Exception as e:
            logger.error(f"DataHub target execution failed: {e}")
            return {"success": False, "target_type": "datahub", "error": str(e)}

    def get_target_info(self) -> dict:
        """Get DataHub target information."""
        return {
            "type": "datahub",
            "server": self.datahub_client.datahub_gms if self.datahub_client else None,
            "has_token": self.datahub_client.api_token is not None
            if self.datahub_client
            else False,
        }


class PrettyPrintTarget(TargetInterface):
    """Target that pretty prints the DataHub AST."""

    def __init__(self, urn_generator=None):
        # Create URN generators if not provided
        if urn_generator is None:
            from datahub.ingestion.source.rdf.entities.domain.urn_generator import (
                DomainUrnGenerator,
            )
            from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
                GlossaryTermUrnGenerator,
            )

            self.domain_urn_generator = DomainUrnGenerator()
            self.glossary_urn_generator = GlossaryTermUrnGenerator()
        else:
            # For backward compatibility, use provided generator if it has the methods
            self.domain_urn_generator = urn_generator
            self.glossary_urn_generator = urn_generator

    def execute(
        self, datahub_ast: DataHubGraph, rdf_graph: Graph = None
    ) -> Dict[str, Any]:
        """Execute pretty print target."""
        try:
            logger.info("Executing pretty print target...")
            results = self._execute_pretty_print(datahub_ast)
            logger.info("Pretty print target execution completed successfully")
            return {"success": True, "target_type": "pretty_print", "results": results}
        except Exception as e:
            logger.error(f"Pretty print target execution failed: {e}")
            return {"success": False, "target_type": "pretty_print", "error": str(e)}

    def _execute_pretty_print(self, datahub_ast: DataHubGraph) -> Dict[str, Any]:
        """Execute pretty print operations."""
        logger.info("Executing pretty print strategy")

        results = {
            "strategy": "pretty_print",
            "success": True,
            "summary": datahub_ast.get_summary(),
            "pretty_output": self._format_pretty_output(datahub_ast),
        }

        logger.info(f"Pretty print complete: {results['summary']}")
        return results

    def _format_pretty_output(self, datahub_ast: DataHubGraph) -> str:  # noqa: C901
        """Format DataHub AST as pretty printed output."""
        output = []
        output.append("=" * 80)
        output.append("DATASETS")
        output.append("=" * 80)

        if not datahub_ast.datasets:
            output.append("No datasets found.")
        else:
            for i, dataset in enumerate(datahub_ast.datasets, 1):
                output.append(f"\n{i}. Dataset: {dataset.name}")
                output.append(f"   URN: {dataset.urn}")
                output.append(f"   Platform: {dataset.platform}")
                output.append(f"   Environment: {dataset.environment}")
                if dataset.description:
                    output.append(f"   Description: {dataset.description}")
                if dataset.path_segments and len(dataset.path_segments) > 1:
                    parent_path = tuple(dataset.path_segments[:-1])
                    assigned_domain_urn = self.domain_urn_generator.generate_domain_urn(
                        parent_path
                    )
                    output.append(f"   Assigned Domain: {assigned_domain_urn}")
                if dataset.custom_properties:
                    output.append(f"   Custom Properties: {dataset.custom_properties}")
                if dataset.schema_fields:
                    output.append(
                        f"   Schema Fields: {len(dataset.schema_fields)} fields"
                    )
                    for field in dataset.schema_fields:
                        # Schema fields are now SchemaFieldClass objects
                        field_name = field.fieldPath
                        if not field_name:
                            raise ValueError(
                                f"Schema field name required for dataset: {dataset.name}"
                            )
                        if not hasattr(field.type, "type") or not field.type.type:
                            raise ValueError(
                                f"Schema field type required for field '{field_name}' in dataset: {dataset.name}"
                            )
                        field_type = field.type.type.__class__.__name__
                        output.append(f"     - {field_name}: {field_type}")

        output.append("\n" + "=" * 80)
        output.append("DOMAINS")
        output.append("=" * 80)

        if not datahub_ast.domains:
            output.append("No domains found.")
        else:
            for i, domain in enumerate(datahub_ast.domains, 1):
                output.append(f"\n{i}. Domain: {domain.name}")
                output.append(f"   URN: {domain.urn}")
                if domain.description:
                    output.append(f"   Description: {domain.description}")
                if hasattr(domain, "parent_domain") and domain.parent_domain:
                    output.append(f"   Parent Domain: {domain.parent_domain}")
                if domain.owners:
                    output.append(f"   Owners: {len(domain.owners)} owner groups")
                    for owner_iri in domain.owners:
                        output.append(f"     - {owner_iri}")

        output.append("\n" + "=" * 80)
        output.append("GLOSSARY TERMS")
        output.append("=" * 80)

        if not datahub_ast.glossary_terms:
            output.append("No glossary terms found.")
        else:
            for i, term in enumerate(datahub_ast.glossary_terms, 1):
                output.append(f"\n{i}. Glossary Term: {term.name}")
                output.append(f"   urn: {term.urn}")
                if term.definition:
                    output.append(f"   Definition: {term.definition}")
                if term.source:
                    output.append(f"   Source: {term.source}")
                if term.path_segments and len(term.path_segments) > 1:
                    parent_path = tuple(term.path_segments[:-1])
                    # Convert tuple to string for glossary node URN generation (preserves hierarchy)
                    parent_path_str = "/".join(parent_path)
                    parent_glossary_node_urn = self.glossary_urn_generator.generate_glossary_node_urn_from_name(
                        parent_path_str
                    )
                    output.append(
                        f"   Parent Glossary Node: {parent_glossary_node_urn}"
                    )
                if term.relationships:
                    for rel_type, rel_values in term.relationships.items():
                        if rel_values:
                            output.append(
                                f"   {rel_type.title()}: {', '.join(rel_values)}"
                            )

        output.append("\n" + "=" * 80)
        output.append("STRUCTURED PROPERTIES")
        output.append("=" * 80)

        if not datahub_ast.structured_properties:
            output.append("No structured properties found.")
        else:
            for i, prop in enumerate(datahub_ast.structured_properties, 1):
                output.append(f"\n{i}. Structured Property: {prop.name}")
                output.append(f"   URN: {prop.urn}")
                output.append(f"   Type: {prop.value_type}")
                output.append(f"   Cardinality: {prop.cardinality}")
                if prop.description:
                    output.append(f"   Description: {prop.description}")
                if prop.allowed_values:
                    output.append(
                        f"   Allowed Values: {', '.join(prop.allowed_values)}"
                    )
                if prop.entity_types:
                    output.append(f"   Entity Types: {', '.join(prop.entity_types)}")

        # Print lineage activities
        output.append("\n" + "=" * 80)
        output.append("LINEAGE ACTIVITIES")
        output.append("=" * 80)

        lineage_activities = getattr(datahub_ast, "lineage_activities", [])
        if not lineage_activities:
            output.append("No lineage activities found.")
        else:
            for i, activity in enumerate(lineage_activities, 1):
                output.append(f"\n{i}. Lineage Activity: {activity.name}")
                output.append(f"   URN: {activity.urn}")
                if activity.description:
                    output.append(f"   Description: {activity.description}")
                if activity.started_at_time:
                    output.append(f"   Started: {activity.started_at_time}")
                if activity.ended_at_time:
                    output.append(f"   Ended: {activity.ended_at_time}")
                if activity.was_associated_with:
                    output.append(f"   Associated With: {activity.was_associated_with}")

        # Print lineage relationships
        output.append("\n" + "=" * 80)
        output.append("LINEAGE RELATIONSHIPS")
        output.append("=" * 80)

        if not datahub_ast.lineage_relationships:
            output.append("No lineage relationships found.")
        else:
            for i, rel in enumerate(datahub_ast.lineage_relationships, 1):
                output.append(f"\n{i}. Lineage Relationship: {rel.lineage_type.value}")
                output.append(f"   Source: {rel.source_urn}")
                output.append(f"   Target: {rel.target_urn}")
                if rel.activity_urn:
                    output.append(f"   Activity: {rel.activity_urn}")

        output.append("\n" + "=" * 80)
        output.append("DATA PRODUCTS")
        output.append("=" * 80)

        if not datahub_ast.data_products:
            output.append("No data products found.")
        else:
            for i, data_product in enumerate(datahub_ast.data_products, 1):
                output.append(f"\n{i}. Data Product: {data_product.name}")
                output.append(f"   URN: {data_product.urn}")
                output.append(f"   Domain: {data_product.domain}")
                output.append(f"   Owner: {data_product.owner}")
                output.append(f"   Description: {data_product.description}")
                if data_product.sla:
                    output.append(f"   SLA: {data_product.sla}")
                if data_product.quality_score:
                    output.append(f"   Quality Score: {data_product.quality_score}")
                if data_product.assets:
                    output.append(f"   Assets ({len(data_product.assets)}):")
                    for asset in data_product.assets:
                        output.append(f"     - {asset}")

        # Only print assertions in debug mode
        if logger.isEnabledFor(logging.DEBUG):
            output.append("\n" + "=" * 80)
            output.append("ASSERTIONS")
            output.append("=" * 80)

            if not datahub_ast.assertions:
                output.append("No assertions found.")
            else:
                for i, assertion in enumerate(datahub_ast.assertions, 1):
                    output.append(f"\n{i}. Assertion: {assertion.assertion_key}")
                    output.append(f"   Dataset URN: {assertion.dataset_urn}")
                    if assertion.field_name:
                        output.append(f"   Field: {assertion.field_name}")
                    output.append(f"   Type: {assertion.assertion_type}")
                    if assertion.operator:
                        output.append(f"   Operator: {assertion.operator}")
                    if assertion.description:
                        output.append(f"   Description: {assertion.description}")
                    if assertion.parameters:
                        output.append(f"   Parameters: {assertion.parameters}")

        output.append("\n" + "=" * 80)
        output.append("=" * 80)
        summary = datahub_ast.get_summary()
        output.append(f"Datasets: {summary['datasets']}")
        output.append(f"Glossary Terms: {summary['glossary_terms']}")
        output.append(f"Structured Properties: {summary['structured_properties']}")
        output.append(f"Data Products: {summary['data_products']}")
        output.append(f"Lineage Activities: {summary.get('lineage_activities', 0)}")
        output.append(
            f"Lineage Relationships: {summary.get('lineage_relationships', 0)}"
        )
        # Always show assertion count in summary (detailed list is debug-only)
        if "assertions" in summary:
            output.append(f"Assertions: {summary['assertions']}")

        return "\n".join(output)

    def get_target_info(self) -> dict:
        """Get pretty print target information."""
        return {"type": "pretty_print"}


class FileTarget(TargetInterface):
    """Target that writes output to files."""

    def __init__(self, output_file: str, format: str):
        if not format:
            raise ValueError("Format is required for FileTarget")
        self.output_file = output_file
        self.format = format

    def execute(
        self, datahub_ast: DataHubGraph, rdf_graph: Graph = None
    ) -> Dict[str, Any]:
        """Execute file target."""
        try:
            logger.info(f"Executing file target: {self.output_file}")
            results = self._execute_file_output(datahub_ast)
            logger.info(f"File target execution completed: {self.output_file}")
            return {
                "success": True,
                "target_type": "file",
                "output_file": self.output_file,
                "results": results,
            }
        except Exception as e:
            logger.error(f"File target execution failed: {e}")
            return {"success": False, "target_type": "file", "error": str(e)}

    def _execute_file_output(self, datahub_ast: DataHubGraph) -> Dict[str, Any]:
        """Execute file output operations."""
        logger.info(f"Executing file output strategy to {self.output_file}")

        results = {
            "strategy": "file_output",
            "success": True,
            "files_created": [],
            "output_file": self.output_file,
            "summary": datahub_ast.get_summary(),
        }

        try:
            # Write datasets
            datasets_data = [self._dataset_to_dict(d) for d in datahub_ast.datasets]
            with open(self.output_file, "w") as f:
                json.dump(
                    {
                        "datasets": datasets_data,
                        "glossary_terms": [
                            self._term_to_dict(t) for t in datahub_ast.glossary_terms
                        ],
                        "structured_properties": [
                            self._property_to_dict(p)
                            for p in datahub_ast.structured_properties
                        ],
                        "summary": datahub_ast.get_summary(),
                    },
                    f,
                    indent=2,
                )

            results["files_created"].append(self.output_file)

            logger.info(
                f"File output complete: {len(results['files_created'])} files created"
            )
            return results

        except Exception as e:
            logger.error(f"File output failed: {e}")
            results["success"] = False
            results["error"] = str(e)
            return results

    def _dataset_to_dict(self, dataset: DataHubDataset) -> Dict[str, Any]:
        """Convert dataset to dictionary."""
        return {
            "urn": dataset.urn,
            "name": dataset.name,
            "description": dataset.description,
            "platform": dataset.platform,
            "environment": dataset.environment,
            "properties": dataset.properties,
            "schema_fields": dataset.schema_fields,
            "structured_properties": dataset.structured_properties,
            "custom_properties": dataset.custom_properties,
        }

    def _term_to_dict(self, term: DataHubGlossaryTerm) -> Dict[str, Any]:
        """Convert glossary term to dictionary."""
        return {
            "name": term.name,
            "definition": term.definition,
            "source": term.source,
            "properties": term.properties,
            "relationships": term.relationships,
            "custom_properties": term.custom_properties,
        }

    def _property_to_dict(self, prop: DataHubStructuredProperty) -> Dict[str, Any]:
        """Convert structured property to dictionary."""
        return {
            "name": prop.name,
            "description": prop.description,
            "value_type": prop.value_type,
            "allowed_values": prop.allowed_values,
            "entity_types": prop.entity_types,
            "cardinality": prop.cardinality,
            "properties": prop.properties,
        }

    def get_target_info(self) -> dict:
        """Get file target information."""
        return {"type": "file", "output_file": self.output_file, "format": self.format}


class DDLTarget(TargetInterface):
    """Target that exports datasets as DDL (Data Definition Language) statements."""

    def __init__(self, output_file: str, dialect: str = "postgresql"):
        """
        Initialize DDL target.

        Args:
            output_file: Path to output DDL file
            dialect: SQL dialect (postgresql, mysql, sqlite, sqlserver, oracle)
        """
        self.output_file = output_file
        self.dialect = dialect.lower()
        self._validate_dialect()

    def _validate_dialect(self):
        """Validate that the dialect is supported."""
        supported_dialects = ["postgresql", "mysql", "sqlite", "sqlserver", "oracle"]
        if self.dialect not in supported_dialects:
            raise ValueError(
                f"Unsupported dialect: {self.dialect}. Supported: {supported_dialects}"
            )

    def execute(
        self, datahub_ast: DataHubGraph, rdf_graph: Graph = None
    ) -> Dict[str, Any]:
        """Execute DDL target."""
        try:
            logger.info(f"Executing DDL target: {self.output_file}")
            results = self._execute_ddl_export(datahub_ast)
            logger.info(f"DDL target execution completed: {self.output_file}")
            return {
                "success": True,
                "target_type": "ddl",
                "output_file": self.output_file,
                "dialect": self.dialect,
                "results": results,
            }
        except Exception as e:
            logger.error(f"DDL target execution failed: {e}")
            return {"success": False, "target_type": "ddl", "error": str(e)}

    def _execute_ddl_export(self, datahub_ast: DataHubGraph) -> Dict[str, Any]:
        """Execute DDL export operations."""
        logger.info(f"Executing DDL export to {self.output_file}")

        # Auto-detect dialect from datasets if not explicitly set
        detected_dialect = self._detect_dialect_from_datasets(datahub_ast.datasets)
        if detected_dialect and detected_dialect != self.dialect:
            logger.info(
                f"Auto-detected dialect '{detected_dialect}' from dataset platforms, overriding '{self.dialect}'"
            )
            self.dialect = detected_dialect

        results = {
            "strategy": "ddl_export",
            "success": True,
            "files_created": [],
            "output_file": self.output_file,
            "dialect": self.dialect,
            "tables_created": 0,
            "summary": datahub_ast.get_summary(),
        }

        try:
            # Generate DDL for all datasets
            ddl_statements = []

            # Add header comment
            summary = datahub_ast.get_summary()
            dataset_count = summary.get("datasets", 0)
            ddl_statements.append("-- DDL Generated by RDF-Lite")
            ddl_statements.append(f"-- Dialect: {self.dialect.upper()}")
            ddl_statements.append(f"-- Generated from {dataset_count} datasets")
            ddl_statements.append("")

            # Generate DDL for each dataset
            vanilla_datasets = []
            skipped_datasets = []
            for dataset in datahub_ast.datasets:
                if dataset.schema_fields:
                    if dataset.platform:
                        # Use detected dialect for datasets with platforms
                        table_ddl = self._generate_table_ddl(dataset)
                    else:
                        # Use vanilla DDL for datasets without platforms
                        table_ddl = self._generate_vanilla_table_ddl(dataset)
                        vanilla_datasets.append(dataset.name)

                    if table_ddl:
                        ddl_statements.extend(table_ddl)
                        ddl_statements.append("")  # Add blank line between tables
                        results["tables_created"] += 1
                else:
                    # Skip datasets without schema fields
                    skipped_datasets.append(f"{dataset.name} (no schema fields)")

            # Add information about vanilla and skipped datasets
            if vanilla_datasets:
                ddl_statements.append(
                    "-- Datasets exported with vanilla DDL (no platform specified):"
                )
                for vanilla in vanilla_datasets:
                    ddl_statements.append(f"--   - {vanilla}")
                ddl_statements.append("")

            if skipped_datasets:
                ddl_statements.append("-- Skipped datasets (no schema fields):")
                for skipped in skipped_datasets:
                    ddl_statements.append(f"--   - {skipped}")
                ddl_statements.append("")

            # Write DDL to file
            with open(self.output_file, "w") as f:
                f.write("\n".join(ddl_statements))

            results["files_created"].append(self.output_file)

            logger.info(
                f"DDL export complete: {len(results['files_created'])} files created, {results['tables_created']} tables"
            )
            return results

        except Exception as e:
            logger.error(f"DDL export failed: {e}")
            results["success"] = False
            results["error"] = str(e)
            return results

    def _generate_table_ddl(self, dataset: DataHubDataset) -> List[str]:
        """Generate DDL statements for a single dataset."""
        ddl_statements = []

        # Extract table name from dataset name (clean it for SQL)
        table_name = self._clean_identifier(dataset.name)

        # Add table comment
        if dataset.description:
            ddl_statements.append(f"-- Table: {table_name}")
            ddl_statements.append(f"-- Description: {dataset.description}")

        # Start CREATE TABLE statement
        create_statement = f"CREATE TABLE {table_name} ("
        ddl_statements.append(create_statement)

        # Add columns
        column_definitions = []
        for i, field in enumerate(dataset.schema_fields):
            column_def = self._generate_column_definition(
                field, i == len(dataset.schema_fields) - 1
            )
            column_definitions.append(column_def)

        ddl_statements.extend(column_definitions)

        # Close CREATE TABLE statement
        ddl_statements.append(");")

        # Add table comment if supported by dialect
        if dataset.description and self.dialect in ["postgresql", "mysql"]:
            comment = dataset.description.replace("'", "''")
            if self.dialect == "postgresql":
                ddl_statements.append(f"COMMENT ON TABLE {table_name} IS '{comment}';")
            elif self.dialect == "mysql":
                ddl_statements.append(
                    f"ALTER TABLE {table_name} COMMENT = '{comment}';"
                )

        return ddl_statements

    def _generate_vanilla_table_ddl(self, dataset: DataHubDataset) -> List[str]:
        """Generate vanilla DDL statements for a dataset without platform information."""
        ddl_statements = []

        # Extract table name from dataset name (clean it for SQL)
        table_name = self._clean_identifier(dataset.name)

        # Add table comment
        if dataset.description:
            ddl_statements.append(f"-- Table: {table_name}")
            ddl_statements.append(f"-- Description: {dataset.description}")
            ddl_statements.append("-- Note: Vanilla DDL (no platform specified)")

        # Start CREATE TABLE statement
        create_statement = f"CREATE TABLE {table_name} ("
        ddl_statements.append(create_statement)

        # Add columns
        column_definitions = []
        for i, field in enumerate(dataset.schema_fields):
            column_def = self._generate_vanilla_column_definition(
                field, i == len(dataset.schema_fields) - 1
            )
            column_definitions.append(column_def)

        ddl_statements.extend(column_definitions)

        # Close CREATE TABLE statement
        ddl_statements.append(");")

        return ddl_statements

    def _generate_vanilla_column_definition(self, field, is_last: bool) -> str:
        """Generate vanilla column definition using standard SQL types."""
        # Extract field name
        field_name = field.fieldPath if hasattr(field, "fieldPath") else str(field)
        field_name = self._clean_identifier(field_name)

        # Use vanilla SQL types (most compatible)
        field_type = self._map_datahub_type_to_vanilla_sql(field)

        # Extract nullable information
        nullable = True  # Default to nullable
        if hasattr(field, "nullable") and field.nullable is not None:
            nullable = field.nullable

        # Build column definition
        column_def = f"    {field_name} {field_type}"

        # Add NOT NULL constraint if needed
        if not nullable:
            column_def += " NOT NULL"

        # Add comma if not last column
        if not is_last:
            column_def += ","

        return column_def

    def _map_datahub_type_to_vanilla_sql(self, field) -> str:
        """Map DataHub field type to vanilla SQL type (most compatible)."""
        # Extract the actual type from DataHub field
        field_type = "VARCHAR(255)"  # Default fallback

        if hasattr(field, "type") and field.type:
            # DataHub types are typically URNs like "urn:li:dataType:datahub.string"
            type_urn = str(field.type)

            # Map common DataHub types to vanilla SQL types
            if "string" in type_urn.lower():
                field_type = "VARCHAR(255)"
            elif "int" in type_urn.lower() or "integer" in type_urn.lower():
                field_type = "INTEGER"
            elif "float" in type_urn.lower() or "double" in type_urn.lower():
                field_type = "REAL"
            elif "boolean" in type_urn.lower() or "bool" in type_urn.lower():
                field_type = "BOOLEAN"
            elif "date" in type_urn.lower():
                field_type = "DATE"
            elif "timestamp" in type_urn.lower() or "datetime" in type_urn.lower():
                field_type = "TIMESTAMP"
            elif "decimal" in type_urn.lower() or "numeric" in type_urn.lower():
                field_type = "DECIMAL(10,2)"

        return field_type

    def _generate_column_definition(self, field, is_last: bool) -> str:
        """Generate column definition for a schema field."""
        # Extract field name
        field_name = field.fieldPath if hasattr(field, "fieldPath") else str(field)
        field_name = self._clean_identifier(field_name)

        # Extract field type
        field_type = self._map_datahub_type_to_sql(field)

        # Extract nullable information
        nullable = True  # Default to nullable
        if hasattr(field, "nullable") and field.nullable is not None:
            nullable = field.nullable

        # Build column definition
        column_def = f"    {field_name} {field_type}"

        # Add NOT NULL constraint if needed
        if not nullable:
            column_def += " NOT NULL"

        # Add comma if not last column
        if not is_last:
            column_def += ","

        return column_def

    def _map_datahub_type_to_sql(self, field) -> str:
        """Map DataHub field type to SQL type based on dialect."""
        # Extract the actual type from DataHub field
        field_type = "VARCHAR(255)"  # Default fallback

        if hasattr(field, "type") and field.type:
            # DataHub types are typically URNs like "urn:li:dataType:datahub.string"
            type_urn = str(field.type)

            # Map common DataHub types to SQL types
            if "string" in type_urn.lower():
                field_type = self._get_string_type()
            elif "int" in type_urn.lower() or "integer" in type_urn.lower():
                field_type = self._get_integer_type()
            elif "float" in type_urn.lower() or "double" in type_urn.lower():
                field_type = self._get_float_type()
            elif "boolean" in type_urn.lower() or "bool" in type_urn.lower():
                field_type = self._get_boolean_type()
            elif "date" in type_urn.lower():
                field_type = self._get_date_type()
            elif "timestamp" in type_urn.lower() or "datetime" in type_urn.lower():
                field_type = self._get_timestamp_type()
            elif "decimal" in type_urn.lower() or "numeric" in type_urn.lower():
                field_type = self._get_decimal_type()

        return field_type

    def _get_string_type(self) -> str:
        """Get string type for current dialect."""
        type_map = {
            "postgresql": "VARCHAR(255)",
            "mysql": "VARCHAR(255)",
            "sqlite": "TEXT",
            "sqlserver": "NVARCHAR(255)",
            "oracle": "VARCHAR2(255)",
        }
        return type_map.get(self.dialect, "VARCHAR(255)")

    def _get_integer_type(self) -> str:
        """Get integer type for current dialect."""
        type_map = {
            "postgresql": "INTEGER",
            "mysql": "INT",
            "sqlite": "INTEGER",
            "sqlserver": "INT",
            "oracle": "NUMBER(10)",
        }
        return type_map.get(self.dialect, "INTEGER")

    def _get_float_type(self) -> str:
        """Get float type for current dialect."""
        type_map = {
            "postgresql": "REAL",
            "mysql": "FLOAT",
            "sqlite": "REAL",
            "sqlserver": "FLOAT",
            "oracle": "BINARY_FLOAT",
        }
        return type_map.get(self.dialect, "REAL")

    def _get_boolean_type(self) -> str:
        """Get boolean type for current dialect."""
        type_map = {
            "postgresql": "BOOLEAN",
            "mysql": "BOOLEAN",
            "sqlite": "INTEGER",  # SQLite doesn't have native boolean
            "sqlserver": "BIT",
            "oracle": "NUMBER(1)",
        }
        return type_map.get(self.dialect, "BOOLEAN")

    def _get_date_type(self) -> str:
        """Get date type for current dialect."""
        type_map = {
            "postgresql": "DATE",
            "mysql": "DATE",
            "sqlite": "TEXT",  # SQLite stores dates as text
            "sqlserver": "DATE",
            "oracle": "DATE",
        }
        return type_map.get(self.dialect, "DATE")

    def _get_timestamp_type(self) -> str:
        """Get timestamp type for current dialect."""
        type_map = {
            "postgresql": "TIMESTAMP",
            "mysql": "TIMESTAMP",
            "sqlite": "TEXT",  # SQLite stores timestamps as text
            "sqlserver": "DATETIME2",
            "oracle": "TIMESTAMP",
        }
        return type_map.get(self.dialect, "TIMESTAMP")

    def _get_decimal_type(self) -> str:
        """Get decimal type for current dialect."""
        type_map = {
            "postgresql": "DECIMAL(10,2)",
            "mysql": "DECIMAL(10,2)",
            "sqlite": "REAL",
            "sqlserver": "DECIMAL(10,2)",
            "oracle": "NUMBER(10,2)",
        }
        return type_map.get(self.dialect, "DECIMAL(10,2)")

    def _clean_identifier(self, identifier: str) -> str:
        """Clean identifier for SQL compatibility."""
        # Remove or replace invalid characters
        cleaned = identifier.replace(" ", "_").replace("-", "_").replace(".", "_")

        # Remove special characters except underscores
        import re

        cleaned = re.sub(r"[^a-zA-Z0-9_]", "", cleaned)

        # Ensure it starts with letter or underscore
        if cleaned and not cleaned[0].isalpha() and cleaned[0] != "_":
            cleaned = f"_{cleaned}"

        # Handle reserved words by adding prefix
        reserved_words = {
            "postgresql": [
                "select",
                "from",
                "where",
                "insert",
                "update",
                "delete",
                "create",
                "drop",
                "alter",
                "table",
                "index",
                "view",
            ],
            "mysql": [
                "select",
                "from",
                "where",
                "insert",
                "update",
                "delete",
                "create",
                "drop",
                "alter",
                "table",
                "index",
                "view",
            ],
            "sqlite": [
                "select",
                "from",
                "where",
                "insert",
                "update",
                "delete",
                "create",
                "drop",
                "alter",
                "table",
                "index",
                "view",
            ],
            "sqlserver": [
                "select",
                "from",
                "where",
                "insert",
                "update",
                "delete",
                "create",
                "drop",
                "alter",
                "table",
                "index",
                "view",
            ],
            "oracle": [
                "select",
                "from",
                "where",
                "insert",
                "update",
                "delete",
                "create",
                "drop",
                "alter",
                "table",
                "index",
                "view",
            ],
        }

        dialect_reserved = reserved_words.get(
            self.dialect, reserved_words["postgresql"]
        )
        if cleaned.lower() in dialect_reserved:
            cleaned = f"{cleaned}_tbl"

        return cleaned

    def _detect_dialect_from_datasets(
        self, datasets: List[DataHubDataset]
    ) -> Optional[str]:
        """Detect SQL dialect from dataset platforms."""
        if not datasets:
            return None

        # Platform to dialect mapping
        platform_dialect_map = {
            # Traditional databases
            "postgres": "postgresql",
            "postgresql": "postgresql",
            "mysql": "mysql",
            "oracle": "oracle",
            "mssql": "sqlserver",
            "sqlserver": "sqlserver",
            "sqlite": "sqlite",
            "sybase": "sqlserver",  # Sybase uses SQL Server-compatible syntax
            # Cloud data warehouses (use PostgreSQL-compatible syntax)
            "snowflake": "postgresql",  # Snowflake uses PostgreSQL-compatible SQL
            "bigquery": "postgresql",  # BigQuery uses standard SQL (closer to PostgreSQL)
            "redshift": "postgresql",  # Redshift uses PostgreSQL-compatible SQL
            "teradata": "postgresql",  # Teradata SQL is closer to PostgreSQL
            # Regulatory reporting platforms
            "axiom": "sqlserver",  # Axiom uses Sybase/SQL Server-compatible syntax
            # Big data platforms
            "hive": "postgresql",  # Hive SQL is closer to PostgreSQL
            "spark": "postgresql",  # Spark SQL is closer to PostgreSQL
            # Streaming platforms (not applicable for DDL, but included for completeness)
            "kafka": "postgresql",  # Kafka doesn't generate DDL, but if it did, use PostgreSQL
        }

        # Collect platforms from all datasets
        platforms = set()
        for dataset in datasets:
            if dataset.platform:
                platform_name = dataset.platform.lower()
                platforms.add(platform_name)

        if not platforms:
            logger.debug("No platforms found in datasets for dialect detection")
            return None

        # Find the most common dialect among platforms
        dialect_counts = {}
        for platform in platforms:
            # Extract platform name from various formats
            platform_clean = platform.lower()

            # Handle DataHub URN format: urn:li:dataPlatform:platform_name
            if platform_clean.startswith("urn:li:dataplatform:"):
                platform_clean = platform_clean.replace("urn:li:dataplatform:", "")

            # Handle platform names that might include paths or prefixes
            if "/" in platform_clean:
                platform_clean = platform_clean.split("/")[-1]
            if ":" in platform_clean:
                platform_clean = platform_clean.split(":")[-1]

            # Map to dialect
            dialect = platform_dialect_map.get(platform_clean)
            if dialect:
                dialect_counts[dialect] = dialect_counts.get(dialect, 0) + 1
                logger.debug(f"Platform '{platform}' -> dialect '{dialect}'")
            else:
                logger.debug(
                    f"Unknown platform '{platform}', skipping dialect detection"
                )

        if not dialect_counts:
            logger.debug("No recognized platforms found for dialect detection")
            return None

        # Return the most common dialect
        most_common_dialect = max(dialect_counts.items(), key=lambda x: x[1])[0]
        logger.info(
            f"Detected dialect '{most_common_dialect}' from platforms: {list(platforms)}"
        )

        return most_common_dialect

    def get_target_info(self) -> dict:
        """Get DDL target information."""
        return {"type": "ddl", "output_file": self.output_file, "dialect": self.dialect}


class OwnershipExportTarget(TargetInterface):
    """Target that exports ownership information to a file."""

    def __init__(self, output_file: str, format: str = "json"):
        self.output_file = output_file
        self.format = format.lower()

    def execute(
        self, datahub_ast: DataHubGraph, rdf_graph: Graph = None
    ) -> Dict[str, Any]:
        """Export ownership information to a file."""
        results = {
            "success": True,
            "target_type": "ownership_export",
            "output_file": self.output_file,
            "format": self.format,
            "ownership_count": 0,
            "files_created": [],
        }

        try:
            # Get ownership information from the RDF graph
            if not rdf_graph:
                results["success"] = False
                results["error"] = "RDF graph required for ownership export"
                return results

            # Extract ownership information
            # Note: Ownership extraction is a specialized function not in the modular architecture
            # For now, extract using entity module approach
            ownership_info = self._extract_ownership_from_graph(rdf_graph)

            results["ownership_count"] = len(ownership_info)

            # Convert to export format
            if self.format == "json":
                self._export_json(ownership_info, results)
            elif self.format == "csv":
                self._export_csv(ownership_info, results)
            elif self.format == "yaml":
                self._export_yaml(ownership_info, results)
            else:
                results["success"] = False
                results["error"] = f"Unsupported format: {self.format}"
                return results

            logger.info(
                f"Ownership export complete: {results['ownership_count']} ownership records exported to {self.output_file}"
            )
            return results

        except Exception as e:
            logger.error(f"Ownership export failed: {e}")
            results["success"] = False
            results["error"] = str(e)
            return results

    def _extract_ownership_from_graph(self, rdf_graph: Graph) -> List[RDFOwnership]:
        """Extract ownership information from RDF graph."""
        from rdflib import Namespace as RDFNamespace

        DPROD = RDFNamespace("https://ekgf.github.io/dprod/")
        SCHEMA_NS = RDFNamespace("http://schema.org/")

        ownership_list = []

        # Find data owners
        for subject in rdf_graph.subjects(RDF.type, DPROD.DataOwner):
            owner_uri = str(subject)
            owner_label = None
            owner_description = None
            owner_type = "DataOwner"

            for label in rdf_graph.objects(subject, RDFS.label):
                owner_label = str(label)
            for desc in rdf_graph.objects(subject, RDFS.comment):
                owner_description = str(desc)

            # Find what entities this owner owns
            for entity in rdf_graph.subjects(DPROD.dataOwner, subject):
                # Determine entity type from RDF graph
                entity_type = None
                # Check for common entity types
                if (entity, RDF.type, DPROD.DataProduct) in rdf_graph:
                    entity_type = "dataProduct"
                elif (
                    (entity, RDF.type, DCAT.Dataset) in rdf_graph
                    or (entity, RDF.type, VOID.Dataset) in rdf_graph
                    or (entity, RDF.type, DCTERMS.Dataset) in rdf_graph
                    or (entity, RDF.type, SCHEMA_NS.Dataset) in rdf_graph
                ):
                    entity_type = "dataset"

                if not entity_type:
                    raise ValueError(
                        f"Cannot determine entity type for ownership relationship. "
                        f"Owner: {owner_uri}, Entity: {entity}. "
                        f"Entity must have a recognized RDF type (dprod:DataProduct, dcat:Dataset, void:Dataset, dcterms:Dataset, or schema:Dataset)."
                    )

                ownership_list.append(
                    RDFOwnership(
                        owner_uri=owner_uri,
                        owner_type=owner_type,
                        owner_label=owner_label,
                        owner_description=owner_description,
                        entity_uri=str(entity),
                        entity_type=entity_type,
                    )
                )

        return ownership_list

    def _export_json(self, ownership_info: List[RDFOwnership], results: Dict[str, Any]):
        """Export ownership information as JSON."""
        import json

        # Convert to dictionary format
        ownership_data = []
        for ownership in ownership_info:
            ownership_data.append(
                {
                    "owner_uri": ownership.owner_uri,
                    "owner_type": ownership.owner_type,
                    "owner_label": ownership.owner_label,
                    "owner_description": ownership.owner_description,
                    "owner_department": ownership.owner_department,
                    "owner_responsibility": ownership.owner_responsibility,
                    "owner_approval_authority": ownership.owner_approval_authority,
                    "entity_uri": ownership.entity_uri,
                    "entity_type": ownership.entity_type,
                }
            )

        # Write to file
        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "export_timestamp": datetime.datetime.now().isoformat(),
                    "ownership_count": len(ownership_data),
                    "ownership": ownership_data,
                },
                f,
                indent=2,
            )

        results["files_created"].append(self.output_file)

    def _export_csv(self, ownership_info: List[RDFOwnership], results: Dict[str, Any]):
        """Export ownership information as CSV."""
        import csv

        with open(self.output_file, "w", newline="") as f:
            writer = csv.writer(f)

            # Write header
            writer.writerow(
                [
                    "owner_uri",
                    "owner_type",
                    "owner_label",
                    "owner_description",
                    "owner_department",
                    "owner_responsibility",
                    "owner_approval_authority",
                    "entity_uri",
                    "entity_type",
                ]
            )

            # Write data
            for ownership in ownership_info:
                writer.writerow(
                    [
                        ownership.owner_uri,
                        ownership.owner_type,
                        ownership.owner_label or "",
                        ownership.owner_description or "",
                        ownership.owner_department or "",
                        ownership.owner_responsibility or "",
                        ownership.owner_approval_authority or "",
                        ownership.entity_uri,
                        ownership.entity_type,
                    ]
                )

        results["files_created"].append(self.output_file)

    def _export_yaml(self, ownership_info: List[RDFOwnership], results: Dict[str, Any]):
        """Export ownership information as YAML."""
        import yaml

        # Convert to dictionary format
        ownership_data = []
        for ownership in ownership_info:
            ownership_data.append(
                {
                    "owner_uri": ownership.owner_uri,
                    "owner_type": ownership.owner_type,
                    "owner_label": ownership.owner_label,
                    "owner_description": ownership.owner_description,
                    "owner_department": ownership.owner_department,
                    "owner_responsibility": ownership.owner_responsibility,
                    "owner_approval_authority": ownership.owner_approval_authority,
                    "entity_uri": ownership.entity_uri,
                    "entity_type": ownership.entity_type,
                }
            )

        # Write to file
        with open(self.output_file, "w") as f:
            yaml.dump(
                {
                    "export_timestamp": datetime.datetime.now().isoformat(),
                    "ownership_count": len(ownership_data),
                    "ownership": ownership_data,
                },
                f,
                default_flow_style=False,
            )

        results["files_created"].append(self.output_file)

    def get_target_info(self) -> dict:
        """Get information about this target."""
        return {
            "type": "ownership_export",
            "output_file": self.output_file,
            "format": self.format,
        }


class TargetFactory:
    """Factory for creating output targets."""

    @staticmethod
    def create_datahub_target(
        datahub_client: DataHubClient, rdf_graph: Graph = None
    ) -> DataHubTarget:
        """Create a DataHub target."""
        return DataHubTarget(datahub_client, rdf_graph)

    @staticmethod
    def create_pretty_print_target(urn_generator=None) -> PrettyPrintTarget:
        """Create a pretty print target."""
        return PrettyPrintTarget(urn_generator)

    @staticmethod
    def create_file_target(output_file: str, format: str) -> FileTarget:
        """Create a file target."""
        return FileTarget(output_file, format)

    @staticmethod
    def create_ddl_target(output_file: str, dialect: str = "postgresql") -> DDLTarget:
        """Create a DDL target."""
        return DDLTarget(output_file, dialect)

    @staticmethod
    def create_ownership_export_target(
        output_file: str, format: str = "json"
    ) -> OwnershipExportTarget:
        """Create an ownership export target."""
        return OwnershipExportTarget(output_file, format)

    @staticmethod
    def create_target_from_config(target_type: str, **kwargs) -> TargetInterface:
        """Create a target from configuration."""
        if target_type == "datahub":
            datahub_client = kwargs.get("datahub_client")
            rdf_graph = kwargs.get("rdf_graph")
            if not datahub_client:
                raise ValueError("datahub_client required for DataHub target")
            return TargetFactory.create_datahub_target(datahub_client, rdf_graph)

        elif target_type == "pretty_print":
            urn_generator = kwargs.get("urn_generator")
            return TargetFactory.create_pretty_print_target(urn_generator)

        elif target_type == "file":
            output_file = kwargs.get("output_file")
            if not output_file:
                raise ValueError("output_file required for file target")
            format_type = kwargs.get("format")
            if not format_type:
                raise ValueError("format required for file target")
            return TargetFactory.create_file_target(output_file, format_type)

        elif target_type == "ddl":
            output_file = kwargs.get("output_file")
            if not output_file:
                raise ValueError("output_file required for DDL target")
            dialect = kwargs.get("dialect", "postgresql")
            return TargetFactory.create_ddl_target(output_file, dialect)

        else:
            raise ValueError(f"Unknown target type: {target_type}")
