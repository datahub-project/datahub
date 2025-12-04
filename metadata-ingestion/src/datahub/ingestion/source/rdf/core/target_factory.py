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
from typing import Any, Dict, List

from rdflib import Graph
from rdflib.namespace import DCAT, DCTERMS, RDF, RDFS, VOID

from datahub.ingestion.source.rdf.core.ast import DataHubGraph, RDFOwnership

# DataHubClient removed - CLI-only, not used by ingestion source
from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
    DataHubGlossaryTerm,
)

logger = logging.getLogger(__name__)


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
        # Dataset export removed for MVP

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
            # Write glossary terms (datasets removed for MVP)
            with open(self.output_file, "w") as f:
                json.dump(
                    {
                        "glossary_terms": [
                            self._term_to_dict(t) for t in datahub_ast.glossary_terms
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

    # Dataset export removed for MVP

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

    # Structured property export removed for MVP

    def get_target_info(self) -> dict:
        """Get file target information."""
        return {"type": "file", "output_file": self.output_file, "format": self.format}


# DDLTarget removed for MVP - dataset export not supported


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
    def create_pretty_print_target(urn_generator=None) -> PrettyPrintTarget:
        """Create a pretty print target."""
        return PrettyPrintTarget(urn_generator)

    @staticmethod
    def create_file_target(output_file: str, format: str) -> FileTarget:
        """Create a file target."""
        return FileTarget(output_file, format)

    @staticmethod
    def create_ddl_target(output_file: str, dialect: str = "postgresql"):
        """Create a DDL target - not supported in MVP (dataset export removed)."""
        raise ValueError(
            "DDL export is not supported in MVP. Dataset export has been removed."
        )

    @staticmethod
    def create_ownership_export_target(
        output_file: str, format: str = "json"
    ) -> OwnershipExportTarget:
        """Create an ownership export target."""
        return OwnershipExportTarget(output_file, format)

    @staticmethod
    def create_target_from_config(target_type: str, **kwargs) -> TargetInterface:
        """Create a target from configuration."""
        if target_type == "pretty_print":
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
            raise ValueError(
                "DDL export is not supported in MVP. Dataset export has been removed."
            )

        else:
            raise ValueError(f"Unknown target type: {target_type}")
