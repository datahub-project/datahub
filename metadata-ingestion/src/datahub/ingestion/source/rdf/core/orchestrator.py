#!/usr/bin/env python3
"""
Orchestrator Pipeline

This module provides the main orchestrator that runs the pipeline:
1. Query Source
2. Transpile to DataHub AST
3. Send to Target

All components are injected via dependency injection.
"""

import logging
from typing import Any, Dict

from datahub.ingestion.source.rdf.core.query_factory import QueryInterface
from datahub.ingestion.source.rdf.core.source_factory import SourceInterface
from datahub.ingestion.source.rdf.core.target_factory import TargetInterface
from datahub.ingestion.source.rdf.core.transpiler import RDFToDataHubTranspiler

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Main orchestrator that runs the RDF to DataHub pipeline.

    This orchestrator uses dependency injection to compose:
    - Source: Where to get RDF data from
    - Query: How to query/filter the RDF data
    - Target: Where to send the results
    - Transpiler: How to convert RDF to DataHub AST
    """

    def __init__(
        self,
        source: SourceInterface,
        query: QueryInterface,
        target: TargetInterface,
        transpiler: RDFToDataHubTranspiler,
    ):
        """
        Initialize the orchestrator with injected dependencies.

        Args:
            source: RDF source (file, folder, server, etc.)
            query: Query to execute against the source
            target: Output target (DataHub, pretty print, file, etc.)
            transpiler: Transpiler (required, no default)
        """
        self.source = source
        self.query = query
        self.target = target
        self.transpiler = transpiler

        logger.debug("Orchestrator initialized with dependency injection")
        logger.debug(f"Source: {source.get_source_info()}")
        logger.debug(f"Query: {query.get_query_info()}")
        logger.debug(f"Target: {target.get_target_info()}")

    def execute(self) -> Dict[str, Any]:
        """
        Execute the complete pipeline.

        Returns:
            Dictionary with execution results
        """
        try:
            logger.debug("Starting orchestrator pipeline execution")

            # Step 1: Query Source
            logger.debug("Step 1: Querying source...")
            source_graph = self.source.get_graph()
            logger.debug(f"Source loaded: {len(source_graph)} triples")

            # Step 2: Execute Query
            logger.debug("Step 2: Executing query...")
            query_result_graph = self.query.execute(source_graph)
            logger.debug(f"Query executed: {len(query_result_graph)} triples in result")

            # Step 3: Transpile to DataHub AST
            logger.debug("Step 3: Transpiling to DataHub AST...")
            datahub_ast = self.transpiler.get_datahub_ast(query_result_graph)
            # Use get_summary() for dynamic entity counts
            summary = datahub_ast.get_summary()
            summary_str = ", ".join(
                [f"{count} {name}" for name, count in summary.items()]
            )
            logger.debug(f"DataHub AST created: {summary_str}")

            # Step 4: Send to Target
            logger.debug("Step 4: Sending to target...")
            target_results = self.target.execute(datahub_ast, query_result_graph)
            logger.debug(
                f"Target execution completed: {target_results.get('success', False)}"
            )

            # Compile final results
            results = {
                "success": target_results.get("success", False),
                "pipeline": {
                    "source": self.source.get_source_info(),
                    "query": self.query.get_query_info(),
                    "target": self.target.get_target_info(),
                },
                "execution": {
                    "source_triples": len(source_graph),
                    "query_result_triples": len(query_result_graph),
                    "datahub_ast": datahub_ast.get_summary(),  # Dynamic summary from registry
                },
                "target_results": target_results,
            }

            if target_results.get("success"):
                logger.info("✅ Orchestrator pipeline execution completed successfully")
            else:
                logger.error("❌ Orchestrator pipeline execution failed")

            return results

        except Exception as e:
            logger.error(f"Orchestrator pipeline execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "pipeline": {
                    "source": self.source.get_source_info(),
                    "query": self.query.get_query_info(),
                    "target": self.target.get_target_info(),
                },
            }

    def validate(self) -> Dict[str, Any]:
        """
        Validate the pipeline configuration without executing.

        Returns:
            Dictionary with validation results
        """
        try:
            logger.info("Validating orchestrator pipeline configuration")

            validation_results = {
                "valid": True,
                "source": self.source.get_source_info(),
                "query": self.query.get_query_info(),
                "target": self.target.get_target_info(),
                "transpiler": {"environment": self.transpiler.environment},
            }

            # Validate source
            try:
                source_info = self.source.get_source_info()
                if not source_info:
                    validation_results["valid"] = False
                    validation_results["source_error"] = "Source info unavailable"
            except Exception as e:
                validation_results["valid"] = False
                validation_results["source_error"] = str(e)

            # Validate query
            try:
                query_info = self.query.get_query_info()
                if not query_info:
                    validation_results["valid"] = False
                    validation_results["query_error"] = "Query info unavailable"
            except Exception as e:
                validation_results["valid"] = False
                validation_results["query_error"] = str(e)

            # Validate target
            try:
                target_info = self.target.get_target_info()
                if not target_info:
                    validation_results["valid"] = False
                    validation_results["target_error"] = "Target info unavailable"
            except Exception as e:
                validation_results["valid"] = False
                validation_results["target_error"] = str(e)

            if validation_results["valid"]:
                logger.info("✅ Pipeline configuration validation passed")
            else:
                logger.error("❌ Pipeline configuration validation failed")

            return validation_results

        except Exception as e:
            logger.error(f"Pipeline validation failed: {e}")
            return {"valid": False, "error": str(e)}

    def get_pipeline_info(self) -> Dict[str, Any]:
        """Get information about the current pipeline configuration."""
        return {
            "source": self.source.get_source_info(),
            "query": self.query.get_query_info(),
            "target": self.target.get_target_info(),
            "transpiler": {"environment": self.transpiler.environment},
        }
