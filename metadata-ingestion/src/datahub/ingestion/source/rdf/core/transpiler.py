#!/usr/bin/env python3
"""
RDF to DataHub Transpiler

This module provides the main orchestrator for the RDF to DataHub transpiler.
It uses the modular entity-based architecture via RDFFacade.

The transpiler now delegates to the facade for all processing.
"""

import logging
from typing import List, Optional

from rdflib import Graph

from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.dialects import RDFDialect

logger = logging.getLogger(__name__)


class RDFToDataHubTranspiler:
    """
    Main orchestrator for the RDF to DataHub transpiler.

    This class uses the modular entity-based architecture via RDFFacade.
    """

    def __init__(
        self,
        environment: str,
        forced_dialect: Optional[RDFDialect] = None,
        export_only: Optional[List[str]] = None,
        skip_export: Optional[List[str]] = None,
    ):
        """
        Initialize the transpiler.

        Args:
            environment: DataHub environment (PROD, DEV, TEST)
            forced_dialect: Optional dialect to force instead of auto-detection
            export_only: Optional list of entity types to export (glossary, datasets, data_products, lineage, properties)
            skip_export: Optional list of entity types to skip exporting
        """
        self.environment = environment
        self.export_only = export_only
        self.skip_export = skip_export
        self.forced_dialect = forced_dialect

        # Use facade for all processing
        from datahub.ingestion.source.rdf.facade import RDFFacade

        self.facade = RDFFacade()

        self.logger = logging.getLogger(__name__)
        self.logger.debug(
            f"Initialized RDF to DataHub transpiler for environment: {environment}"
        )

    def get_datahub_ast(self, rdf_graph: Graph) -> DataHubGraph:
        """
        Get the DataHub AST representation without executing output.

        This is useful for debugging and testing the conversion phases.

        Args:
            rdf_graph: RDFLib Graph containing the RDF data

        Returns:
            DataHubGraph: Internal DataHub AST representation
        """
        self.logger.debug("Converting RDF Graph to DataHub AST using modular facade")
        return self.facade.get_datahub_graph(
            rdf_graph,
            environment=self.environment,
            export_only=self.export_only or [],
            skip_export=self.skip_export or [],
        )
