#!/usr/bin/env python3
"""
DataHub Ingestion Source for RDF.

This module provides a DataHub ingestion source that allows RDF to be used
as a native DataHub ingestion plugin.
"""

from datahub.ingestion.source.rdf.ingestion.rdf_source import (
    RDFSource,
    RDFSourceConfig,
    RDFSourceReport,
)

__all__ = ["RDFSource", "RDFSourceConfig", "RDFSourceReport"]
