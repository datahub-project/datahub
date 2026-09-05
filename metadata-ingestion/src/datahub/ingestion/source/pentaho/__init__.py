"""
Pentaho Data Integration Source for DataHub

This package extracts metadata and lineage information from Pentaho Kettle files (.ktr and .kjb).
"""

from datahub.ingestion.source.pentaho.pentaho import PentahoSource

__all__ = ["PentahoSource"]
