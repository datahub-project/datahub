"""
Dataplex Explorer Package

A comprehensive tool for extracting Google Cloud Dataplex metadata including:
- Lakes, Zones, Assets, Entities
- Entry Groups and Entries
- Data Quality scans and results
- Data Profiling scans and results
"""

from datahub.ingestion.source.dataplex.example_code.dataplex_client import (
    DataplexClient,
)
from datahub.ingestion.source.dataplex.example_code.dataplex_writer import (
    write_extraction_log,
)

__version__ = "2.0.0"
__all__ = ["DataplexClient", "write_extraction_log"]
