#!/usr/bin/env python3
"""
DataHub RDF CLI

A simple command-line interface for processing RDF files into DataHub entities
using the transpiler architecture.
"""

import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

from datahub.ingestion.source.rdf.scripts.datahub_rdf import main  # noqa: E402

if __name__ == "__main__":
    exit(main())
