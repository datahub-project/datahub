#!/usr/bin/env python3
"""
RDF Dialects package.

This package contains different RDF modeling dialect implementations
for handling various approaches to RDF modeling (BCBS239, FIBO, etc.).
"""

from datahub.ingestion.source.rdf.dialects.base import RDFDialect, RDFDialectInterface
from datahub.ingestion.source.rdf.dialects.bcbs239 import DefaultDialect
from datahub.ingestion.source.rdf.dialects.fibo import FIBODialect
from datahub.ingestion.source.rdf.dialects.generic import GenericDialect
from datahub.ingestion.source.rdf.dialects.router import DialectRouter

__all__ = [
    "RDFDialect",
    "RDFDialectInterface",
    "DefaultDialect",
    "FIBODialect",
    "GenericDialect",
    "DialectRouter",
]
