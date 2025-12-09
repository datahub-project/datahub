#!/usr/bin/env python3
"""
Simple RDF Graph Loader

Replaces the SourceFactory pattern with a straightforward function.
Supports loading RDF from files, folders, URLs, and glob patterns.
"""

import glob
import logging
from pathlib import Path
from typing import List, Optional

from rdflib import Graph

logger = logging.getLogger(__name__)


def load_rdf_graph(
    source: str,
    format: Optional[str] = None,
    recursive: bool = True,
    file_extensions: Optional[List[str]] = None,
) -> Graph:
    """
    Load an RDF graph from various sources.

    Args:
        source: File path, folder path, URL, comma-separated files, or glob pattern
        format: RDF format (auto-detected from extension if not specified)
        recursive: Enable recursive folder processing (default: True)
        file_extensions: File extensions to process when source is a folder

    Returns:
        RDFLib Graph containing the loaded RDF data

    Raises:
        FileNotFoundError: If source file/folder doesn't exist
        ValueError: If source cannot be determined
    """
    if file_extensions is None:
        file_extensions = [".ttl", ".turtle", ".rdf", ".xml", ".jsonld", ".n3", ".nt"]

    graph = Graph()

    # Check if it's a server URL
    if source.startswith(("http://", "https://")):
        format_str = format or "turtle"
        try:
            graph.parse(source, format=format_str)
            logger.info(f"Loaded {len(graph)} triples from {source}")
            return graph
        except Exception as e:
            logger.error(f"Failed to load from {source}: {e}")
            raise

    # Check if it's comma-separated files
    if "," in source:
        files = [f.strip() for f in source.split(",")]
        return _load_multiple_files(graph, files, format)

    # Check if it's a folder (before glob, since glob can match directories)
    path = Path(source)
    if path.exists() and path.is_dir():
        return _load_folder(graph, path, recursive, file_extensions, format)

    # Check if it's a single file (before glob)
    if path.exists() and path.is_file():
        return _load_single_file(graph, source, format)

    # Try glob pattern (only if not a directory or file)
    matching_files = glob.glob(source)
    if matching_files:
        # Filter out directories from glob results
        matching_files = [f for f in matching_files if Path(f).is_file()]
        if not matching_files:
            raise ValueError(
                f"Glob pattern '{source}' matched only directories, no files"
            )
        if len(matching_files) == 1:
            return _load_single_file(graph, matching_files[0], format)
        else:
            return _load_multiple_files(graph, matching_files, format)

    raise ValueError(f"Source not found or invalid: {source}")


def _load_single_file(
    graph: Graph, file_path: str, format: Optional[str] = None
) -> Graph:
    """Load RDF from a single file."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    format_str = format or _detect_format_from_extension(path.suffix)
    try:
        graph.parse(str(file_path), format=format_str)
        logger.info(f"Loaded {len(graph)} triples from {file_path}")
        return graph
    except Exception as e:
        logger.error(f"Failed to load file {file_path}: {e}")
        raise


def _load_multiple_files(
    graph: Graph, file_paths: List[str], format: Optional[str] = None
) -> Graph:
    """Load RDF from multiple files."""
    files_loaded = 0
    for file_path in file_paths:
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"File not found: {file_path}, skipping")
            continue

        format_str = format or _detect_format_from_extension(path.suffix)
        try:
            graph.parse(str(file_path), format=format_str)
            files_loaded += 1
            logger.debug(f"Loaded {file_path}")
        except Exception as e:
            logger.warning(f"Failed to load {file_path}: {e}")

    logger.info(f"Loaded {len(graph)} triples from {files_loaded} files")
    return graph


def _load_folder(
    graph: Graph,
    folder_path: Path,
    recursive: bool,
    file_extensions: List[str],
    format: Optional[str] = None,
) -> Graph:
    """Load RDF from all files in a folder."""
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    if not folder_path.is_dir():
        raise ValueError(f"Path is not a directory: {folder_path}")

    files_loaded = 0
    pattern = "**/*" if recursive else "*"

    for file_path in folder_path.glob(pattern):
        if file_path.is_file() and file_path.suffix.lower() in [
            ext.lower() for ext in file_extensions
        ]:
            format_str = format or _detect_format_from_extension(file_path.suffix)
            try:
                graph.parse(str(file_path), format=format_str)
                files_loaded += 1
                logger.debug(f"Loaded {file_path}")
            except Exception as e:
                logger.warning(f"Failed to load {file_path}: {e}")

    logger.info(
        f"Loaded {len(graph)} triples from {files_loaded} files in {folder_path}"
    )
    return graph


def _detect_format_from_extension(extension: str) -> str:
    """Detect RDF format from file extension."""
    format_map = {
        ".ttl": "turtle",
        ".turtle": "turtle",
        ".rdf": "xml",
        ".xml": "xml",
        ".jsonld": "json-ld",
        ".n3": "n3",
        ".nt": "nt",
    }
    return format_map.get(extension.lower(), "turtle")
