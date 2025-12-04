#!/usr/bin/env python3
"""
Source Factory Interface

This module provides a factory interface for creating different types of RDF sources.
Supports file sources, folder sources, and server sources with dependency injection.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from rdflib import Graph

logger = logging.getLogger(__name__)


class SourceInterface(ABC):
    """Abstract interface for RDF sources."""

    @abstractmethod
    def get_graph(self) -> Graph:
        """Get the RDF graph from this source."""
        pass

    @abstractmethod
    def get_source_info(self) -> dict:
        """Get information about this source."""
        pass


class FileSource(SourceInterface):
    """RDF source that loads from a single file."""

    def __init__(self, file_path: str, format: str = "turtle"):
        self.file_path = Path(file_path)
        self.format = format

        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

    def get_graph(self) -> Graph:
        """Load RDF graph from file."""
        graph = Graph()
        try:
            graph.parse(str(self.file_path), format=self.format)
            logger.info(f"Loaded {len(graph)} triples from {self.file_path}")
            return graph
        except Exception as e:
            logger.error(f"Failed to load file {self.file_path}: {e}")
            raise

    def get_source_info(self) -> dict:
        """Get file source information."""
        return {
            "type": "file",
            "path": str(self.file_path),
            "format": self.format,
            "size": self.file_path.stat().st_size if self.file_path.exists() else 0,
        }


class FolderSource(SourceInterface):
    """RDF source that loads from a folder with optional recursion."""

    def __init__(
        self,
        folder_path: str,
        recursive: bool = True,
        file_extensions: List[str] | None = None,
    ):
        self.folder_path = Path(folder_path)
        self.recursive = recursive
        self.file_extensions = file_extensions or [
            ".ttl",
            ".turtle",
            ".rdf",
            ".xml",
            ".jsonld",
        ]

        if not self.folder_path.exists():
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        if not self.folder_path.is_dir():
            raise ValueError(f"Path is not a directory: {folder_path}")

    def get_graph(self) -> Graph:
        """Load RDF graph from all files in folder."""
        graph = Graph()
        files_loaded = 0

        # Find all matching files
        pattern = "**/*" if self.recursive else "*"
        for file_path in self.folder_path.glob(pattern):
            if file_path.is_file() and file_path.suffix.lower() in self.file_extensions:
                try:
                    # Determine format from extension
                    format_map = {
                        ".ttl": "turtle",
                        ".turtle": "turtle",
                        ".rdf": "xml",
                        ".xml": "xml",
                        ".jsonld": "json-ld",
                    }
                    format_type = format_map.get(file_path.suffix.lower(), "turtle")

                    graph.parse(str(file_path), format=format_type)
                    files_loaded += 1
                    logger.debug(f"Loaded {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to load {file_path}: {e}")

        logger.info(
            f"Loaded {len(graph)} triples from {files_loaded} files in {self.folder_path}"
        )
        return graph

    def get_source_info(self) -> dict:
        """Get folder source information."""
        # Count files
        pattern = "**/*" if self.recursive else "*"
        files = [
            f
            for f in self.folder_path.glob(pattern)
            if f.is_file() and f.suffix.lower() in self.file_extensions
        ]

        return {
            "type": "folder",
            "path": str(self.folder_path),
            "recursive": self.recursive,
            "file_extensions": self.file_extensions,
            "file_count": len(files),
        }


class ServerSource(SourceInterface):
    """RDF source that loads from a remote server."""

    def __init__(self, url: str, format: str = "turtle"):
        self.url = url
        self.format = format

    def get_graph(self) -> Graph:
        """Load RDF graph from remote server."""
        graph = Graph()
        try:
            graph.parse(self.url, format=self.format)
            logger.info(f"Loaded {len(graph)} triples from {self.url}")
            return graph
        except Exception as e:
            logger.error(f"Failed to load from {self.url}: {e}")
            raise

    def get_source_info(self) -> dict:
        """Get server source information."""
        return {"type": "server", "url": self.url, "format": self.format}


class SourceFactory:
    """Factory for creating RDF sources."""

    @staticmethod
    def create_file_source(file_path: str, format: str = "turtle") -> FileSource:
        """Create a file source."""
        return FileSource(file_path, format)

    @staticmethod
    def create_folder_source(
        folder_path: str, recursive: bool = True, file_extensions: List[str] = None
    ) -> FolderSource:
        """Create a folder source."""
        return FolderSource(folder_path, recursive, file_extensions)

    @staticmethod
    def create_server_source(url: str, format: str = "turtle") -> ServerSource:
        """Create a server source."""
        return ServerSource(url, format)

    @staticmethod
    def create_multi_file_source(
        file_paths: List[str], format: str = "turtle"
    ) -> SourceInterface:
        """Create a source that loads from multiple files."""
        if len(file_paths) == 1:
            return SourceFactory.create_file_source(file_paths[0], format)
        else:
            # For multiple files, we'll create a custom source
            return MultiFileSource(file_paths, format)


class MultiFileSource(SourceInterface):
    """RDF source that loads from multiple files."""

    def __init__(self, file_paths: List[str], format: str = "turtle"):
        self.file_paths = [Path(p) for p in file_paths]
        self.format = format

        # Validate all files exist
        for file_path in self.file_paths:
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

    def get_graph(self) -> Graph:
        """Load RDF graph from multiple files."""
        graph = Graph()
        files_loaded = 0

        for file_path in self.file_paths:
            try:
                graph.parse(str(file_path), format=self.format)
                files_loaded += 1
                logger.info(f"Loaded {file_path}")
            except Exception as e:
                logger.warning(f"Failed to load {file_path}: {e}")

        logger.info(f"Loaded {len(graph)} triples from {files_loaded} files")
        return graph

    def get_source_info(self) -> dict:
        """Get multi-file source information."""
        return {
            "type": "multi_file",
            "paths": [str(p) for p in self.file_paths],
            "format": self.format,
            "file_count": len(self.file_paths),
        }
