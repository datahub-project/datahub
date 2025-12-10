#!/usr/bin/env python3
"""
Simple RDF Graph Loader

Supports loading RDF from files, folders, URLs, and glob patterns.
"""

import glob
import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Set

import requests
from rdflib import Graph
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Security and performance constants
DEFAULT_URL_TIMEOUT = 30  # seconds
DEFAULT_MAX_URL_SIZE = 100 * 1024 * 1024  # 100MB
DEFAULT_MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB
MAX_GLOB_RESULTS = 1000  # Maximum files to process from glob pattern
# Threshold for using temp file vs memory for URL downloads (10MB)
URL_MEMORY_THRESHOLD = 10 * 1024 * 1024

# Valid RDFLib format strings
VALID_RDF_FORMATS: Set[str] = {
    "turtle",
    "ttl",
    "xml",
    "rdf",
    "rdfxml",
    "json-ld",
    "jsonld",
    "n3",
    "nt",
    "ntriples",
    "nquads",
    "trix",
    "trig",
    "hext",
}


def _validate_format(format_str: str) -> str:
    """
    Validate and normalize RDF format string.

    Args:
        format_str: Format string to validate

    Returns:
        Normalized format string

    Raises:
        ValueError: If format is not supported
    """
    format_lower = format_str.lower()

    # Normalize common aliases
    format_aliases = {
        "ttl": "turtle",
        "rdf": "xml",
        "rdfxml": "xml",
        "jsonld": "json-ld",
        "ntriples": "nt",
    }

    normalized = format_aliases.get(format_lower, format_lower)

    if normalized not in VALID_RDF_FORMATS:
        raise ValueError(
            f"Invalid RDF format: {format_str}. "
            f"Supported formats: {', '.join(sorted(VALID_RDF_FORMATS))}"
        )

    return normalized


def load_rdf_graph(
    source: str,
    format: Optional[str] = None,
    recursive: bool = True,
    file_extensions: Optional[List[str]] = None,
    url_timeout: int = DEFAULT_URL_TIMEOUT,
    max_url_size: int = DEFAULT_MAX_URL_SIZE,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    base_dir: Optional[str] = None,
) -> Graph:
    """
    Load an RDF graph from various sources.

    Args:
        source: File path, folder path, URL, comma-separated files, or glob pattern
        format: RDF format (auto-detected from extension if not specified)
        recursive: Enable recursive folder processing (default: True)
        file_extensions: File extensions to process when source is a folder
        url_timeout: Timeout in seconds for URL requests (default: 30)
        max_url_size: Maximum size in bytes for URL downloads (default: 100MB)
        max_file_size: Maximum size in bytes for local files (default: 500MB)
        base_dir: Optional base directory for path traversal protection.
                 If provided, all file paths must be within this directory.

    Returns:
        RDFLib Graph containing the loaded RDF data

    Raises:
        FileNotFoundError: If source file/folder doesn't exist
        ValueError: If source cannot be determined, exceeds size limits,
                   format is invalid, or path traversal detected
        requests.RequestException: If URL request fails
    """
    if file_extensions is None:
        file_extensions = [".ttl", ".turtle", ".rdf", ".xml", ".jsonld", ".n3", ".nt"]

    # Validate format if provided
    if format:
        format = _validate_format(format)

    graph = Graph()

    # Check if it's a server URL
    if source.startswith(("http://", "https://")):
        url_format = format or "turtle"
        if format:
            url_format = _validate_format(format)
        return _load_from_url(source, url_format, url_timeout, max_url_size)

    # Check if it's comma-separated files
    if "," in source:
        files = [f.strip() for f in source.split(",")]
        return _load_multiple_files(graph, files, format, max_file_size, base_dir)

    # Check if it's a folder (before glob, since glob can match directories)
    path = Path(source)
    if path.exists() and path.is_dir():
        return _load_folder(
            graph, path, recursive, file_extensions, format, max_file_size, base_dir
        )

    # Check if it's a single file (before glob)
    if path.exists() and path.is_file():
        return _load_single_file(graph, source, format, max_file_size, base_dir)

    # Try glob pattern (only if not a directory or file)
    # Security: Limit glob results to prevent resource exhaustion
    MAX_GLOB_RESULTS = 1000
    matching_files = glob.glob(source)
    if matching_files:
        # Filter out directories from glob results
        matching_files = [f for f in matching_files if Path(f).is_file()]
        if not matching_files:
            raise ValueError(
                f"Glob pattern '{source}' matched only directories, no files"
            )
        if len(matching_files) > MAX_GLOB_RESULTS:
            logger.warning(
                f"Glob pattern '{source}' matched {len(matching_files)} files, "
                f"limiting to first {MAX_GLOB_RESULTS} files"
            )
            matching_files = matching_files[:MAX_GLOB_RESULTS]
        if len(matching_files) == 1:
            return _load_single_file(
                graph, matching_files[0], format, max_file_size, base_dir
            )
        else:
            return _load_multiple_files(
                graph, matching_files, format, max_file_size, base_dir
            )

    raise ValueError(f"Source not found or invalid: {source}")


def _load_from_url(
    url: str,
    format_str: str,
    timeout: int = DEFAULT_URL_TIMEOUT,
    max_size: int = DEFAULT_MAX_URL_SIZE,
) -> Graph:
    """
    Load RDF from a URL with security controls.

    Args:
        url: URL to load from
        format_str: RDF format
        timeout: Request timeout in seconds
        max_size: Maximum file size in bytes

    Returns:
        RDFLib Graph containing the loaded RDF data

    Raises:
        requests.RequestException: If request fails
        ValueError: If file exceeds size limit
    """
    graph = Graph()

    try:
        # Create session with retry strategy
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Prevent infinite redirect loops
        session.max_redirects = 10

        # Make request with timeout
        response = session.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        # Check content length if available
        content_length = None
        if "content-length" in response.headers:
            content_length = int(response.headers["content-length"])
            if content_length > max_size:
                raise ValueError(
                    f"URL content too large: {content_length} bytes "
                    f"(max: {max_size} bytes). Consider downloading and processing locally."
                )

        # For large files, use temporary file to avoid memory issues
        # For smaller files, use in-memory buffer for better performance
        use_temp_file = content_length and content_length > URL_MEMORY_THRESHOLD

        if use_temp_file:
            # Stream to temporary file for large downloads
            with tempfile.NamedTemporaryFile(delete=False, suffix=".rdf") as tmp_file:
                tmp_path = Path(tmp_file.name)
                try:
                    bytes_downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # Filter out keep-alive chunks
                            bytes_downloaded += len(chunk)
                            if bytes_downloaded > max_size:
                                raise ValueError(
                                    f"URL content exceeds size limit: {bytes_downloaded} bytes "
                                    f"(max: {max_size} bytes). Consider downloading and processing locally."
                                )
                            tmp_file.write(chunk)

                    tmp_file.flush()
                    # Parse from temporary file
                    graph.parse(str(tmp_path), format=format_str)
                    logger.info(
                        f"Loaded {len(graph)} triples from {url} (via temp file)"
                    )
                    return graph
                finally:
                    # Clean up temporary file
                    try:
                        tmp_path.unlink()
                    except Exception as e:
                        logger.warning(f"Failed to delete temp file {tmp_path}: {e}")
        else:
            # Read content with size limit (in-memory for smaller files)
            content = b""
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    content += chunk
                    if len(content) > max_size:
                        raise ValueError(
                            f"URL content exceeds size limit: {len(content)} bytes "
                            f"(max: {max_size} bytes). Consider downloading and processing locally."
                        )

            # Parse RDF from content
            graph.parse(data=content, format=format_str)
            logger.info(f"Loaded {len(graph)} triples from {url}")
            return graph

    except requests.Timeout:
        raise ValueError(
            f"Request to {url} timed out after {timeout} seconds. "
            "The server may be slow or unreachable."
        ) from None
    except requests.RequestException as e:
        raise ValueError(
            f"Failed to load from URL {url}: {e}. "
            "Please verify the URL is accessible and try again."
        ) from e


def _load_single_file(
    graph: Graph,
    file_path: str,
    format: Optional[str] = None,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    base_dir: Optional[str] = None,
) -> Graph:
    """
    Load RDF from a single file with size validation.

    Args:
        graph: RDFLib Graph to populate
        file_path: Path to RDF file
        format: RDF format (auto-detected if not specified)
        max_file_size: Maximum file size in bytes
        base_dir: Optional base directory for path traversal protection

    Returns:
        RDFLib Graph containing the loaded RDF data

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file exceeds size limit or path traversal detected
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Check file size
    file_size = path.stat().st_size
    if file_size > max_file_size:
        raise ValueError(
            f"File too large: {file_size} bytes (max: {max_file_size} bytes). "
            "Consider splitting the file or using export filters."
        )

    # Validate path security (prevent path traversal)
    resolved_path = path.resolve()
    if base_dir:
        # Enforce base directory restriction
        base_path = Path(base_dir).resolve()
        try:
            # Python 3.9+ has is_relative_to, but we need to support older versions
            # Check if resolved_path is within base_path
            resolved_str = str(resolved_path)
            base_str = str(base_path)
            if not resolved_str.startswith(base_str):
                raise ValueError(
                    f"Path traversal detected: {file_path} resolves to {resolved_path}, "
                    f"which is outside the allowed base directory {base_dir}. "
                    "This is blocked for security reasons."
                )
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            logger.warning(f"Could not validate path against base_dir: {e}")
    else:
        # Basic check: warn if path resolves outside current directory
        try:
            if ".." in str(resolved_path) and not str(resolved_path).startswith(
                str(Path.cwd())
            ):
                logger.warning(
                    f"Path resolved outside current directory: {resolved_path}. "
                    "This may be a security concern if processing untrusted input. "
                    "Consider setting base_dir parameter for stricter security."
                )
        except Exception as e:
            logger.warning(f"Could not resolve path {file_path}: {e}")

    format_str = format or _detect_format_from_extension(path.suffix)
    try:
        # Handle potential race condition: file might be deleted between
        # existence check and read
        graph.parse(str(file_path), format=format_str)
        logger.info(f"Loaded {len(graph)} triples from {file_path}")
        return graph
    except FileNotFoundError:
        # File was deleted between check and read
        raise FileNotFoundError(
            f"File not found: {file_path} (may have been deleted)"
        ) from None
    except Exception as e:
        logger.error(f"Failed to load file {file_path}: {e}")
        raise


def _load_multiple_files(
    graph: Graph,
    file_paths: List[str],
    format: Optional[str] = None,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    base_dir: Optional[str] = None,
) -> Graph:
    """
    Load RDF from multiple files.

    Args:
        graph: RDFLib Graph to populate
        file_paths: List of file paths to load
        format: RDF format (auto-detected if not specified)
        max_file_size: Maximum file size in bytes
        base_dir: Optional base directory for path traversal protection

    Returns:
        RDFLib Graph containing the loaded RDF data
    """
    files_loaded = 0
    for file_path in file_paths:
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"File not found: {file_path}, skipping")
            continue

        # Check file size
        try:
            file_size = path.stat().st_size
            if file_size > max_file_size:
                logger.warning(
                    f"File {file_path} too large ({file_size} bytes), skipping"
                )
                continue
        except Exception as e:
            logger.warning(f"Could not check size of {file_path}: {e}, skipping")
            continue

        # Validate path if base_dir is set
        if base_dir:
            try:
                resolved_path = path.resolve()
                base_path = Path(base_dir).resolve()
                resolved_str = str(resolved_path)
                base_str = str(base_path)
                if not resolved_str.startswith(base_str):
                    logger.warning(
                        f"Skipping {file_path}: path traversal detected "
                        f"(resolves to {resolved_path}, outside {base_dir})"
                    )
                    continue
            except Exception as e:
                logger.warning(f"Could not validate path {file_path}: {e}, skipping")
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
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    base_dir: Optional[str] = None,
) -> Graph:
    """
    Load RDF from all files in a folder.

    Args:
        graph: RDFLib Graph to populate
        folder_path: Path to folder
        recursive: Enable recursive processing
        file_extensions: File extensions to process
        format: RDF format (auto-detected if not specified)
        max_file_size: Maximum file size in bytes
        base_dir: Optional base directory for path traversal protection

    Returns:
        RDFLib Graph containing the loaded RDF data

    Raises:
        FileNotFoundError: If folder doesn't exist
        ValueError: If path is not a directory or path traversal detected
    """
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    if not folder_path.is_dir():
        raise ValueError(f"Path is not a directory: {folder_path}")

    # Validate folder path if base_dir is set
    if base_dir:
        resolved_folder = folder_path.resolve()
        base_path = Path(base_dir).resolve()
        resolved_str = str(resolved_folder)
        base_str = str(base_path)
        if not resolved_str.startswith(base_str):
            raise ValueError(
                f"Folder path traversal detected: {folder_path} resolves to {resolved_folder}, "
                f"which is outside the allowed base directory {base_dir}. "
                "This is blocked for security reasons."
            )

    files_loaded = 0
    pattern = "**/*" if recursive else "*"

    for file_path in folder_path.glob(pattern):
        if file_path.is_file() and file_path.suffix.lower() in [
            ext.lower() for ext in file_extensions
        ]:
            # Check file size
            try:
                file_size = file_path.stat().st_size
                if file_size > max_file_size:
                    logger.warning(
                        f"File {file_path} too large ({file_size} bytes), skipping"
                    )
                    continue
            except Exception as e:
                logger.warning(f"Could not check size of {file_path}: {e}, skipping")
                continue

            # Additional path validation for files in folder (redundant but safe)
            if base_dir:
                try:
                    resolved_file = file_path.resolve()
                    base_path = Path(base_dir).resolve()
                    resolved_str = str(resolved_file)
                    base_str = str(base_path)
                    if not resolved_str.startswith(base_str):
                        logger.warning(
                            f"Skipping {file_path}: path traversal detected "
                            f"(resolves to {resolved_file}, outside {base_dir})"
                        )
                        continue
                except Exception as e:
                    logger.warning(
                        f"Could not validate path {file_path}: {e}, skipping"
                    )
                    continue

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
    """
    Detect RDF format from file extension.

    Args:
        extension: File extension (with or without leading dot)

    Returns:
        RDFLib format string (defaults to "turtle" if unknown)
    """
    # Normalize extension (ensure it starts with a dot)
    if extension and not extension.startswith("."):
        extension = f".{extension}"

    format_map = {
        ".ttl": "turtle",
        ".turtle": "turtle",
        ".rdf": "xml",
        ".xml": "xml",
        ".jsonld": "json-ld",
        ".n3": "n3",
        ".nt": "nt",
        ".owl": "xml",  # OWL files are typically RDF/XML
    }
    return format_map.get(extension.lower(), "turtle")
