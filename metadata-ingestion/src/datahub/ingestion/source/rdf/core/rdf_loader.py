#!/usr/bin/env python3
"""
Simple RDF Graph Loader

Supports loading RDF from files, folders, URLs, zip files (local and remote),
web folder URLs (HTML directory listings), and glob patterns.
"""

import glob
import io
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

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
        source: File path, folder path, URL, zip file (local or remote),
               web folder URL, comma-separated files, or glob pattern
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
        # Check if it's a zip file URL
        if _is_zip_url(source):
            return _load_from_zip_url(
                source,
                recursive,
                file_extensions,
                format,
                url_timeout,
                max_url_size,
                max_file_size,
            )
        # Check if it's a web folder (HTML directory listing)
        if _is_web_folder_url(source):
            return _load_from_web_folder(
                source,
                recursive,
                file_extensions,
                format,
                url_timeout,
                max_url_size,
                max_file_size,
            )
        # Otherwise, treat as a single RDF file URL
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

    # Check if it's a zip file (local)
    if path.exists() and path.is_file() and _is_zip_file(path):
        return _load_from_zip_file(
            path, recursive, file_extensions, format, max_file_size, base_dir
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


def _is_zip_file(path: Path) -> bool:
    """
    Check if a file is a zip file by extension or by trying to open it.

    Args:
        path: Path to file

    Returns:
        True if file is a zip file, False otherwise
    """
    # Check by extension first (fast)
    zip_extensions = {".zip", ".jar", ".war", ".ear"}
    if path.suffix.lower() in zip_extensions:
        return True

    # Try to open as zip file (more reliable)
    try:
        with zipfile.ZipFile(path, "r") as zf:
            # Just check if we can read the file list
            zf.testzip()
            return True
    except (zipfile.BadZipFile, IOError, OSError):
        return False


def _is_zip_url(url: str) -> bool:
    """
    Check if a URL points to a zip file.

    Args:
        url: URL to check

    Returns:
        True if URL likely points to a zip file, False otherwise
    """
    # Check by extension in URL
    zip_extensions = {".zip", ".jar", ".war", ".ear"}
    parsed = urlparse(url)
    path = parsed.path.lower()
    return any(path.endswith(ext) for ext in zip_extensions)


def _is_web_folder_url(url: str) -> bool:
    """
    Check if a URL is likely a web folder (HTML directory listing).

    Args:
        url: URL to check

    Returns:
        True if URL likely points to a web folder, False otherwise
    """
    # URLs ending with / are likely folders
    if url.endswith("/"):
        return True

    # Check if URL doesn't have a file extension (likely a folder)
    parsed = urlparse(url)
    path = parsed.path
    if not path or path == "/":
        return True

    # If path ends with /, it's a folder
    if path.endswith("/"):
        return True

    # If no extension, might be a folder
    if "." not in Path(path).name:
        return True

    return False


def _load_from_zip_file(
    zip_path: Path,
    recursive: bool,
    file_extensions: List[str],
    format: Optional[str] = None,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    base_dir: Optional[str] = None,
) -> Graph:
    """
    Load RDF from a local zip file.

    Args:
        zip_path: Path to zip file
        recursive: Enable recursive processing
        file_extensions: File extensions to process
        format: RDF format (auto-detected if not specified)
        max_file_size: Maximum file size in bytes for individual files
        base_dir: Optional base directory for path traversal protection

    Returns:
        RDFLib Graph containing the loaded RDF data

    Raises:
        FileNotFoundError: If zip file doesn't exist
        ValueError: If zip file is invalid or exceeds size limits
        zipfile.BadZipFile: If file is not a valid zip file
    """
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    # Check zip file size
    zip_size = zip_path.stat().st_size
    if (
        zip_size > max_file_size * 10
    ):  # Allow larger zip files (10x individual file limit)
        raise ValueError(
            f"Zip file too large: {zip_size} bytes (max: {max_file_size * 10} bytes)"
        )

    graph = Graph()
    files_loaded = 0

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            # Get all file names in zip
            file_names = zf.namelist()

            for file_name in file_names:
                # Skip directories
                if file_name.endswith("/"):
                    continue

                # Check if file matches extensions
                file_path = Path(file_name)
                if file_path.suffix.lower() not in [
                    ext.lower() for ext in file_extensions
                ]:
                    continue

                # Check recursive processing
                if not recursive and "/" in file_name:
                    # Check if file is in a subdirectory
                    parts = file_name.split("/")
                    if len(parts) > 2:  # More than just filename
                        continue

                # Get file info for size check
                try:
                    file_info = zf.getinfo(file_name)
                    if file_info.file_size > max_file_size:
                        logger.warning(
                            f"File {file_name} in zip too large ({file_info.file_size} bytes), skipping"
                        )
                        continue
                except Exception as e:
                    logger.warning(
                        f"Could not check size of {file_name} in zip: {e}, skipping"
                    )
                    continue

                # Validate path security (prevent zip slip attacks)
                # Reject absolute paths and paths with .. components that could escape
                if file_name.startswith("/") or ".." in file_name:
                    logger.warning(
                        f"Skipping {file_name}: potential zip slip attack detected (absolute path or '..' component)"
                    )
                    continue

                # Read and parse file from zip
                try:
                    with zf.open(file_name) as file_in_zip:
                        content = file_in_zip.read()
                        format_str = format or _detect_format_from_extension(
                            file_path.suffix
                        )
                        graph.parse(data=content, format=format_str)
                        files_loaded += 1
                        logger.debug(f"Loaded {file_name} from zip {zip_path}")
                except Exception as e:
                    logger.warning(f"Failed to load {file_name} from zip: {e}")

    except zipfile.BadZipFile:
        raise ValueError(f"Invalid zip file: {zip_path}") from None
    except Exception as e:
        logger.error(f"Failed to process zip file {zip_path}: {e}")
        raise

    logger.info(
        f"Loaded {len(graph)} triples from {files_loaded} files in zip {zip_path}"
    )
    return graph


def _load_from_zip_url(
    url: str,
    recursive: bool,
    file_extensions: List[str],
    format: Optional[str] = None,
    url_timeout: int = DEFAULT_URL_TIMEOUT,
    max_url_size: int = DEFAULT_MAX_URL_SIZE,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
) -> Graph:
    """
    Load RDF from a zip file URL.

    Args:
        url: URL to zip file
        recursive: Enable recursive processing
        file_extensions: File extensions to process
        format: RDF format (auto-detected if not specified)
        url_timeout: Request timeout in seconds
        max_url_size: Maximum zip file size in bytes
        max_file_size: Maximum file size in bytes for individual files in zip

    Returns:
        RDFLib Graph containing the loaded RDF data

    Raises:
        requests.RequestException: If request fails
        ValueError: If zip file exceeds size limit or is invalid
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
        session.max_redirects = 10

        # Download zip file
        response = session.get(url, timeout=url_timeout, stream=True)
        response.raise_for_status()

        # Check content length
        content_length = None
        if "content-length" in response.headers:
            content_length = int(response.headers["content-length"])
            if content_length > max_url_size:
                raise ValueError(
                    f"Zip file too large: {content_length} bytes (max: {max_url_size} bytes)"
                )

        # Download zip content
        zip_content = b""
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                zip_content += chunk
                if len(zip_content) > max_url_size:
                    raise ValueError(
                        f"Zip file exceeds size limit: {len(zip_content)} bytes "
                        f"(max: {max_url_size} bytes)"
                    )

        # Process zip from memory
        files_loaded = 0
        with zipfile.ZipFile(io.BytesIO(zip_content), "r") as zf:
            file_names = zf.namelist()

            for file_name in file_names:
                # Skip directories
                if file_name.endswith("/"):
                    continue

                # Check if file matches extensions
                file_path = Path(file_name)
                if file_path.suffix.lower() not in [
                    ext.lower() for ext in file_extensions
                ]:
                    continue

                # Check recursive processing
                if not recursive and "/" in file_name:
                    parts = file_name.split("/")
                    if len(parts) > 2:
                        continue

                # Get file info for size check
                try:
                    file_info = zf.getinfo(file_name)
                    if file_info.file_size > max_file_size:
                        logger.warning(
                            f"File {file_name} in zip too large ({file_info.file_size} bytes), skipping"
                        )
                        continue
                except Exception as e:
                    logger.warning(
                        f"Could not check size of {file_name} in zip: {e}, skipping"
                    )
                    continue

                # Read and parse file from zip
                try:
                    with zf.open(file_name) as file_in_zip:
                        content = file_in_zip.read()
                        format_str = format or _detect_format_from_extension(
                            file_path.suffix
                        )
                        graph.parse(data=content, format=format_str)
                        files_loaded += 1
                        logger.debug(f"Loaded {file_name} from zip URL {url}")
                except Exception as e:
                    logger.warning(f"Failed to load {file_name} from zip: {e}")

        logger.info(
            f"Loaded {len(graph)} triples from {files_loaded} files in zip from {url}"
        )
        return graph

    except requests.Timeout:
        raise ValueError(
            f"Request to {url} timed out after {url_timeout} seconds"
        ) from None
    except requests.RequestException as e:
        raise ValueError(f"Failed to load zip from URL {url}: {e}") from e
    except zipfile.BadZipFile:
        raise ValueError(f"Invalid zip file from URL: {url}") from None


def _create_retry_session() -> requests.Session:
    """Create a requests session with retry strategy."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.max_redirects = 10
    return session


def _parse_html_directory_listing(
    html_content: str, base_url: str, file_extensions: List[str], recursive: bool
) -> tuple[List[str], List[str]]:
    """Parse HTML directory listing and return (file_urls, folder_urls)."""
    from html.parser import HTMLParser

    class DirectoryListingParser(HTMLParser):
        def __init__(self, base_url: str, file_extensions: List[str], recursive: bool):
            super().__init__()
            self.base_url = base_url
            self.file_extensions = file_extensions
            self.recursive = recursive
            self.links: List[str] = []
            self.folders: List[str] = []

        def handle_starttag(self, tag, attrs):
            if tag == "a":
                attrs_dict = dict(attrs)
                href = attrs_dict.get("href", "")
                if href and href != "../" and href != "../":
                    # Resolve relative URL
                    full_url = urljoin(self.base_url, href)
                    # Check if it's a file or folder
                    if href.endswith("/"):
                        if self.recursive:
                            self.folders.append(full_url)
                    else:
                        # Check if file matches extensions
                        path = Path(href)
                        if path.suffix.lower() in [
                            ext.lower() for ext in self.file_extensions
                        ]:
                            self.links.append(full_url)

    parser = DirectoryListingParser(base_url, file_extensions, recursive)
    parser.feed(html_content)
    return parser.links, parser.folders


def _download_and_parse_rdf_file(
    session: requests.Session,
    file_url: str,
    format: Optional[str],
    url_timeout: int,
    max_url_size: int,
    graph: Graph,
) -> bool:
    """Download and parse a single RDF file. Returns True if successful."""
    try:
        file_response = session.get(file_url, timeout=url_timeout, stream=True)
        file_response.raise_for_status()

        # Check content length
        if "content-length" in file_response.headers:
            file_size = int(file_response.headers["content-length"])
            if file_size > max_url_size:
                logger.warning(
                    f"File {file_url} too large ({file_size} bytes), skipping"
                )
                return False

        # Download file content
        file_content = b""
        for chunk in file_response.iter_content(chunk_size=8192):
            if chunk:
                file_content += chunk
                if len(file_content) > max_url_size:
                    logger.warning(f"File {file_url} exceeds size limit, skipping")
                    return False

        if len(file_content) > max_url_size:
            return False

        # Parse RDF
        file_path = Path(file_url)
        format_str = format or _detect_format_from_extension(file_path.suffix)
        graph.parse(data=file_content, format=format_str)
        logger.debug(f"Loaded {file_url}")
        return True

    except Exception as e:
        logger.warning(f"Failed to load {file_url}: {e}")
        return False


def _load_from_web_folder(
    url: str,
    recursive: bool,
    file_extensions: List[str],
    format: Optional[str] = None,
    url_timeout: int = DEFAULT_URL_TIMEOUT,
    max_url_size: int = DEFAULT_MAX_URL_SIZE,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
) -> Graph:
    """
    Load RDF from a web folder URL (HTML directory listing).

    This function attempts to parse HTML directory listings and download
    RDF files from the folder. This works with servers that provide HTML
    directory listings (like Apache's mod_autoindex).

    Args:
        url: URL to web folder (should end with /)
        recursive: Enable recursive processing (downloads from subfolders)
        file_extensions: File extensions to process
        format: RDF format (auto-detected if not specified)
        url_timeout: Request timeout in seconds
        max_url_size: Maximum file size in bytes for individual files
        max_file_size: Maximum file size in bytes (same as max_url_size for consistency)

    Returns:
        RDFLib Graph containing the loaded RDF data

    Raises:
        requests.RequestException: If request fails
        ValueError: If folder cannot be accessed or parsed
    """
    graph = Graph()

    try:
        # Ensure URL ends with /
        if not url.endswith("/"):
            url = url + "/"

        # Create session with retry strategy
        session = _create_retry_session()

        # Fetch HTML directory listing
        response = session.get(url, timeout=url_timeout)
        response.raise_for_status()

        # Check if response is HTML
        content_type = response.headers.get("content-type", "").lower()
        if "text/html" not in content_type:
            raise ValueError(
                f"URL {url} does not appear to be an HTML directory listing "
                f"(content-type: {content_type})"
            )

        # Parse HTML to find links
        file_urls, folder_urls = _parse_html_directory_listing(
            response.text, url, file_extensions, recursive
        )

        files_loaded = 0

        # Download and parse RDF files
        for file_url in file_urls:
            if _download_and_parse_rdf_file(
                session, file_url, format, url_timeout, max_url_size, graph
            ):
                files_loaded += 1

        # Recursively process subfolders if enabled
        if recursive:
            for folder_url in folder_urls:
                try:
                    subgraph = _load_from_web_folder(
                        folder_url,
                        recursive,
                        file_extensions,
                        format,
                        url_timeout,
                        max_url_size,
                        max_file_size,
                    )
                    # Merge subgraph into main graph
                    for triple in subgraph:
                        graph.add(triple)
                except Exception as e:
                    logger.warning(f"Failed to process subfolder {folder_url}: {e}")

        logger.info(
            f"Loaded {len(graph)} triples from {files_loaded} files in web folder {url}"
        )
        return graph

    except requests.Timeout:
        raise ValueError(
            f"Request to {url} timed out after {url_timeout} seconds"
        ) from None
    except requests.RequestException as e:
        raise ValueError(f"Failed to load from web folder {url}: {e}") from e


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
