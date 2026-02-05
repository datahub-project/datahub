"""Encrypted archive handling for recording packages.

This module handles creation and extraction of password-protected archives
containing ingestion recordings. It uses AES-256 encryption and LZMA compression.

Archive format:
    recording.zip (AES-256 encrypted, LZMA compressed)
    ├── manifest.json       # Metadata, versions, checksums
    ├── recipe.yaml         # Recipe with secrets replaced by __REPLAY_DUMMY__
    ├── http/cassette.yaml  # VCR HTTP recordings (YAML for binary data support)
    └── db/queries.jsonl    # Database query recordings
"""

import hashlib
import json
import logging
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from datahub.ingestion.recording.config import (
    REPLAY_DUMMY_MARKER,
    REPLAY_DUMMY_VALUE,
    check_recording_dependencies,
)

logger = logging.getLogger(__name__)

# Version of the archive format
ARCHIVE_FORMAT_VERSION = "1.0.0"

# Manifest filename
MANIFEST_FILENAME = "manifest.json"

# Recipe filename
RECIPE_FILENAME = "recipe.yaml"

# HTTP recordings directory
HTTP_DIR = "http"

# Database recordings directory
DB_DIR = "db"

# Patterns for secret field names (case-insensitive)
SECRET_PATTERNS = [
    r".*password.*",
    r".*secret.*",
    r".*token.*",
    r".*api[_-]?key.*",
    r".*private[_-]?key.*",
    r".*access[_-]?key.*",
    r".*credential.*",
]

# Fields that contain auth-related words but are NOT secrets
# These are typically enum values, config options, or metadata
NON_SECRET_FIELDS = [
    "authentication_type",
    "authenticator",
    "auth_type",
    "auth_method",
    "authorization_type",
]


def redact_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
    """Replace secret values with replay-safe dummy markers.

    This function recursively traverses the config and replaces any values
    that appear to be secrets (based on key names) with REPLAY_DUMMY_MARKER.
    This allows the recipe to be safely stored while still being loadable
    during replay.

    Args:
        config: Configuration dictionary (e.g., recipe)

    Returns:
        New dictionary with secrets replaced by REPLAY_DUMMY_MARKER
    """
    return _redact_recursive(config)


def _redact_recursive(obj: Any, parent_key: str = "") -> Any:
    """Recursively redact secrets in a nested structure."""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if _is_secret_key(key) and isinstance(value, str):
                result[key] = REPLAY_DUMMY_MARKER
                logger.debug(f"Redacted secret field: {key}")
            else:
                result[key] = _redact_recursive(value, key)
        return result

    if isinstance(obj, list):
        return [_redact_recursive(item, parent_key) for item in obj]

    return obj


def _is_secret_key(key: str) -> bool:
    """Check if a key name indicates a secret value.

    Checks against known secret patterns while excluding fields that
    contain auth-related words but are not secrets (e.g., authentication_type).
    """
    key_lower = key.lower()

    # First check if this is a known non-secret field
    if key_lower in NON_SECRET_FIELDS:
        return False

    # Then check against secret patterns
    return any(re.match(pattern, key_lower) for pattern in SECRET_PATTERNS)


def prepare_recipe_for_replay(recipe: Dict[str, Any]) -> Dict[str, Any]:
    """Replace REPLAY_DUMMY_MARKER with valid dummy values for replay.

    During replay, the recipe is loaded by Pydantic which validates secret
    fields. We replace the marker with format-appropriate dummy values that
    pass validation but are never actually used (all data comes from recordings).

    For example:
    - private_key fields get a valid PEM-formatted dummy key
    - Other secrets get a generic dummy string

    Args:
        recipe: Recipe dictionary with REPLAY_DUMMY_MARKER values

    Returns:
        New dictionary with markers replaced by valid dummy values
    """
    return _replace_markers_recursive(recipe, "")


def _replace_markers_recursive(obj: Any, parent_key: str = "") -> Any:
    """Recursively replace REPLAY_DUMMY_MARKER with valid values.

    For certain fields (like private_key), we need to provide values in
    the expected format rather than a generic dummy string.
    """
    if isinstance(obj, dict):
        return {
            key: _replace_markers_recursive(value, key) for key, value in obj.items()
        }

    if isinstance(obj, list):
        return [_replace_markers_recursive(item, parent_key) for item in obj]

    if obj == REPLAY_DUMMY_MARKER:
        # Return format-appropriate dummy values based on field name
        return _get_dummy_value_for_field(parent_key)

    return obj


# Test RSA private key for replay testing
# This is a PUBLIC test key generated specifically for DataHub replay testing
# Generated with: openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt
# This key has NO real-world use and should NEVER be used for actual authentication
# It's only used during replay when secret redaction is enabled (connection is mocked anyway)
TEST_RSA_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDHjpPXINuhTHs+
M+RS/mDapEmrr7SUiSdB4uzc1EfOSJiV44JzIzVxNIb6UCGilNvdC+xYoDTbkEUX
XPrdqMJFRcgeyd4AiynJzKFtkiJNnoOaa4FOCvFvmKOQWegrdkNOyTetdV+54vz/
LU33SZYWJKGPzQE9U4vioQy1Lsql9jXB3n83CIvo9jvR6oyS7e0v32OWRDnrjlzP
zOGjQz2VRVo8pCiw6HkPYj4A8wbTLuuKiowY2dJjs5eLwndOI1qc+iv87ksrwYjJ
kZiWBenhHsbh45v86QLUqlI5accPHvBb3fy1dininxmhRN1Z9lEhdBCGH0rj5FiY
Lik8/p5RAgMBAAECggEABGW0xgcqM8sTxaZ+0+HZWEQJwAvggKvnhq0Fj2Wpmebx
Xs8rORaH51FTHpmsrhCV6jBosjjAhWyPyzCgMgl1k3Fy1BPabZxjbLgSwA957Egv
ifPvvtygnpcIVqZWhnumBsrKDGtTU00oSkaxKr9vPFhtC3ZG3lc0lEc8eJMaAc9p
tLImxv17qU2jGFTJbF7Por65M10YbArQOdXdk5vsMbJyAPx+AQTlJyFvZ/d/bTyR
Js7zwjP75L86p92vUOn+5b+Zl+OkuJTluSEIuxSsVHLKJP8B/HPCM7cmXUnSeFcS
IRLrhOi7f1CP9iHsH/M5/Mfbh4VTQVDdprnWVYcrwQKBgQD44j8rvChj0d92OI9F
ll17mjRw/yBqKYdgroayIuHXEovd2c1Zj6DAqlfRltEooBLmzaB5MW7m25bJpA/R
M9Z4LfUi/cqF2l1v0B4180pXjgVVyzKSVlMV2GWwwHIqc8vkEe9yqjEql9jlGcU/
97FyPwXf/ZL8jxUS+URkGGoisQKBgQDNQ0X8nV/8V6ak/Ku8IlkAXZvjqYBXFELP
bnsuxlX1A7VDI9eszdkjyyefSUm0w/wE2UJXqD6QMlvL9d0HRgIy2UshEB2c/lGs
hlteLv4QTDWAGx5T6WrYDM14EZuhGS3ITWE9EpqRmRKami0/2iyYgc0saUkjYoEl
YrtnQgzdoQKBgH4WkQ5lKsk3YFCSYvNMNFwUSZEdj5x5IZ63jIHe7i95s+ZXG5PO
EhDJu+fw0lIUlr7bWftMMfU/Nms9dM31xyfnkJODpACgGko1U7jdYsJsrwNCCILe
vQUKNqqPNMeRFrCa7YZX9sSvXTDkF2xK3lkU2LMb0kWlb3XHVwCm5c5hAoGBAL1z
Af2OIzF8lMpiiv8xlIPJ4j/WCiZVBPT/O6KIXH2v1nUJd95+f5ORxhg2RFkbKlgv
ThQprNTaJe+yFTbJXu4fsD/r5+kmsatStrHPHZ9dN2Pto6g/H+YYquvPFJ0z6BWf
lcgQi6kmZw1aj7kHXXHFG+GJq3+FQz2GSwGa7NUBAoGAW8qpBFtG8ExEug7kTDNF
4Lgdyb2kyGtq8OGLgPctVhDGAv8zJeb3GbEtZbjhBEXzQ/kyCkVZEXpWGHwv1wrP
hxU8kG/Q3sbr9FMMD0iLakcoWOus3T1NY7GOlTo6hiAlkpJufU7jOLZgaci8+koQ
gu1Yi3GEOFR5fhCw7xjuO+E=
-----END PRIVATE KEY-----
"""


def _get_dummy_value_for_field(field_name: str) -> str:
    """Get an appropriate dummy value based on field name.

    Some fields require specific formats (e.g., PEM keys, JSON tokens).
    This ensures the dummy value passes validation during replay.
    """
    field_lower = field_name.lower()

    # Private key fields need valid PEM format (PKCS#8)
    # Use the public test key constant defined above
    if "private_key" in field_lower or "private-key" in field_lower:
        return TEST_RSA_PRIVATE_KEY

    # Default dummy value for other secrets
    return REPLAY_DUMMY_VALUE


def compute_checksum(file_path: Path) -> str:
    """Compute SHA-256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


class ArchiveManifest:
    """Manifest containing metadata about the recording archive."""

    def __init__(
        self,
        run_id: str,
        source_type: Optional[str] = None,
        sink_type: Optional[str] = None,
        datahub_cli_version: Optional[str] = None,
        python_version: Optional[str] = None,
        created_at: Optional[str] = None,
        checksums: Optional[Dict[str, str]] = None,
        format_version: str = ARCHIVE_FORMAT_VERSION,
        exception_info: Optional[Dict[str, Any]] = None,
        recording_start_time: Optional[str] = None,
    ) -> None:
        self.run_id = run_id
        self.source_type = source_type
        self.sink_type = sink_type
        self.datahub_cli_version = datahub_cli_version
        self.python_version = python_version
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()
        self.checksums = checksums or {}
        self.format_version = format_version
        # Exception info captures any error that occurred during ingestion
        # This allows replay to reproduce the exact failure
        self.exception_info = exception_info
        # Recording start time - used to freeze time during replay for determinism
        self.recording_start_time = recording_start_time or self.created_at

    @property
    def has_exception(self) -> bool:
        """Check if the recording captured an exception."""
        return self.exception_info is not None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "format_version": self.format_version,
            "run_id": self.run_id,
            "source_type": self.source_type,
            "sink_type": self.sink_type,
            "datahub_cli_version": self.datahub_cli_version,
            "python_version": self.python_version,
            "created_at": self.created_at,
            "checksums": self.checksums,
            "recording_start_time": self.recording_start_time,
        }
        # Only include exception_info if present
        if self.exception_info:
            result["exception_info"] = self.exception_info
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArchiveManifest":
        """Create from dictionary."""
        return cls(
            run_id=data["run_id"],
            source_type=data.get("source_type"),
            sink_type=data.get("sink_type"),
            datahub_cli_version=data.get("datahub_cli_version"),
            python_version=data.get("python_version"),
            created_at=data.get("created_at"),
            checksums=data.get("checksums", {}),
            format_version=data.get("format_version", ARCHIVE_FORMAT_VERSION),
            exception_info=data.get("exception_info"),
            recording_start_time=data.get("recording_start_time"),
        )


class RecordingArchive:
    """Handles creation and extraction of encrypted recording archives."""

    def __init__(self, password: str) -> None:
        """Initialize archive handler.

        Args:
            password: Password for encryption/decryption.
        """
        check_recording_dependencies()
        self.password = password.encode("utf-8")

    def create(
        self,
        output_path: Path,
        temp_dir: Path,
        manifest: ArchiveManifest,
        recipe: Dict[str, Any],
        redact_secrets_enabled: bool = True,
    ) -> Path:
        """Create an encrypted archive from recording files.

        Args:
            output_path: Path for the output archive file.
            temp_dir: Directory containing recording files (http/, db/).
            manifest: Archive manifest with metadata.
            recipe: Original recipe dictionary.
            redact_secrets_enabled: Whether to redact secrets from the recipe (default: True).
                                   Set to False for local debugging to keep actual credentials.

        Returns:
            Path to the created archive.
        """
        import pyzipper

        # Conditionally redact secrets from recipe before storing
        if redact_secrets_enabled:
            stored_recipe = redact_secrets(recipe)
            logger.debug("Secrets redacted from recipe before archiving")
        else:
            stored_recipe = recipe
            logger.warning(
                "Secret redaction DISABLED - recipe contains actual credentials. "
                "Do NOT share this recording outside secure environments."
            )

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Calculate checksums for all files
        checksums: Dict[str, str] = {}
        for file_path in temp_dir.rglob("*"):
            if file_path.is_file():
                rel_path = file_path.relative_to(temp_dir)
                checksums[str(rel_path)] = compute_checksum(file_path)

        manifest.checksums = checksums

        # Create the encrypted zip
        with pyzipper.AESZipFile(
            output_path,
            "w",
            compression=pyzipper.ZIP_LZMA,
            encryption=pyzipper.WZ_AES,
        ) as zf:
            zf.setpassword(self.password)

            # Add manifest
            manifest_data = json.dumps(manifest.to_dict(), indent=2)
            zf.writestr(MANIFEST_FILENAME, manifest_data.encode("utf-8"))

            # Add recipe
            recipe_data = yaml.dump(stored_recipe, default_flow_style=False)
            zf.writestr(RECIPE_FILENAME, recipe_data.encode("utf-8"))

            # Add recording files
            for file_path in temp_dir.rglob("*"):
                if file_path.is_file():
                    rel_path = file_path.relative_to(temp_dir)
                    zf.write(file_path, str(rel_path))

        logger.info(f"Created encrypted archive: {output_path}")
        return output_path

    def extract(self, archive_path: Path, output_dir: Optional[Path] = None) -> Path:
        """Extract an encrypted archive.

        Args:
            archive_path: Path to the archive file.
            output_dir: Directory to extract to. If None, uses a temp directory.

        Returns:
            Path to the extraction directory.
        """
        import pyzipper

        if output_dir is None:
            output_dir = Path(tempfile.mkdtemp(prefix="datahub_recording_"))

        output_dir.mkdir(parents=True, exist_ok=True)

        with pyzipper.AESZipFile(archive_path, "r") as zf:
            zf.setpassword(self.password)
            zf.extractall(output_dir)

        logger.info(f"Extracted archive to: {output_dir}")
        return output_dir

    def read_manifest(self, archive_path: Path) -> ArchiveManifest:
        """Read manifest from archive without full extraction."""
        import pyzipper

        with pyzipper.AESZipFile(archive_path, "r") as zf:
            zf.setpassword(self.password)
            manifest_data = zf.read(MANIFEST_FILENAME).decode("utf-8")
            return ArchiveManifest.from_dict(json.loads(manifest_data))

    def read_recipe(self, archive_path: Path) -> Dict[str, Any]:
        """Read recipe from archive without full extraction."""
        import pyzipper

        with pyzipper.AESZipFile(archive_path, "r") as zf:
            zf.setpassword(self.password)
            recipe_data = zf.read(RECIPE_FILENAME).decode("utf-8")
            return yaml.safe_load(recipe_data)

    def verify_checksums(self, extracted_dir: Path, manifest: ArchiveManifest) -> bool:
        """Verify file checksums after extraction.

        Args:
            extracted_dir: Directory where archive was extracted.
            manifest: Manifest with expected checksums.

        Returns:
            True if all checksums match, False otherwise.
        """
        for rel_path, expected_checksum in manifest.checksums.items():
            file_path = extracted_dir / rel_path
            if not file_path.exists():
                logger.error(f"Missing file: {rel_path}")
                return False

            actual_checksum = compute_checksum(file_path)
            if actual_checksum != expected_checksum:
                logger.error(
                    f"Checksum mismatch for {rel_path}: "
                    f"expected {expected_checksum}, got {actual_checksum}"
                )
                return False

        return True


def list_archive_contents(archive_path: Path, password: str) -> List[str]:
    """List contents of an encrypted archive.

    Args:
        archive_path: Path to the archive file.
        password: Archive password.

    Returns:
        List of file paths in the archive.
    """
    check_recording_dependencies()
    import pyzipper

    with pyzipper.AESZipFile(archive_path, "r") as zf:
        zf.setpassword(password.encode("utf-8"))
        return zf.namelist()


def get_archive_info(archive_path: Path, password: str) -> Dict[str, Any]:
    """Get information about an archive.

    Args:
        archive_path: Path to the archive file.
        password: Archive password.

    Returns:
        Dictionary with archive information.
    """
    archive = RecordingArchive(password)
    manifest = archive.read_manifest(archive_path)

    files = list(manifest.checksums.keys())

    # Check for HTTP cassette
    has_http_cassette = any(f.startswith(f"{HTTP_DIR}/") for f in files)
    # Check for DB queries
    has_db_queries = any(f.startswith(f"{DB_DIR}/") for f in files)

    info = {
        "format_version": manifest.format_version,
        "run_id": manifest.run_id,
        "source_type": manifest.source_type,
        "sink_type": manifest.sink_type,
        "datahub_cli_version": manifest.datahub_cli_version,
        "python_version": manifest.python_version,
        "created_at": manifest.created_at,
        "recording_start_time": manifest.recording_start_time,
        "file_count": len(manifest.checksums),
        "files": files,
        "has_exception": manifest.has_exception,
        "has_http_cassette": has_http_cassette,
        "has_db_queries": has_db_queries,
    }

    # Include exception details if present
    if manifest.exception_info:
        info["exception_type"] = manifest.exception_info.get("type")
        info["exception_message"] = manifest.exception_info.get("message")
        # Traceback can be very long, include a truncated version
        traceback_str = manifest.exception_info.get("traceback", "")
        if len(traceback_str) > 500:
            info["exception_traceback"] = traceback_str[:500] + "..."
        else:
            info["exception_traceback"] = traceback_str

    return info
