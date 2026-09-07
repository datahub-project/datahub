import hashlib
import logging
import os
from pathlib import Path
from typing import List, Optional
from urllib.request import urlopen

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.informix.config import InformixSourceConfig

logger = logging.getLogger(__name__)

_MAVEN = "https://repo1.maven.org/maven2"
_LICENSE_URL = (
    "http://www-03.ibm.com/software/sla/sladb.nsf/doclookup/"
    "CA4476C0AF8346EC852579290012D218?OpenDocument"
)
# Maven Central only publishes .sha1 and .md5 for these two artifacts (.sha256 and
# .sha512 both 404), so SHA-1 is the strongest checksum actually on offer. It is
# not the trust anchor: the jar and its checksum come from the same origin, so
# anyone able to swap one can swap the other. Integrity rests on TLS to
# repo1.maven.org (enforced in _download); the checksum catches truncation and
# corruption, and detects a stale or partially-written cache entry.
_CHECKSUM_EXT = ".sha1"


def _download(url: str) -> bytes:
    # urlopen honours whatever scheme it is handed, including file:// and ftp://.
    # Every caller composes URLs from _MAVEN, but assert it rather than assume it.
    if not url.startswith("https://"):
        raise ConfigurationError(f"Refusing to download over a non-HTTPS URL: {url}")
    try:
        with urlopen(url, timeout=30) as resp:
            return resp.read()
    except OSError as e:
        raise ConfigurationError(
            f"Failed to download the Informix JDBC driver from {url}. For "
            "air-gapped or network-restricted environments, pre-provision the "
            "jars and set 'driver_jar_paths' instead."
        ) from e


def _digest(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def _first_token(text: str) -> Optional[str]:
    # Maven .sha1 files are either a bare hex digest or `hexdigest  filename`.
    tokens = text.strip().split()
    return tokens[0] if tokens else None


def _fetch_verified(base_url: str, filename: str, cache: Path) -> str:
    jar_path = cache / filename
    sha_path = cache / (filename + _CHECKSUM_EXT)
    if jar_path.exists() and sha_path.exists():
        expected = _first_token(sha_path.read_text())
        if expected is None:
            raise ConfigurationError(f"Malformed {_CHECKSUM_EXT} file for {filename}")
        if _digest(jar_path.read_bytes()) == expected:
            return str(jar_path)
        logger.warning("Cached %s failed checksum; re-downloading.", filename)

    expected = _first_token(_download(base_url + _CHECKSUM_EXT).decode())
    if expected is None:
        raise ConfigurationError(f"Malformed {_CHECKSUM_EXT} payload for {filename}")
    data = _download(base_url)
    actual = _digest(data)
    if actual != expected:
        raise ConfigurationError(
            f"Checksum mismatch for {filename}: expected {expected}, got {actual}"
        )
    cache.mkdir(parents=True, exist_ok=True)
    jar_path.write_bytes(data)
    sha_path.write_text(expected)
    return str(jar_path)


def resolve_driver_jars(config: InformixSourceConfig) -> List[str]:
    if config.driver_jar_paths:
        return list(config.driver_jar_paths)

    if not config.accept_ibm_jdbc_license:
        raise ConfigurationError(
            "The Informix JDBC driver is proprietary. Either set "
            "'driver_jar_paths' to pre-provisioned jars, or set "
            "'accept_ibm_jdbc_license: true' to auto-download it from Maven "
            f"Central under the IBM Informix JDBC Software License Agreement "
            f"({_LICENSE_URL})."
        )

    cache = Path(
        config.driver_cache_dir
        or os.path.join(os.path.expanduser("~"), ".datahub", "jars", "informix")
    )
    v = config.jdbc_driver_version
    bv = config.bson_version
    jdbc = _fetch_verified(
        f"{_MAVEN}/com/ibm/informix/jdbc/{v}/jdbc-{v}.jar", f"jdbc-{v}.jar", cache
    )
    bson = _fetch_verified(
        f"{_MAVEN}/org/mongodb/bson/{bv}/bson-{bv}.jar", f"bson-{bv}.jar", cache
    )
    return [jdbc, bson]
