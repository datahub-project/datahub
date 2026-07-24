import hashlib
import logging
import os
from pathlib import Path
from typing import List
from urllib.request import urlopen

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.informix.config import InformixSourceConfig

logger = logging.getLogger(__name__)

_MAVEN = "https://repo1.maven.org/maven2"
_LICENSE_URL = (
    "http://www-03.ibm.com/software/sla/sladb.nsf/doclookup/"
    "CA4476C0AF8346EC852579290012D218?OpenDocument"
)


def _download(url: str) -> bytes:
    with urlopen(url) as resp:
        return resp.read()


def _fetch_verified(base_url: str, filename: str, cache: Path) -> str:
    jar_path = cache / filename
    sha_path = cache / (filename + ".sha1")
    if jar_path.exists() and sha_path.exists():
        expected = sha_path.read_text().strip().split()[0]
        if hashlib.sha1(jar_path.read_bytes()).hexdigest() == expected:
            return str(jar_path)
        logger.warning("Cached %s failed checksum; re-downloading.", filename)

    expected = _download(base_url + ".sha1").decode().strip().split()[0]
    data = _download(base_url)
    actual = hashlib.sha1(data).hexdigest()
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
