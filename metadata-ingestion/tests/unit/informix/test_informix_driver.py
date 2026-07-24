import hashlib

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.driver import resolve_driver_jars


def _cfg(**kwargs):
    base = {"server": "informix", "database": "testdb"}
    base.update(kwargs)
    return InformixSourceConfig.parse_obj(base)


def test_byo_paths_used_verbatim(tmp_path):
    jar = tmp_path / "jdbc.jar"
    jar.write_bytes(b"fake")
    result = resolve_driver_jars(_cfg(driver_jar_paths=[str(jar)]))
    assert result == [str(jar)]


def test_no_consent_no_byo_raises():
    with pytest.raises(ConfigurationError):
        resolve_driver_jars(_cfg())


def test_cache_hit_skips_download(tmp_path, monkeypatch):
    # Pre-place both jars + valid .sha1 sidecars in the cache; download must not run.
    cache = tmp_path / "cache"
    cache.mkdir()

    def _place(name: str, content: bytes):
        p = cache / name
        p.write_bytes(content)
        (cache / (name + ".sha1")).write_text(hashlib.sha1(content).hexdigest())

    _place("jdbc-4.50.10.jar", b"jdbc-bytes")
    _place("bson-4.11.1.jar", b"bson-bytes")

    def _fail_download(*args, **kwargs):
        raise AssertionError("download must not be called on cache hit")

    monkeypatch.setattr(
        "datahub.ingestion.source.informix.driver._download", _fail_download
    )
    result = resolve_driver_jars(
        _cfg(accept_ibm_jdbc_license=True, driver_cache_dir=str(cache))
    )
    assert sorted(p.split("/")[-1] for p in result) == ["bson-4.11.1.jar", "jdbc-4.50.10.jar"]


def test_checksum_mismatch_raises_and_cleans_cache(tmp_path, monkeypatch):
    # Monkeypatch _download to return a valid sha1 hex for .sha1 URLs,
    # but jar-bytes that do NOT hash to it. Should raise ConfigurationError
    # and clean up the cache.
    cache = tmp_path / "cache"
    cache.mkdir()

    fake_checksum = b"a" * 40  # Fixed invalid hex digest
    mismatched_jar_bytes = b"wrong-jar-content"

    def _mock_download(url: str) -> bytes:
        if url.endswith(".sha1"):
            return fake_checksum
        else:
            return mismatched_jar_bytes

    monkeypatch.setattr(
        "datahub.ingestion.source.informix.driver._download", _mock_download
    )

    with pytest.raises(ConfigurationError, match="Checksum mismatch"):
        resolve_driver_jars(
            _cfg(accept_ibm_jdbc_license=True, driver_cache_dir=str(cache))
        )

    # Verify jar and .sha1 sidecar were NOT left in cache
    assert not (cache / "jdbc-4.50.10.jar").exists()
    assert not (cache / "jdbc-4.50.10.jar.sha1").exists()
    assert not (cache / "bson-4.11.1.jar").exists()
    assert not (cache / "bson-4.11.1.jar.sha1").exists()
