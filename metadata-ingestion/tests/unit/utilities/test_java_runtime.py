import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterator, Optional

import pytest

from datahub.utilities import java_runtime
from datahub.utilities.java_runtime import prepare_jvm_settings


def _fake_jpype(system_jvm: Optional[Path]) -> ModuleType:
    """A jpype stand-in whose JVM discovery mirrors the real lookup order:
    JAVA_HOME wins, then the system JVM if one exists."""
    module = ModuleType("jpype")

    def get_default_jvm_path() -> str:
        java_home = os.environ.get("JAVA_HOME")
        if java_home:
            return str(Path(java_home) / "lib" / "server" / "libjvm.so")
        if system_jvm:
            return str(system_jvm / "lib" / "server" / "libjvm.so")
        raise ValueError("No JVM shared library file found")

    module.getDefaultJVMPath = get_default_jvm_path  # type: ignore[attr-defined]
    return module


def _fake_jdk4py(java_home: Path) -> ModuleType:
    module = ModuleType("jdk4py")
    module.JAVA_HOME = java_home  # type: ignore[attr-defined]
    return module


@pytest.fixture
def jdk4py_home(tmp_path: Path) -> Path:
    home = tmp_path / "jdk4py" / "java-runtime"
    (home / "lib" / "server").mkdir(parents=True)
    return home


@pytest.fixture
def system_jvm_home(tmp_path: Path) -> Path:
    home = tmp_path / "usr" / "lib" / "jvm" / "java-25-openjdk"
    (home / "lib" / "server").mkdir(parents=True)
    (home / "release").write_text('JAVA_VERSION="25.0.2"\n')
    return home


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("JAVA_HOME", raising=False)
    yield


def test_system_jvm_preferred_over_jdk4py(
    monkeypatch: pytest.MonkeyPatch,
    clean_env: None,
    system_jvm_home: Path,
    jdk4py_home: Path,
) -> None:
    monkeypatch.setitem(sys.modules, "jpype", _fake_jpype(system_jvm_home))
    monkeypatch.setitem(sys.modules, "jdk4py", _fake_jdk4py(jdk4py_home))

    assert prepare_jvm_settings() == []
    assert "JAVA_HOME" not in os.environ


def test_falls_back_to_jdk4py_without_system_jvm(
    monkeypatch: pytest.MonkeyPatch,
    clean_env: None,
    jdk4py_home: Path,
    tmp_path: Path,
) -> None:
    monkeypatch.setitem(sys.modules, "jpype", _fake_jpype(system_jvm=None))
    monkeypatch.setitem(sys.modules, "jdk4py", _fake_jdk4py(jdk4py_home))
    monkeypatch.setattr(
        java_runtime, "SYSTEM_JAVA_TRUST_STORE", tmp_path / "missing-cacerts"
    )

    assert prepare_jvm_settings() == []
    assert os.environ["JAVA_HOME"] == str(jdk4py_home)


def test_jdk4py_fallback_uses_system_trust_store_when_present(
    monkeypatch: pytest.MonkeyPatch,
    clean_env: None,
    jdk4py_home: Path,
    tmp_path: Path,
) -> None:
    monkeypatch.setitem(sys.modules, "jpype", _fake_jpype(system_jvm=None))
    monkeypatch.setitem(sys.modules, "jdk4py", _fake_jdk4py(jdk4py_home))
    trust_store = tmp_path / "cacerts"
    trust_store.write_bytes(b"not-a-real-keystore")
    monkeypatch.setattr(java_runtime, "SYSTEM_JAVA_TRUST_STORE", trust_store)

    assert prepare_jvm_settings() == [f"-Djavax.net.ssl.trustStore={trust_store}"]


@pytest.mark.parametrize(
    "release_content",
    [
        pytest.param('JAVA_VERSION="1.8.0_392"\n', id="java-8"),
        pytest.param(None, id="no-release-file"),
    ],
)
def test_unstartable_system_jvm_falls_back_to_jdk4py(
    monkeypatch: pytest.MonkeyPatch,
    clean_env: None,
    jdk4py_home: Path,
    tmp_path: Path,
    release_content: Optional[str],
) -> None:
    # JPype's finder returns any libjvm regardless of Java version, but JPype
    # cannot start pre-11 JVMs; such a JVM must not displace the jdk4py fallback.
    home = tmp_path / "usr" / "lib" / "jvm" / "java-8-openjdk"
    (home / "lib" / "server").mkdir(parents=True)
    if release_content is not None:
        (home / "release").write_text(release_content)
    monkeypatch.setitem(sys.modules, "jpype", _fake_jpype(system_jvm=home))
    monkeypatch.setitem(sys.modules, "jdk4py", _fake_jdk4py(jdk4py_home))
    monkeypatch.setattr(
        java_runtime, "SYSTEM_JAVA_TRUST_STORE", tmp_path / "missing-cacerts"
    )

    assert prepare_jvm_settings() == []
    assert os.environ["JAVA_HOME"] == str(jdk4py_home)


def test_preset_java_home_is_respected(
    monkeypatch: pytest.MonkeyPatch,
    system_jvm_home: Path,
    jdk4py_home: Path,
) -> None:
    monkeypatch.setenv("JAVA_HOME", str(system_jvm_home))
    monkeypatch.setitem(sys.modules, "jpype", _fake_jpype(system_jvm=None))
    monkeypatch.setitem(sys.modules, "jdk4py", _fake_jdk4py(jdk4py_home))

    assert prepare_jvm_settings() == []
    assert os.environ["JAVA_HOME"] == str(system_jvm_home)
