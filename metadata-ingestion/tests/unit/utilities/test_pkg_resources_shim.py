import importlib.metadata
import importlib.util
import os
import sys

import pytest

from datahub.utilities import pkg_resources_shim
from datahub.utilities.pkg_resources_shim import (
    DistributionNotFound,
    ensure_pkg_resources,
    get_distribution,
    parse_version,
    require,
    resource_filename,
)

_PKG = "acryl-datahub"


def test_get_distribution_returns_version_and_parsed_version():
    dist = get_distribution(_PKG)
    assert dist.version == importlib.metadata.version(_PKG)
    assert dist.parsed_version == parse_version(dist.version)


def test_get_distribution_missing_raises():
    with pytest.raises(DistributionNotFound):
        get_distribution("no-such-distribution-xyz-123")


def test_parse_version_orders_correctly():
    assert parse_version("2.4") < parse_version("2.5")


def test_require_returns_sequence_with_version():
    assert require(_PKG)[0].version == importlib.metadata.version(_PKG)


def test_require_accepts_version_specifier():
    # Real pkg_resources.require() accepts PEP 508 requirement strings; the name
    # must be resolved out of the specifier, not passed whole to metadata lookup.
    installed = importlib.metadata.version(_PKG)
    assert require(f"{_PKG}>=0")[0].version == installed


def test_get_distribution_accepts_requirement_specifier():
    installed = importlib.metadata.version(_PKG)
    assert get_distribution(f"{_PKG}>=0").version == installed


def test_get_distribution_unmet_specifier_raises():
    installed = importlib.metadata.version(_PKG)
    with pytest.raises(DistributionNotFound):
        get_distribution(f"{_PKG}!={installed}")


def test_get_distribution_inapplicable_marker_raises():
    # A requirement whose environment marker excludes this interpreter is not
    # applicable, so it must not resolve as if the marker were absent.
    with pytest.raises(DistributionNotFound):
        get_distribution(f"{_PKG}; python_version < '3.0'")


def test_get_distribution_applicable_marker_resolves():
    installed = importlib.metadata.version(_PKG)
    assert get_distribution(f"{_PKG}; python_version >= '3.0'").version == installed


def test_resource_filename_returns_existing_path():
    path = resource_filename("datahub.cli.gql", "fragments.gql")
    assert os.path.exists(path)
    assert path.endswith("fragments.gql")


def test_resource_filename_rejects_traversal():
    with pytest.raises(ValueError):
        resource_filename("datahub.cli.gql", "../evil")
    with pytest.raises(ValueError):
        resource_filename("datahub.cli.gql", "/etc/passwd")


def test_ensure_pkg_resources_installs_usable_module():
    ensure_pkg_resources()
    import pkg_resources

    assert pkg_resources.get_distribution(_PKG).version
    ensure_pkg_resources()  # idempotent, must not raise


def test_shim_is_loud_on_unimplemented_symbol():
    ensure_pkg_resources()
    import pkg_resources

    if not getattr(pkg_resources, "__datahub_shim__", False):
        pytest.skip("real pkg_resources present; shim not active in this env")
    with pytest.raises(AttributeError):
        _ = pkg_resources.working_set


def test_shim_does_not_break_pytest_syspath_prepend(monkeypatch, tmp_path):
    # pytest's syspath_prepend imports fixup_namespace_packages when pkg_resources
    # is in sys.modules; the shim must provide it. Regression test.
    ensure_pkg_resources()
    monkeypatch.syspath_prepend(str(tmp_path))


@pytest.mark.parametrize("mod", ["sqlalchemy_redshift", "sqlalchemy_cockroachdb"])
def test_stranded_dialects_import_after_shim(mod):
    # With the shim installed, these dialects (which import pkg_resources at load)
    # must import cleanly. find_spec doesn't execute the module, so it's safe here.
    ensure_pkg_resources()
    if importlib.util.find_spec(mod) is None:
        pytest.skip(f"{mod} not installed")
    importlib.import_module(mod)  # must not raise


def test_make_shim_exposes_documented_api_and_rejects_others():
    # Exercise the fallback module directly, so its API and the loud __getattr__
    # are covered even in environments where real pkg_resources is present.
    shim = pkg_resources_shim._make_shim()
    assert shim.__datahub_shim__ is True
    assert shim.get_distribution(_PKG).version == importlib.metadata.version(_PKG)
    assert shim.require(_PKG)[0].version == importlib.metadata.version(_PKG)
    assert shim.parse_version("2.5") == parse_version("2.5")
    with pytest.raises(AttributeError):
        _ = shim.working_set  # unimplemented -> module __getattr__ raises


def test_namespace_helpers_are_noops():
    # Both are no-ops for PEP 420; call them for coverage (they return None).
    pkg_resources_shim.declare_namespace("foo.bar")
    pkg_resources_shim.fixup_namespace_packages("/some/path")


def test_ensure_pkg_resources_installs_shim_when_import_fails(monkeypatch):
    # Force the real pkg_resources to be unimportable so the fallback-install
    # branch runs (the py3.12 / setuptools-absent scenario). monkeypatch.delitem
    # snapshots and restores the original entry on teardown.
    monkeypatch.delitem(sys.modules, "pkg_resources", raising=False)
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "pkg_resources":
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    pkg_resources_shim.ensure_pkg_resources()
    assert getattr(sys.modules["pkg_resources"], "__datahub_shim__", False) is True
