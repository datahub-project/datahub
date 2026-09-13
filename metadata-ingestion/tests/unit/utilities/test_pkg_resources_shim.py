import importlib
import importlib.metadata
import importlib.util
import os
import sys

import pytest

from datahub.utilities import pkg_resources_shim
from datahub.utilities.pkg_resources_shim import (
    DistributionNotFound,
    get_distribution,
    parse_version,
    require,
    resource_filename,
)

_PKG = "acryl-datahub"


# --- shim API (exercised via the module directly) ---


def test_get_distribution_returns_version_and_parsed_version():
    dist = get_distribution(_PKG)
    assert dist.version == importlib.metadata.version(_PKG)
    assert dist.parsed_version == parse_version(dist.version)


def test_get_distribution_missing_raises():
    with pytest.raises(DistributionNotFound):
        get_distribution("no-such-distribution-xyz-123")


def test_require_returns_sequence_with_version():
    assert require(_PKG)[0].version == importlib.metadata.version(_PKG)


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


def test_resource_filename_accepts_submodule_anchor():
    # The real redshift call is a *submodule* anchor (no __path__); exercise that
    # branch against the dialect this shim exists to support.
    if importlib.util.find_spec("sqlalchemy_redshift") is None:
        pytest.skip("sqlalchemy_redshift not installed")
    path = resource_filename("sqlalchemy_redshift.dialect", "redshift-ca-bundle.crt")
    assert os.path.exists(path)


def test_resource_filename_rejects_traversal():
    with pytest.raises(ValueError):
        resource_filename("datahub.cli.gql", "../evil")
    with pytest.raises(ValueError):
        resource_filename("datahub.cli.gql", "/etc/passwd")


def test_shim_is_loud_on_unimplemented_symbol():
    # PEP 562 module __getattr__ names the implemented surface for anything else.
    with pytest.raises(AttributeError):
        _ = pkg_resources_shim.working_set


# --- the sys.meta_path finder ---


def test_finder_installed_once_and_targets_only_pkg_resources():
    from datahub._pkg_resources_finder import _PkgResourcesShimFinder

    finders = [f for f in sys.meta_path if isinstance(f, _PkgResourcesShimFinder)]
    assert len(finders) == 1  # installed once, no duplicates
    assert finders[0].find_spec("pkg_resources", None) is not None
    assert finders[0].find_spec("something_else", None) is None


def test_finder_spec_loads_a_working_shim():
    from datahub._pkg_resources_finder import _PkgResourcesShimFinder

    spec = _PkgResourcesShimFinder().find_spec("pkg_resources", None)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.__datahub_shim__ is True
    assert module.get_distribution(_PKG).version == importlib.metadata.version(_PKG)
    with pytest.raises(AttributeError):
        _ = module.working_set


@pytest.mark.parametrize("mod", ["sqlalchemy_redshift", "sqlalchemy_cockroachdb"])
def test_stranded_dialects_import(mod):
    # These dialects import pkg_resources at load; with the finder installed
    # (via `import datahub`, done at conftest load) they must import cleanly.
    if importlib.util.find_spec(mod) is None:
        pytest.skip(f"{mod} not installed")
    importlib.import_module(mod)


def test_shim_does_not_break_pytest_syspath_prepend(monkeypatch, tmp_path):
    # pytest's syspath_prepend calls pkg_resources.fixup_namespace_packages when
    # pkg_resources is imported; the shim must provide it. Regression test.
    import pkg_resources  # noqa: F401

    monkeypatch.syspath_prepend(str(tmp_path))
