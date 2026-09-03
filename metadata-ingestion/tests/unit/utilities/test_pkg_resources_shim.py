import importlib.metadata
import importlib.util
import os

import pytest

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
