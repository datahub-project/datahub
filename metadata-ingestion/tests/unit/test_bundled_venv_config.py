import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SNIPPETS = _REPO_ROOT / "docker" / "snippets" / "ingestion"
sys.path.insert(0, str(_SNIPPETS))

from build_bundled_venvs_unified import (  # noqa: E402
    _TLSV1_COMPAT_MODULE,
    _TLSV1_COMPAT_MODULE_NAME,
    _TLSV1_COMPAT_SENTINEL,
    _install_tlsv1_compat_shim,
)
from bundled_venv_config import (  # noqa: E402
    BundledVenvGroupPlan,
    alias_cluster,
    build_group_plans,
    bundled_venv_symlink_names,
    canonical_plugin,
    extras_to_install_string,
    groups_config_from_plugin_group_env,
    plugin_extras_for_plugin,
    sorted_union_extras,
)


def test_canonical_plugin_resolves_databricks_alias() -> None:
    assert canonical_plugin("databricks") == "unity-catalog"
    assert canonical_plugin("unity-catalog") == "unity-catalog"


def test_alias_cluster_unity_catalog_and_databricks() -> None:
    assert alias_cluster("unity-catalog") == frozenset({"unity-catalog", "databricks"})


def test_alias_cluster_hive_metastore_and_presto_on_hive() -> None:
    assert alias_cluster("hive-metastore") == frozenset(
        {"hive-metastore", "presto-on-hive"}
    )


def test_plugin_extras_databricks_uses_unity_catalog_extra() -> None:
    assert plugin_extras_for_plugin(
        "databricks", slim_mode=False
    ) == plugin_extras_for_plugin("unity-catalog", slim_mode=False)


def test_bundled_venv_symlink_names_includes_aliases_for_singleton() -> None:
    plan = BundledVenvGroupPlan(
        label="unity-catalog",
        members=("unity-catalog",),
        extras=("unity-catalog",),
        canonical_dir_name="unity-catalog-bundled",
    )
    assert bundled_venv_symlink_names(plan) == (
        "databricks-bundled",
        "unity-catalog-bundled",
    )


def test_bundled_venv_symlink_names_includes_aliases_for_group_member() -> None:
    plan = BundledVenvGroupPlan(
        label="common",
        members=("s3", "unity-catalog"),
        extras=("s3", "unity-catalog"),
        canonical_dir_name="common-venv",
    )
    names = bundled_venv_symlink_names(plan)
    assert "databricks-bundled" in names
    assert "unity-catalog-bundled" in names
    assert "s3-bundled" in names


def test_build_group_plans_rejects_alias_duplicates_in_plugin_list() -> None:
    with pytest.raises(ValueError, match="lists aliases"):
        build_group_plans(["unity-catalog", "databricks"], None, slim_mode=False)


def test_plugin_extras_dedupes_file_plugin_extra() -> None:
    extras = plugin_extras_for_plugin("file", slim_mode=False)
    assert extras.count("file") == 1
    assert "s3" in extras


def test_sorted_union_s3_and_file_full_mode() -> None:
    u = sorted_union_extras(["s3", "file"], slim_mode=False)
    assert "datahub-rest" in u
    assert "s3" in u
    assert "file" in u


def test_sorted_union_slim_uses_s3_slim() -> None:
    u = sorted_union_extras(["s3", "file"], slim_mode=True)
    assert "s3-slim" in u
    assert "s3" not in u


def test_groups_config_from_env_suffix_is_lowercased_group_id() -> None:
    cfg = groups_config_from_plugin_group_env(
        {"BUNDLED_VENV_PLUGINS_COMMON": "s3,demo-data,file"}
    )
    assert cfg == {"groups": {"common": {"plugins": ["s3", "demo-data", "file"]}}}


def test_groups_config_from_env_multiple_groups() -> None:
    cfg = groups_config_from_plugin_group_env(
        {
            "BUNDLED_VENV_PLUGINS_COMMON": "s3,file",
            "BUNDLED_VENV_PLUGINS_GC_DOCS": "datahub-gc,datahub-documents",
        }
    )
    assert set(cfg["groups"].keys()) == {"common", "gc_docs"}


def test_groups_config_aux_primary_from_env() -> None:
    cfg = groups_config_from_plugin_group_env(
        {
            "BUNDLED_VENV_PLUGINS_COMMON": "s3,file",
            "BUNDLED_VENV_AUX_PRIMARY_COMMON": "file",
        }
    )
    assert cfg["groups"]["common"]["aux_primary_plugin"] == "file"


def test_groups_config_aux_primary_unknown_group_raises() -> None:
    with pytest.raises(ValueError, match="no .* group was defined"):
        groups_config_from_plugin_group_env({"BUNDLED_VENV_AUX_PRIMARY_COMMON": "s3"})


def test_build_group_plan_carries_aux_primary() -> None:
    cfg = {
        "groups": {
            "common": {
                "plugins": ["s3", "file"],
                "aux_primary_plugin": "file",
            }
        }
    }
    plans = build_group_plans(["s3", "file"], cfg, slim_mode=False)
    assert len(plans) == 1
    assert plans[0].aux_primary_plugin == "file"


def test_groups_config_from_env_empty_means_no_groups() -> None:
    assert groups_config_from_plugin_group_env({"PATH": "/usr/bin"}) is None


def test_groups_config_from_env_duplicate_label_after_lower_raises() -> None:
    with pytest.raises(ValueError, match="Duplicate bundled venv group"):
        groups_config_from_plugin_group_env(
            {
                "BUNDLED_VENV_PLUGINS_COMMON": "s3",
                "BUNDLED_VENV_PLUGINS_common": "file",
            }
        )


def test_build_group_plans_all_plugins_in_common() -> None:
    cfg = {
        "groups": {
            "common": {
                "plugins": [
                    "s3",
                    "demo-data",
                    "file",
                    "datahub-gc",
                    "datahub-documents",
                ]
            }
        }
    }
    plugins = [
        "s3",
        "demo-data",
        "file",
        "datahub-gc",
        "datahub-documents",
    ]
    plans = build_group_plans(plugins, cfg, slim_mode=False)
    assert len(plans) == 1
    assert plans[0].label == "common"
    assert plans[0].canonical_dir_name == "common-venv"
    assert set(plans[0].members) == set(plugins)


def test_build_group_plans_singleton_when_plugin_not_in_any_group() -> None:
    cfg = {"groups": {"common": {"plugins": ["s3", "file"]}}}
    plugins = ["s3", "demo-data", "file"]
    plans = build_group_plans(plugins, cfg, slim_mode=False)
    by_label = {p.label: p for p in plans}
    assert set(by_label["common"].members) == {"s3", "file"}
    assert by_label["demo-data"].canonical_dir_name == "demo-data-bundled"
    assert by_label["demo-data"].members == ("demo-data",)


def test_build_group_plans_duplicate_membership_errors() -> None:
    cfg = {"groups": {"a": {"plugins": ["s3", "file"]}, "b": {"plugins": ["s3"]}}}
    with pytest.raises(ValueError, match="more than one group"):
        build_group_plans(["s3", "file"], cfg, slim_mode=False)


def test_build_group_plans_unknown_plugin_in_group_errors() -> None:
    cfg = {"groups": {"bad": {"plugins": ["not-a-plugin"]}}}
    with pytest.raises(ValueError, match="not in BUNDLED_VENV_PLUGINS"):
        build_group_plans(["s3"], cfg, slim_mode=False)


def test_primary_plugin_override() -> None:
    cfg = {"groups": {"common": {"plugins": ["s3", "file"], "primary_plugin": "s3"}}}
    plugins = ["s3", "file"]
    plans = build_group_plans(plugins, cfg, slim_mode=False)
    assert len(plans) == 1
    assert extras_to_install_string(plans[0].extras) == extras_to_install_string(
        sorted_union_extras(["s3"], slim_mode=False)
    )


def test_plugin_alias_symlink_uses_relative_canonical_name() -> None:
    """Matches ensure_plugin_symlinks: sibling dirs use a single-path-component target."""
    base = "/opt/datahub/venvs"
    canonical_path = os.path.join(base, "common-venv")
    link_path = os.path.join(base, "s3-bundled")
    target_rel = os.path.relpath(canonical_path, start=os.path.dirname(link_path))
    assert target_rel == "common-venv"


def test_explicit_extras_override() -> None:
    cfg = {
        "groups": {
            "x": {
                "plugins": ["s3", "file"],
                "extras": ["datahub-rest", "datahub-kafka", "file", "s3"],
            }
        }
    }
    plugins = ["s3", "file"]
    plans = build_group_plans(plugins, cfg, slim_mode=False)
    assert set(plans[0].extras) == {"datahub-rest", "datahub-kafka", "file", "s3"}


# ---------------------------------------------------------------------------
# TLSv1 compat shim tests
# ---------------------------------------------------------------------------


def test_tlsv1_compat_module_uses_unique_sentinel() -> None:
    """The shim must not alias PROTOCOL_TLSv1 to PROTOCOL_TLS, which would
    collide with the existing key in vendored urllib3's _openssl_versions dict
    and silently force TLSv1-only negotiation for all connections."""
    assert _TLSV1_COMPAT_SENTINEL != 2  # ssl.PROTOCOL_TLS is 2
    assert f"ssl.PROTOCOL_TLSv1 = {_TLSV1_COMPAT_SENTINEL}" in _TLSV1_COMPAT_MODULE
    # The module must not reference ssl.PROTOCOL_TLS as the assigned value
    assert "ssl.PROTOCOL_TLSv1 = ssl.PROTOCOL_TLS" not in _TLSV1_COMPAT_MODULE


def test_tlsv1_compat_module_guards_openssl_import() -> None:
    """The shim must not crash if pyOpenSSL is absent from the venv."""
    assert "except ImportError" in _TLSV1_COMPAT_MODULE
    assert "TLSv1_METHOD" in _TLSV1_COMPAT_MODULE


def test_tlsv1_compat_shim_noop_when_protocol_tlsv1_exists(tmp_path: Path) -> None:
    """When the venv's interpreter already has PROTOCOL_TLSv1 (OpenSSL 3.x),
    no shim files should be written."""
    venv_path = tmp_path / "venv"
    venv_path.mkdir()

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = (
            f"{venv_path}/lib/python3.11/site-packages\nTrue\n"
        )
        mock_run.return_value.returncode = 0
        _install_tlsv1_compat_shim(str(venv_path))

    site_packages = venv_path / "lib" / "python3.11" / "site-packages"
    assert not (site_packages / f"{_TLSV1_COMPAT_MODULE_NAME}.py").exists()
    assert not (site_packages / f"{_TLSV1_COMPAT_MODULE_NAME}.pth").exists()


def test_tlsv1_compat_shim_writes_files_when_protocol_missing(tmp_path: Path) -> None:
    """When the venv's interpreter lacks PROTOCOL_TLSv1 (OpenSSL 4.0), the
    shim module and .pth file must be written to site-packages."""
    venv_path = tmp_path / "venv"
    site_packages = venv_path / "lib" / "python3.11" / "site-packages"
    site_packages.mkdir(parents=True)

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = f"{site_packages}\nFalse\n"
        mock_run.return_value.returncode = 0
        _install_tlsv1_compat_shim(str(venv_path))

    module_file = site_packages / f"{_TLSV1_COMPAT_MODULE_NAME}.py"
    pth_file = site_packages / f"{_TLSV1_COMPAT_MODULE_NAME}.pth"
    assert module_file.exists()
    assert pth_file.exists()
    assert module_file.read_text() == _TLSV1_COMPAT_MODULE
    assert pth_file.read_text() == f"import {_TLSV1_COMPAT_MODULE_NAME}\n"
