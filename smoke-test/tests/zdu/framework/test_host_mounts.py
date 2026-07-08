"""Unit tests for worktree_mount_env (G20c-Option-A)."""

from __future__ import annotations

import pathlib

import pytest

from tests.zdu.framework.host_mounts import worktree_mount_env


def _make_worktree(root: pathlib.Path, side: str) -> pathlib.Path:
    """Materialize a fake worktree skeleton with all three mount-source dirs."""
    wt = root / side
    (wt / "datahub-upgrade" / "build" / "libs").mkdir(parents=True)
    (wt / "metadata-models" / "src" / "main" / "resources").mkdir(parents=True)
    (wt / "metadata-service" / "war" / "build" / "libs").mkdir(parents=True)
    return wt


class TestWorktreeMountEnv:
    def test_returns_env_dict_when_all_dirs_present(
        self, tmp_path: pathlib.Path
    ) -> None:
        repo_root = tmp_path
        build_root = "zdu-images"
        (tmp_path / build_root).mkdir()
        wt = _make_worktree(tmp_path / build_root, "old")

        env = worktree_mount_env(repo_root, build_root, "old")
        assert env is not None
        assert (
            env["DATAHUB_UPGRADE_BIN_HOST_DIR"] == f"{wt}/datahub-upgrade/build/libs/"
        )
        assert (
            env["DATAHUB_MODELS_RESOURCES_HOST_DIR"]
            == f"{wt}/metadata-models/src/main/resources/"
        )
        assert (
            env["DATAHUB_GMS_WAR_HOST_DIR"] == f"{wt}/metadata-service/war/build/libs/"
        )

    def test_returns_none_when_worktree_missing(self, tmp_path: pathlib.Path) -> None:
        # No worktree dir at all → None (skip-build flow uses YAML defaults).
        env = worktree_mount_env(tmp_path, "zdu-images", "old")
        assert env is None

    def test_returns_none_when_any_mount_source_missing(
        self, tmp_path: pathlib.Path
    ) -> None:
        # Worktree exists but build hasn't materialized. Fail-safe: don't
        # half-mount OLD jar + dev-tree PDL — that mismatch would silently
        # corrupt two-image semantics.
        build_root = "zdu-images"
        (tmp_path / build_root / "new").mkdir(parents=True)
        # Create upgrade libs but not models or war.
        (tmp_path / build_root / "new" / "datahub-upgrade" / "build" / "libs").mkdir(
            parents=True
        )

        env = worktree_mount_env(tmp_path, build_root, "new")
        assert env is None

    def test_rejects_invalid_side(self, tmp_path: pathlib.Path) -> None:
        with pytest.raises(ValueError, match="side must be 'old' or 'new'"):
            worktree_mount_env(tmp_path, "zdu-images", "older")

    def test_paths_have_trailing_slash(self, tmp_path: pathlib.Path) -> None:
        # Compose YAML mount syntax ends in `/`; keep override identical so
        # log lines remain comparable.
        build_root = "zdu-images"
        (tmp_path / build_root).mkdir()
        _make_worktree(tmp_path / build_root, "new")
        env = worktree_mount_env(tmp_path, build_root, "new")
        assert env is not None
        for v in env.values():
            assert v.endswith("/"), v
