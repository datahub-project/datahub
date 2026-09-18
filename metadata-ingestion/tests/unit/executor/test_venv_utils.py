import os
import pathlib

import pytest

from datahub.executor.execution import venv_utils


class TestVenvUtils:
    def test_is_bundled_version(self):
        """Test bundled version detection."""
        assert venv_utils.is_bundled_version("bundled")

        # Other versions should not be considered bundled
        assert not venv_utils.is_bundled_version("latest")
        assert not venv_utils.is_bundled_version("v0.12.1")
        assert not venv_utils.is_bundled_version("native")

    def test_should_use_bundled_venv(self):
        """Test bundled venv usage decision."""
        assert venv_utils.should_use_bundled_venv("bundled")

        assert not venv_utils.should_use_bundled_venv("latest")
        assert not venv_utils.should_use_bundled_venv("v0.12.1")

    def test_get_venv_name_bundled_version(self):
        """Test venv name generation for bundled versions."""
        # For bundled versions, should return simple plugin-bundled name
        name = venv_utils.get_venv_name("snowflake", "bundled")
        assert name == "snowflake-bundled"

        name = venv_utils.get_venv_name("bigquery", "bundled")
        assert name == "bigquery-bundled"

    def test_get_venv_name_standard_version(self):
        """Test venv name generation for standard versions."""
        # For non-bundled versions, should generate hash-based name
        name1 = venv_utils.get_venv_name("snowflake", "v0.12.1")
        name2 = venv_utils.get_venv_name("snowflake", "v0.12.1")

        # Same inputs should produce same name
        assert name1 == name2
        assert name1.startswith("snowflake-")
        assert len(name1) == len("snowflake-") + 16  # 16 hex chars

        # Different versions should produce different names
        name3 = venv_utils.get_venv_name("snowflake", "v0.12.2")
        assert name1 != name3

        # Different plugins should produce different names
        bigquery_name = venv_utils.get_venv_name("bigquery", "v0.12.1")
        assert name1 != bigquery_name

    def test_get_venv_name_with_extras(self):
        """Test venv name generation with extra requirements and plugins."""
        # Names with different extra requirements should be different
        name1 = venv_utils.get_venv_name("snowflake", "v0.12.1", ["pkg1"])
        name2 = venv_utils.get_venv_name("snowflake", "v0.12.1", ["pkg2"])
        assert name1 != name2

        # Names with different extra plugins should be different
        name3 = venv_utils.get_venv_name(
            "snowflake", "v0.12.1", extra_pip_plugins=["plugin1"]
        )
        name4 = venv_utils.get_venv_name(
            "snowflake", "v0.12.1", extra_pip_plugins=["plugin2"]
        )
        assert name3 != name4

    def test_get_venv_path_dynamic(self):
        """Test venv path generation for dynamic venvs."""
        # For non-bundled venvs, should use tmp_dir structure
        path = venv_utils.get_venv_path("snowflake-abc123", "/tmp/test")
        assert path == "/tmp/test/venv-snowflake-abc123"

    def test_get_venv_path_bundled(self):
        """Test venv path generation for bundled venvs."""
        # Test bundled venv path (must end with -bundled)
        path = venv_utils.get_venv_path("snowflake-bundled", "/tmp/test")
        assert path == "/opt/datahub/venvs/snowflake-bundled"

        # Test non-bundled venv path
        path = venv_utils.get_venv_path("snowflake-abc123", "/tmp/test")
        assert path == "/tmp/test/venv-snowflake-abc123"

    def test_should_use_bundled_venv_by_name_with_bundled_hash(self):
        """Test detection of bundled version venvs by their hash."""
        # Generate a bundled version name
        bundled_name = venv_utils.get_venv_name("snowflake", "bundled")

        # Should detect this as a Bundled venv
        assert venv_utils.should_use_bundled_venv_by_name(bundled_name)

        # Non-bundled names should not be detected as Bundled
        regular_name = venv_utils.get_venv_name("snowflake", "v0.12.1")
        assert not venv_utils.should_use_bundled_venv_by_name(regular_name)

    def test_should_use_bundled_venv_by_name_with_dependency_resolution_disabled(self):
        """Test bundled venv detection by name."""
        # Only bundled venvs should be considered bundled
        assert venv_utils.should_use_bundled_venv_by_name("snowflake-bundled")
        assert venv_utils.should_use_bundled_venv_by_name("bigquery-bundled")
        assert not venv_utils.should_use_bundled_venv_by_name("snowflake-abc123")

    def test_should_use_bundled_venv_validates_compatibility(self):
        """Test that should_use_bundled_venv works correctly."""
        assert venv_utils.should_use_bundled_venv("bundled")

        assert not venv_utils.should_use_bundled_venv("latest")

    def test_is_bundled_venv_name(self):
        """Test detection of bundled venv names."""
        # Generate a bundled venv name
        bundled_name = venv_utils.get_venv_name("snowflake", "bundled")
        assert bundled_name == "snowflake-bundled"

        # Should be detected as bundled by name
        assert venv_utils.should_use_bundled_venv_by_name(bundled_name)

        # Non-bundled names should not be detected as bundled
        regular_name = venv_utils.get_venv_name("snowflake", "v0.12.1")
        assert not venv_utils.should_use_bundled_venv_by_name(regular_name)


class TestVenvUtilsIntegration:
    def test_end_to_end_bundled_version_workflow(self):
        """Test complete workflow with bundled version."""
        plugin = "snowflake"
        version = "bundled"

        # Check version detection
        assert venv_utils.is_bundled_version(version)
        assert venv_utils.should_use_bundled_venv(version)

        # Generate venv name
        venv_name = venv_utils.get_venv_name(plugin, version)
        assert venv_name == "snowflake-bundled"

        # Check name detection
        assert venv_utils.should_use_bundled_venv_by_name(venv_name)

        # Get venv path
        venv_path = venv_utils.get_venv_path(venv_name, "/tmp/test")
        assert venv_path == "/opt/datahub/venvs/snowflake-bundled"

    def test_end_to_end_dependency_resolution_disabled_workflow(self):
        """Test complete workflow when dependency resolution is disabled."""
        plugin = "bigquery"
        version = "bundled"  # Only bundled allowed when resolution disabled

        # Check version detection
        assert venv_utils.is_bundled_version(version)
        assert venv_utils.should_use_bundled_venv(version)

        # Generate venv name
        venv_name = venv_utils.get_venv_name(plugin, version)
        assert venv_name == "bigquery-bundled"

        # Check name detection
        assert venv_utils.should_use_bundled_venv_by_name(venv_name)

    def test_end_to_end_dynamic_venv_workflow(self):
        """Test complete workflow with dynamic venv creation."""
        plugin = "snowflake"
        version = "v0.12.1"

        # Check version detection
        assert not venv_utils.is_bundled_version(version)
        assert not venv_utils.should_use_bundled_venv(version)

        # Generate venv name
        venv_name = venv_utils.get_venv_name(plugin, version)
        assert venv_name.startswith("snowflake-")
        assert len(venv_name) == len("snowflake-") + 16

        # Check name detection
        assert not venv_utils.should_use_bundled_venv_by_name(venv_name)

        # Get venv path
        venv_path = venv_utils.get_venv_path(venv_name, "/tmp/dynamic")
        assert venv_path == f"/tmp/dynamic/venv-{venv_name}"


class TestVenvLocation:
    """Where a venv is built decides whether it can ever be reused.

    A cacheable venv already gets a content-addressed name from
    get_stable_venv_name(); it was the LOCATION -- under the per-execution
    exec_out_dir, deleted in the task's finally -- that made the existing
    "already exists, skip setup" branch in setup_venv unreachable.
    """

    def test_a_cacheable_venv_lands_outside_the_execution_directory(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DATAHUB_VENV_CACHE_PATH", raising=False)
        exec_out_dir = "/tmp/datahub/ingest/exec-123"

        loc = venv_utils.venv_location("snowflake-abc123", exec_out_dir, cacheable=True)

        assert exec_out_dir not in loc, (
            "a cacheable venv under exec_out_dir is deleted with it, which is "
            "why the reuse branch never fired"
        )

    def test_an_ephemeral_venv_stays_in_the_execution_directory(self) -> None:
        exec_out_dir = "/tmp/datahub/ingest/exec-123"

        loc = venv_utils.venv_location("eph-deadbeef", exec_out_dir, cacheable=False)

        assert loc == f"{exec_out_dir}/venv-eph-deadbeef"

    def test_bundled_still_resolves_to_the_shared_image_path(self) -> None:
        """Unchanged behaviour: bundled venvs are pre-built, never created."""
        loc = venv_utils.venv_location(
            "snowflake-bundled", "/tmp/whatever", cacheable=False
        )

        assert loc == "/opt/datahub/venvs/snowflake-bundled"

    def test_the_cache_root_is_overridable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", "/mnt/cache")

        loc = venv_utils.venv_location("snowflake-abc", "/tmp/x", cacheable=True)

        assert loc == "/mnt/cache/venv-snowflake-abc"

    def test_the_cache_is_a_sibling_of_the_execution_directory(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Not inside it. exec_out_dir is what finalize_task_output deletes,
        so a cache underneath it would be destroyed with every run -- the
        exact problem this feature exists to fix."""
        monkeypatch.delenv("DATAHUB_VENV_CACHE_PATH", raising=False)

        loc = venv_utils.venv_location(
            "snowflake-abc", "/tmp/datahub/ingest/exec-123", cacheable=True
        )

        assert loc == "/tmp/datahub/ingest/_venv_cache/venv-snowflake-abc"


class TestVenvEntryState:
    """A half-built venv must never be reused.

    setup_venv's reuse check accepts any directory containing bin/python. A
    build killed midway -- pod evicted, OOM, cancellation -- leaves exactly
    that: a venv with no packages. Per-run directories made this harmless
    because they were deleted; a cache turns it into a poisoned entry every
    later run reuses.
    """

    def _venv_with_python(self, root: pathlib.Path) -> pathlib.Path:
        (root / "bin").mkdir(parents=True)
        (root / "bin" / "python").touch()
        return root

    def test_a_venv_with_a_python_binary_but_no_marker_is_incomplete(
        self, tmp_path: pathlib.Path
    ) -> None:
        venv = self._venv_with_python(tmp_path / "venv-x")

        assert not venv_utils.is_venv_complete(venv), (
            "this is exactly the shape a killed build leaves behind"
        )

    def test_marking_complete_makes_it_reusable(self, tmp_path: pathlib.Path) -> None:
        venv = self._venv_with_python(tmp_path / "venv-x")

        venv_utils.mark_venv_complete(venv)

        assert venv_utils.is_venv_complete(venv)

    def test_a_marker_without_a_python_binary_is_still_incomplete(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Both conditions, not either: some systems clear files out of temp
        directories but leave the directories themselves."""
        venv = tmp_path / "venv-x"
        venv.mkdir()
        venv_utils.mark_venv_complete(venv)

        assert not venv_utils.is_venv_complete(venv)

    def test_last_used_advances_on_touch(self, tmp_path: pathlib.Path) -> None:
        """LRU orders by THIS file, never by filesystem atime: containers mount
        relatime or noatime, so atime lags by a day or never updates."""
        venv = self._venv_with_python(tmp_path / "venv-x")

        venv_utils.touch_last_used(venv)
        first = venv_utils.last_used_at(venv)
        os.utime(venv / venv_utils.LAST_USED_MARKER, (first - 500, first - 500))
        venv_utils.touch_last_used(venv)

        assert venv_utils.last_used_at(venv) > first - 500

    def test_last_used_of_an_unmarked_venv_sorts_oldest(
        self, tmp_path: pathlib.Path
    ) -> None:
        """So an entry from before this feature is evicted first, not never."""
        venv = self._venv_with_python(tmp_path / "venv-x")

        assert venv_utils.last_used_at(venv) == 0.0
