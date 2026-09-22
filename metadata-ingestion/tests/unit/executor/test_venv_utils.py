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


class TestVenvUtilsIntegration:
    def test_end_to_end_bundled_version_workflow(self):
        """Test complete workflow with bundled version."""
        plugin = "snowflake"
        version = "bundled"

        # Check version detection
        assert venv_utils.is_bundled_version(version)
        assert venv_utils.should_use_bundled_venv(version)

        venv_name = f"{plugin}-bundled"

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

        venv_name = f"{plugin}-bundled"

        # Check name detection
        assert venv_utils.should_use_bundled_venv_by_name(venv_name)

    def test_end_to_end_dynamic_venv_workflow(self):
        """Test complete workflow with dynamic venv creation."""
        plugin = "snowflake"
        version = "v0.12.1"

        # Check version detection
        assert not venv_utils.is_bundled_version(version)
        assert not venv_utils.should_use_bundled_venv(version)

        venv_name = f"{plugin}-6f1d2c4a9b3e5d70"

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


class TestVenvEntryNameSafety:
    """A cache entry's name reaches the path from unvalidated recipe input.

    `main_plugin` is recipe["source"]["type"], and it is string-concatenated
    into a node-SHARED directory. Before the cache the name only ever indexed
    a per-execution directory that was deleted wholesale, so nothing here
    mattered; now a separator or a `..` breaks two invariants at once.
    """

    CACHE_ROOT = "/tmp/datahub/ingest/_venv_cache"

    @pytest.mark.parametrize(
        "venv_name",
        [
            pytest.param("mysql/x-latest-abc123", id="separator"),
            pytest.param("../../escaped-latest-abc123", id="parent-traversal"),
            pytest.param("a/b/c-latest-abc123", id="nested-separators"),
            pytest.param(".-latest-abc123", id="dot"),
            pytest.param("..", id="bare-parent"),
            pytest.param("sn\x00owflake-latest-abc", id="nul-byte"),
        ],
    )
    def test_an_unsafe_name_stays_a_single_child_of_the_cache_root(
        self, venv_name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two things go wrong when it does not.

        _acquire_cache_entry evicts against `venv_loc.parent`, so a name with
        a separator makes that build trim the wrong directory and never the
        real cache. Worse, any OTHER task's eviction pass, running against the
        real root, sees the intermediate directory as an entry (it starts with
        `venv-`), finds no last-used marker so it sorts FIRST for eviction,
        and takes a `.lock` path nobody holds before rmtree-ing a venv a live
        ingestion is executing from.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", self.CACHE_ROOT)

        loc = pathlib.Path(
            venv_utils.venv_location(venv_name, "/tmp/x", cacheable=True)
        )

        assert loc.parent == pathlib.Path(self.CACHE_ROOT), (
            f"{loc} is not a direct child of the cache root, so eviction and "
            "the lock-path convention both break"
        )
        assert loc.name.startswith(venv_utils.ENTRY_PREFIX), (
            "eviction only considers directories with the entry prefix, so an "
            "entry without it is never reclaimed"
        )
        assert "\x00" not in loc.name

    def test_unsafe_names_do_not_collapse_onto_each_other(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sanitising by replacement alone would map `a/b` and `a_b` -- and
        every recipe whose source type differs only in a stripped character --
        onto one entry, which is the wrong-venv bug the cache key exists to
        prevent."""
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", self.CACHE_ROOT)

        first = venv_utils.venv_location("a/b-latest-abc", "/tmp/x", cacheable=True)
        second = venv_utils.venv_location("a_b-latest-abc", "/tmp/x", cacheable=True)

        assert first != second

    @pytest.mark.parametrize(
        "venv_name",
        ["snowflake-abc123", "snowflake-latest-0123456789abcdef", "mysql-0.15.0.1-ab"],
    )
    def test_an_ordinary_name_is_left_byte_identical(
        self, venv_name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every real source type is already safe, so sanitising must be a
        no-op for them -- otherwise every existing entry is orphaned."""
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", self.CACHE_ROOT)

        loc = venv_utils.venv_location(venv_name, "/tmp/x", cacheable=True)

        assert loc == f"{self.CACHE_ROOT}/{venv_utils.ENTRY_PREFIX}{venv_name}"


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

    def test_last_used_reads_mtime_not_atime(self, tmp_path: pathlib.Path) -> None:
        """LRU orders by mtime, never by filesystem atime.

        Containers mount relatime or noatime, so atime either lags by a day or
        never updates -- an eviction policy reading it looks correct in
        development and evicts the wrong entries in production. Set the two to
        DIFFERENT values: a version reading st_atime returns the wrong one and
        fails here, which a test touching both together cannot detect.
        """
        venv = self._venv_with_python(tmp_path / "venv-x")
        venv_utils.touch_last_used(venv)
        marker = venv / venv_utils.LAST_USED_MARKER

        atime, mtime = 1_000_000.0, 2_000_000.0
        os.utime(marker, (atime, mtime))

        assert venv_utils.last_used_at(venv) == mtime

    def test_last_used_of_an_unmarked_venv_sorts_oldest(
        self, tmp_path: pathlib.Path
    ) -> None:
        """So an entry from before this feature is evicted first, not never."""
        venv = self._venv_with_python(tmp_path / "venv-x")

        assert venv_utils.last_used_at(venv) == 0.0
