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


class TestIsMovingRequirement:
    """A requirement is "moving" when its text is stable but its resolution is not.

    This is the distinction the venv cache turns on: the cache key is a hash of the requirement
    strings, so a requirement like `pkg==2.1.*` hashes identically forever. Without resolving it
    first, the venv built on the first run is reused on every later run, the index is never
    consulted again, and a newly published release is silently never installed.
    """

    def test_a_range_can_move(self):
        assert venv_utils.is_moving_requirement("acryl-datahub-cloud-docs==2.1.*")
        assert venv_utils.is_moving_requirement("pkg>=1.2")
        assert venv_utils.is_moving_requirement("pkg~=1.2")
        assert venv_utils.is_moving_requirement("pkg>=1.0,<2.0")

    def test_an_unpinned_name_can_move(self):
        assert venv_utils.is_moving_requirement("acryl-datahub-integrations")
        assert venv_utils.is_moving_requirement("pkg[extra]")

    def test_an_exact_pin_cannot_move(self):
        assert not venv_utils.is_moving_requirement("acryl-datahub-cloud-docs==2.1.0.2")
        assert not venv_utils.is_moving_requirement("acryl-datahub[snowflake]==1.7.0.6")

    def test_a_direct_reference_cannot_move(self):
        # One named artifact. Re-resolving these would cost a round trip to learn nothing.
        assert not venv_utils.is_moving_requirement(
            "pkg @ https://example.invalid/pkg.whl"
        )
        assert not venv_utils.is_moving_requirement(
            "acryl-datahub-cloud@/metadata-ingestion-modules/acryl-cloud"
        )

    def test_an_unparseable_line_is_left_alone(self):
        # Better to keep the previous behaviour than to resolve something we cannot reason about.
        assert not venv_utils.is_moving_requirement(
            "--extra-index-url https://example.invalid"
        )
