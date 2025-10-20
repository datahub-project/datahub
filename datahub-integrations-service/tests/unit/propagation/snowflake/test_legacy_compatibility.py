"""
Unit tests for backward compatibility of SnowflakeTagPropagatorAction.

Ensures that old automations using the full class path continue to work.
"""


class TestLegacyCompatibility:
    """Test backward compatibility for old automation configurations."""

    def test_tag_propagator_action_imports(self) -> None:
        """Test that SnowflakeTagPropagatorAction can be imported from old path."""
        # This is how the action registry dynamically imports the class
        from datahub_integrations.propagation.snowflake.tag_propagator import (
            SnowflakeTagPropagatorAction,
        )

        assert SnowflakeTagPropagatorAction is not None
        assert hasattr(SnowflakeTagPropagatorAction, "__init__")
        assert hasattr(SnowflakeTagPropagatorAction, "act")

    def test_tag_propagator_config_imports(self) -> None:
        """Test that SnowflakeTagPropagatorConfig can be imported from old path."""
        from datahub_integrations.propagation.snowflake.tag_propagator import (
            SnowflakeTagPropagatorConfig,
        )

        assert SnowflakeTagPropagatorConfig is not None

    def test_tag_propagator_is_alias_for_metadata_sync(self) -> None:
        """
        Test that SnowflakeTagPropagatorAction is actually an alias
        for SnowflakeMetadataSyncAction.
        """
        from datahub_integrations.propagation.snowflake.metadata_sync_action import (
            SnowflakeMetadataSyncAction,
            SnowflakeMetadataSyncConfig,
        )
        from datahub_integrations.propagation.snowflake.tag_propagator import (
            SnowflakeTagPropagatorAction,
            SnowflakeTagPropagatorConfig,
        )

        # Verify they are the exact same class objects
        assert SnowflakeTagPropagatorAction is SnowflakeMetadataSyncAction
        assert SnowflakeTagPropagatorConfig is SnowflakeMetadataSyncConfig

    def test_full_import_path_works(self) -> None:
        """
        Test that the full import path works as it would be used by the
        action registry's dynamic import mechanism.
        """
        import datahub_integrations.propagation.snowflake.tag_propagator

        # This is how the registry accesses the class after importing the module
        action_class = datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagPropagatorAction

        assert action_class is not None
        assert action_class.__name__ == "SnowflakeMetadataSyncAction"
        assert action_class.__module__ == (
            "datahub_integrations.propagation.snowflake.metadata_sync_action"
        )

    def test_config_class_attributes(self) -> None:
        """Test that the config class has the expected attributes."""
        from datahub_integrations.propagation.snowflake.tag_propagator import (
            SnowflakeTagPropagatorConfig,
        )

        # Verify the config class has the expected fields
        # Use model_fields (Pydantic V2) or __fields__ (Pydantic V1) for compatibility
        fields = getattr(
            SnowflakeTagPropagatorConfig,
            "model_fields",
            getattr(SnowflakeTagPropagatorConfig, "__fields__", {}),
        )

        # Check for key fields that old automations would use
        assert "snowflake" in fields
        assert "tag_propagation" in fields
        assert "term_propagation" in fields
        # New field should also be present
        assert "description_sync" in fields

    def test_action_class_methods(self) -> None:
        """Test that the action class has the expected methods."""
        from datahub_integrations.propagation.snowflake.tag_propagator import (
            SnowflakeTagPropagatorAction,
        )

        # Verify the action class has the expected methods
        assert hasattr(SnowflakeTagPropagatorAction, "act")
        assert hasattr(SnowflakeTagPropagatorAction, "create")
        assert hasattr(SnowflakeTagPropagatorAction, "name")
        assert hasattr(SnowflakeTagPropagatorAction, "close")
        assert hasattr(SnowflakeTagPropagatorAction, "process_directive")
        assert hasattr(SnowflakeTagPropagatorAction, "bootstrap_asset")
        assert hasattr(SnowflakeTagPropagatorAction, "rollback_asset")

    def test_module_exports(self) -> None:
        """Test that the module exports the expected symbols."""
        import datahub_integrations.propagation.snowflake.tag_propagator as module

        # Check __all__ exports
        assert hasattr(module, "__all__")
        assert "SnowflakeTagPropagatorAction" in module.__all__
        assert "SnowflakeTagPropagatorConfig" in module.__all__

        # Verify exports are accessible
        assert hasattr(module, "SnowflakeTagPropagatorAction")
        assert hasattr(module, "SnowflakeTagPropagatorConfig")
