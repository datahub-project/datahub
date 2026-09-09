from datahub.ingestion.source.source_registry import source_registry


def test_source_is_registered_under_its_recipe_type():
    source_class = source_registry.get("snowflake-openflow")
    assert source_class.__name__ == "SnowflakeOpenflowSource"


def test_registered_class_exposes_its_config():
    source_class = source_registry.get("snowflake-openflow")
    # get_config_class is added to the class dynamically by @config_class, so it
    # is invisible to mypy's static view of the registry's declared Type[Source].
    assert (
        source_class.get_config_class().__name__  # type: ignore[attr-defined]
        == "SnowflakeOpenflowSourceConfig"
    )
