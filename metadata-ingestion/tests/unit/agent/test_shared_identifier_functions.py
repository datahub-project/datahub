def test_redshift_and_unity_catalog_probe_hooks_share_one_identifier_function():
    """Redshift and Unity Catalog wire their identifier through a
    SQLCommonConfig.probe_filter_target override (see sql_probe.py) rather
    than a ProbeLevel.filter_target lambda, since they share the generic
    SQL_PROBE Table level with every other SQL connector. The regression this
    guards against: re-inlining `f"{database}.{schema}.{entity}"` (or the
    catalog equivalent) directly in probe_filter_target, instead of calling
    the shared function, would drop its name from the method's bytecode.
    """
    from datahub.ingestion.source.redshift import config as redshift_config
    from datahub.ingestion.source.unity import config as unity_config

    assert "dataset_name" in (
        redshift_config.RedshiftConfig.probe_filter_target.__code__.co_names
    )
    assert "qualified_table_name" in (
        unity_config.UnityCatalogSourceConfig.probe_filter_target.__code__.co_names
    )
