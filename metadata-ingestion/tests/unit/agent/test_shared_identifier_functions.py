from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def test_redshift_and_unity_catalog_probe_hooks_share_one_identifier_function():
    """Ingestion and the probe must agree on the identifier they filter on.

    Redshift used to guarantee that by *routing* through dataset_name in its
    own probe_filter_target. That override is gone -- the framework builds
    `container.schema.entity` for every non-SQLAlchemy SQL source, and
    Qualifier(authoritative=True) is how Redshift says its configured
    database wins -- so the coupling is asserted here instead of indirected
    through a per-connector method. Same protection, no per-connector code:
    if redshift.py ever changes what it filters on, this fails.

    Unity Catalog still routes through its own, because its override does
    something the framework cannot: decline when no single catalog is
    pinned.
    """
    from datahub.ingestion.agent.filter_check import check_filters
    from datahub.ingestion.source.redshift.config import dataset_name

    result = check_filters(
        source_type="redshift",
        config_dict={
            "host_port": "h:5439",
            "database": "prod",
            "username": "u",
            "password": "p",
        },
        kind=str(DatasetSubTypes.TABLE),
        parent_path=["public"],
        names=["orders"],
    )
    assert result.results[0].target == dataset_name("prod", "public", "orders")

    # Unity keeps its override, and it is still the shared builder.
    from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
    from datahub.ingestion.source.unity.proxy_types import qualified_table_name

    unity = UnityCatalogSourceConfig.model_validate(
        {
            "token": "t",
            "workspace_url": "https://example.cloud.databricks.com",
            "catalogs": ["main"],
        }
    )
    assert unity.probe_filter_target(
        schema="s", entity="t", warn=lambda _m: None
    ) == qualified_table_name("main", "s", "t")
