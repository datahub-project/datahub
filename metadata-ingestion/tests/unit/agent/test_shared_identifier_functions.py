from typing import List

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
    from datahub.ingestion.source.redshift.config import RedshiftConfig, dataset_name
    from datahub.ingestion.source.redshift.redshift import RedshiftSource
    from datahub.ingestion.source.redshift.redshift_schema import RedshiftTable
    from datahub.ingestion.source.redshift.report import RedshiftReport

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

    # And the other half of "must agree": what Redshift INGESTION filters on.
    #
    # The assertion above pins the probe against dataset_name. It does not
    # pin ingestion to dataset_name, so redshift.py could switch its filter
    # to an inline f-string and this test would keep passing while the two
    # diverged -- not hypothetical, since gen_view_dataset_workunits already
    # builds `f"{database}.{schema}.{view.name}"` by hand a few lines away
    # for its URN.
    #
    # Driven through the real _process_table rather than re-deriving the
    # name, so what is captured is the exact string table_pattern is asked
    # about.
    ingestion_saw: List[str] = []

    class _RecordingPattern:
        def allowed(self, name: str) -> bool:
            ingestion_saw.append(name)
            return False  # drop it; we only want the name it was asked about

    source = RedshiftSource.__new__(RedshiftSource)
    source.config = RedshiftConfig.model_validate(
        {
            "host_port": "h:5439",
            "database": "prod",
            "username": "u",
            "password": "p",
        }
    )
    source.config.table_pattern = _RecordingPattern()  # type: ignore[assignment]
    source.report = RedshiftReport()

    list(
        source._process_table(
            RedshiftTable(
                name="orders",
                schema="public",
                comment=None,
                created=None,
                last_altered=None,
                size_in_bytes=None,
                rows_count=None,
            ),
            database="prod",
        )
    )

    assert ingestion_saw == [dataset_name("prod", "public", "orders")]
    # The two paths, compared directly rather than each against a constant.
    assert ingestion_saw[0] == result.results[0].target

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
