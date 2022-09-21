import pytest
import test_tableau_common
from freezegun import freeze_time

from datahub.configuration.source_common import DEFAULT_ENV
from datahub.ingestion.source.tableau_common import (
    TableauLineageOverrides,
    make_table_urn,
)

FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = None


@freeze_time(FROZEN_TIME)
@pytest.mark.slow_unit
def test_tableau_ingest(pytestconfig, tmp_path):
    output_file_name: str = "tableau_mces.json"
    golden_file_name: str = "tableau_mces_golden.json"
    side_effect_query_metadata = test_tableau_common.define_query_metadata_func(
        "workbooksConnection_0.json", "workbooksConnection_all.json"
    )
    test_tableau_common.tableau_ingest_common(
        pytestconfig,
        tmp_path,
        side_effect_query_metadata,
        golden_file_name,
        output_file_name,
    )


def test_lineage_overrides():
    # Simple - specify platform instance to presto table
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "presto_catalog",
            "presto",
            "test-schema",
            "presto_catalog.test-schema.test-table",
            platform_instance_map={"presto": "my_presto_instance"},
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # Transform presto urn to hive urn
    # resulting platform instance for hive = mapped platform instance + presto_catalog
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "presto_catalog",
            "presto",
            "test-schema",
            "presto_catalog.test-schema.test-table",
            platform_instance_map={"presto": "my_instance"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"presto": "hive"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:hive,my_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # tranform hive urn to presto urn
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "",
            "hive",
            "test-schema",
            "test-schema.test-table",
            platform_instance_map={"hive": "my_presto_instance.presto_catalog"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"hive": "presto"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )
