import functools
import logging

import pytest
import test_tableau_common
from test_tableau_common import _read_response
from freezegun import freeze_time

from datahub.configuration.source_common import DEFAULT_ENV
from datahub.ingestion.source.tableau_common import (
    TableauLineageOverrides,
    make_table_urn,
)

FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = None


@freeze_time(FROZEN_TIME)
#@pytest.mark.slow_unit
def test_tableau_ingest(pytestconfig, tmp_path):
    import gc
    gc.collect()
    objects = [i for i in gc.get_objects()
               if isinstance(i, functools._lru_cache_wrapper)]

    for object in objects:
        object.cache_clear()

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


def side_effect_query_metadata2(query):

    if "workbooksConnection (first:0" in query:
        return _read_response("workbooksConnection_0.json")

    if "workbooksConnection (first:3" in query:
        return _read_response("workbooksConnection_state_all.json")

    if "embeddedDatasourcesConnection (first:0" in query:
        return _read_response("embeddedDatasourcesConnection_0.json")

    if "embeddedDatasourcesConnection (first:8" in query:
        return _read_response("embeddedDatasourcesConnection_all.json")

    if "publishedDatasourcesConnection (first:0" in query:
        return _read_response("publishedDatasourcesConnection_0.json")

    if "publishedDatasourcesConnection (first:2" in query:
        return _read_response("publishedDatasourcesConnection_all.json")

    if "customSQLTablesConnection (first:0" in query:
        return _read_response("customSQLTablesConnection_0.json")

    if "customSQLTablesConnection (first:2" in query:
        return _read_response("customSQLTablesConnection_all.json")


@freeze_time(FROZEN_TIME)
#@pytest.mark.slow_unit
def test_tableau_usage_stat(pytestconfig, tmp_path):
    import gc
    gc.collect()
    objects = [i for i in gc.get_objects()
               if isinstance(i, functools._lru_cache_wrapper)]

    for object in objects:
        object.cache_clear()

    print("Mohd objects {}".format(len(objects)))

    output_file_name: str = "tableau_stat_mces.json"
    golden_file_name: str = "tableau_state_mces_golden.json"
    func = side_effect_query_metadata2
    test_tableau_common.tableau_ingest_common(
        pytestconfig,
        "/tmp",
        func,
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

