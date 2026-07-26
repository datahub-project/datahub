import inspect
from typing import Any, Callable

import datahub.ingestion.source.aerospike as aerospike
import datahub.ingestion.source.aerospike_probe as aerospike_probe
import datahub.ingestion.source.aws.glue as glue
import datahub.ingestion.source.aws.glue_probe as glue_probe
import datahub.ingestion.source.cassandra.cassandra as cassandra
import datahub.ingestion.source.cassandra.cassandra_probe as cassandra_probe
import datahub.ingestion.source.iceberg.iceberg as iceberg
import datahub.ingestion.source.iceberg.iceberg_probe as iceberg_probe
import datahub.ingestion.source.mongodb as mongodb
import datahub.ingestion.source.mongodb_probe as mongodb_probe
from datahub.ingestion.agent.probe import ClientProbe
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _table_level_filter_target(probe: ClientProbe) -> Callable[[Any], str]:
    for level in probe._all_levels:
        if level.kind == DatasetSubTypes.TABLE:
            assert level.filter_target is not None, (
                f"the Table level of {probe} has no filter_target set"
            )
            return level.filter_target
    raise AssertionError("no Table level found on this probe")


def test_non_sql_connectors_share_one_identifier_function():
    """Both the ingestion path and the probe must route through dataset_name().

    Re-inlining the shared expression in either module is the one realistic
    way this regresses: it would either drop the module-level dataset_name
    (failing the hasattr/identity checks below) or stop the Table level's
    filter_target lambda from closing over it (failing the closure check).
    """
    connectors = [
        (cassandra, cassandra_probe, cassandra_probe.CASSANDRA_PROBE),
        (glue, glue_probe, glue_probe.GLUE_PROBE),
        (iceberg, iceberg_probe, iceberg_probe.ICEBERG_PROBE),
        (mongodb, mongodb_probe, mongodb_probe.MONGODB_PROBE),
        (aerospike, aerospike_probe, aerospike_probe.AEROSPIKE_PROBE),
    ]
    for ingestion_module, probe_module, probe in connectors:
        assert hasattr(ingestion_module, "dataset_name"), (
            f"{ingestion_module.__name__} must define a module-level "
            "dataset_name() that ingestion filters against"
        )
        assert probe_module.dataset_name is ingestion_module.dataset_name, (
            f"{probe_module.__name__} must import the exact dataset_name "
            f"function {ingestion_module.__name__} filters with, not a "
            "reimplementation of it"
        )
        filter_target = _table_level_filter_target(probe)
        closure_globals = inspect.getclosurevars(filter_target).globals
        assert closure_globals.get("dataset_name") is ingestion_module.dataset_name, (
            f"{probe_module.__name__}'s Table level.filter_target must close "
            f"over {ingestion_module.__name__}.dataset_name"
        )


def test_redshift_and_unity_catalog_probe_hooks_share_one_identifier_function():
    """Redshift and Unity Catalog wire their identifier through a
    SQLCommonConfig.probe_filter_target override (see sql_probe.py) rather
    than a ProbeLevel.filter_target lambda, since they share the generic
    SQL_PROBE Table level with every other SQL connector. The regression this
    guards against is the same one as above: re-inlining `f"{database}."
    f"{schema}.{entity}"` (or the catalog equivalent) directly in
    probe_filter_target, instead of calling the shared function, would drop
    its name from the method's bytecode.
    """
    from datahub.ingestion.source.redshift import config as redshift_config
    from datahub.ingestion.source.unity import config as unity_config

    assert "dataset_name" in (
        redshift_config.RedshiftConfig.probe_filter_target.__code__.co_names
    )
    assert "qualified_table_name" in (
        unity_config.UnityCatalogSourceConfig.probe_filter_target.__code__.co_names
    )
