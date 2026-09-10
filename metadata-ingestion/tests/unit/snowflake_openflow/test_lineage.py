import copy
import gzip
import json
import pathlib
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest
from snowflake.connector.errors import OperationalError, ProgrammingError

from datahub.ingestion.source.snowflake import snowflake_openflow
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    CONFIG_FILENAME,
    CONNECTOR_DEFINITION_PLATFORM,
    SCHEMA_STRATEGY_SOURCE_SCHEMA,
    ConnectorTableLineage,
    SnowflakeOpenflowSource,
    _canvas_url,
    _parse_table_names,
    _url_shape,
    destination_identifier,
    parse_connector_config,
    property_value,
    upstream_identifier,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_config import (
    SnowflakeOpenflowSourceConfig,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_models import (
    OpenflowConnector,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_query import (
    CONNECTOR_HISTORY,
    DEPLOYMENT_HISTORY,
    RUNTIME_HISTORY,
    SnowflakeOpenflowQuery,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_report import (
    SnowflakeOpenflowReport,
)
from datahub.ingestion.workunit_processors.auto_lowercase_urns import (
    AutoLowercaseUrnsProcessor,
)


def _wrap(value: Optional[str], value_type: str = "STRING_LITERAL") -> Dict[str, Any]:
    # Mirrors Openflow's real property wrapper: {"valueType": ..., "value": ...}.
    # An unset property (value=None) omits the "value" key entirely, matching
    # how a real connector represents e.g. an unused Destination Schema Suffix
    # -- not `{"value": null}` and not `{"value": ""}`.
    wrapped: Dict[str, Any] = {"valueType": value_type}
    if value is not None:
        wrapped["value"] = value
    return wrapped


CONFIG_JSON = {
    "configuration": [
        {
            "name": "Source",
            "properties": {
                "JDBC URL": _wrap("jdbc:postgresql://host:5432/mysourcedb"),
                "Postgres Username": _wrap("repl"),
            },
        },
        {
            "name": "Replication table schema",
            "properties": {
                "Included Comma Separated Source Table Names": _wrap(
                    '"public"."mytable"'
                )
            },
        },
        {
            "name": "Destination details",
            "properties": {
                "Snowflake Destination Database": _wrap("MY_DB"),
                "Destination Schema Strategy": _wrap("SOURCE_SCHEMA"),
                "Object Identifier Resolution": _wrap("CASE_INSENSITIVE"),
            },
        },
    ]
}


def test_parses_source_tables_and_destination():
    lineage = parse_connector_config(CONFIG_JSON)
    assert lineage.source_tables == [("public", "mytable")]
    assert lineage.destination_database == "MY_DB"
    assert lineage.schema_strategy == SCHEMA_STRATEGY_SOURCE_SCHEMA
    assert lineage.source_database == "mysourcedb"


def test_walks_every_configuration_section_not_just_the_first():
    # An early implementation descended only into configuration[0] and silently
    # found no destination at all.
    shuffled = {"configuration": list(reversed(CONFIG_JSON["configuration"]))}
    lineage = parse_connector_config(shuffled)
    assert lineage.destination_database == "MY_DB"
    assert lineage.source_tables == [("public", "mytable")]


def test_source_schema_strategy_maps_schema_through():
    identifier = destination_identifier(
        destination_database="MY_DB",
        source_schema="public",
        source_table="mytable",
        schema_strategy=SCHEMA_STRATEGY_SOURCE_SCHEMA,
    )
    assert identifier == "MY_DB.public.mytable"


def test_unrecognised_schema_strategy_returns_none_rather_than_guessing():
    # Prefix/Suffix/Pattern strategies exist. A guess here produces a
    # well-formed URN pointing at a table that does not exist.
    assert (
        destination_identifier(
            destination_database="MY_DB",
            source_schema="public",
            source_table="mytable",
            schema_strategy="SOME_FUTURE_STRATEGY",
        )
        is None
    )


def test_pattern_configured_connector_yields_no_enumerable_tables():
    config = {
        "configuration": [
            {
                "name": "Replication table schema",
                "properties": {"Included Source Table Pattern": _wrap("public\\..*")},
            },
            {
                "name": "Destination details",
                "properties": {
                    "Snowflake Destination Database": _wrap("MY_DB"),
                    "Destination Schema Strategy": _wrap("SOURCE_SCHEMA"),
                },
            },
        ]
    }
    lineage = parse_connector_config(config)
    assert lineage.source_tables == []
    assert lineage.table_pattern == "public\\..*"


def test_source_url_uses_the_observed_property_name():
    # Probe Result 37 recorded the real name as "Source Database Connection URL".
    # An earlier draft looked for "JDBC URL", which no observed connector uses, so
    # source_database stayed None and no upstream was ever emitted -- silently.
    config = {
        "configuration": [
            {
                "name": "Source",
                "properties": {
                    "Source Database Connection URL": _wrap(
                        "jdbc:postgresql://host:5432/mysourcedb"
                    ),
                    "Source Database User": _wrap("repl"),
                },
            }
        ]
    }
    assert parse_connector_config(config).source_database == "mysourcedb"


def test_unrecognised_source_url_key_is_reported_not_silently_ignored():
    config = {
        "configuration": [
            {
                "name": "Source",
                "properties": {"Some Future Url Property": _wrap("jdbc:x://h/db")},
            }
        ]
    }
    lineage = parse_connector_config(config)
    assert lineage.source_database is None
    assert lineage.unrecognised_source_url_keys == ["Some Future Url Property"]


def test_unqualified_table_name_is_reported_not_silently_dropped():
    # An entry with no schema qualifier must be surfaced. Dropping it silently
    # makes a connector with 9 of 10 tables look identical to one with all 10.
    config = {
        "configuration": [
            {
                "name": "Replication table schema",
                "properties": {
                    "Included Comma Separated Source Table Names": _wrap(
                        '"public"."a",noschema'
                    )
                },
            }
        ]
    }
    lineage = parse_connector_config(config)
    assert lineage.source_tables == [("public", "a")]
    assert lineage.unparseable_tables == ["noschema"]


@pytest.mark.parametrize(
    "raw,expected",
    [
        ('"public"."mytable"', [("public", "mytable")]),
        ('"public"."a","public"."b"', [("public", "a"), ("public", "b")]),
        ("public.mytable", [("public", "mytable")]),
        ("", []),
    ],
)
def test_table_name_list_parsing(raw, expected):
    config = {
        "configuration": [
            {
                "name": "Replication table schema",
                "properties": {
                    "Included Comma Separated Source Table Names": _wrap(raw)
                },
            }
        ]
    }
    assert parse_connector_config(config).source_tables == expected


# --- property_value: the wrapper accessor. A live TypeError (unhashable ------
# --- type: 'slice') is what surfaced that every property is wrapped as -------
# --- {"valueType": ..., "value": ...} rather than a bare string. -------------


def test_property_value_reads_the_wrapped_string():
    properties = {"Snowflake Destination Database": _wrap("MY_DB")}
    assert property_value(properties, "Snowflake Destination Database") == "MY_DB"


def test_property_value_unset_property_reads_as_none():
    # Openflow marks an unset property by omitting "value" entirely -- not
    # null, not "" -- exactly how a real connector's unused Included Source
    # Table Pattern and Destination Schema Suffix both look.
    properties = {"Included Source Table Pattern": _wrap(None)}
    assert property_value(properties, "Included Source Table Pattern") is None


def test_property_value_ignores_non_literal_value_types():
    # ASSET_REFERENCE and SECRET_REFERENCE properties carry assetIds /
    # fullyQualifiedSecretName instead of "value". Reaching into one for a
    # string would silently pick up whatever happened to be under a "value"
    # key, or crash when there isn't one.
    properties = {
        "Source Database Driver": {
            "valueType": "ASSET_REFERENCE",
            "assetIds": ["postgresql-42.7.13-2.jar"],
        }
    }
    assert property_value(properties, "Source Database Driver") is None


def test_property_value_missing_key_reads_as_none():
    assert property_value({}, "Anything") is None


def test_property_value_tolerates_a_bare_string_for_forward_compatibility():
    # Defensive: every observed property is wrapped, but a future config
    # format version could flatten one to a bare string.
    properties = {"Snowflake Destination Database": "MY_DB"}
    assert property_value(properties, "Snowflake Destination Database") == "MY_DB"


def test_parses_the_real_observed_wrapped_config_shape():
    # The real config.json shape (Round 13 probe, corrected): every property is
    # {"valueType": ..., "value": ...}, with "value" entirely absent when unset.
    # CONFIG_JSON above, using bare strings, is a placeholder fixture that never
    # actually occurs -- this is the real file, in its real shape, asserting
    # the real answer.
    config = {
        "configuration": [
            {
                "name": "Source",
                "properties": {
                    "Source Database Connection URL": _wrap(
                        "jdbc:postgresql://host:5432/postgres?sslmode=require"
                    ),
                    "Source Database Publication Name": _wrap("openflow_pub"),
                    "Source Database Driver": {
                        "valueType": "ASSET_REFERENCE",
                        "assetIds": ["postgresql-42.7.13-2.jar"],
                    },
                },
            },
            {
                "name": "Replication table schema",
                "properties": {
                    "Included Comma Separated Source Table Names": _wrap(
                        '"public"."mytable"'
                    ),
                    "Included Source Table Pattern": _wrap(None),
                },
            },
            {
                "name": "Destination details",
                "properties": {
                    "Snowflake Destination Database": _wrap("MY_DB"),
                    "Destination Schema Strategy": _wrap("SOURCE_SCHEMA"),
                    "Destination Schema Suffix": _wrap(None),
                    "Table Storage Format": _wrap("STANDARD"),
                },
            },
        ]
    }

    lineage = parse_connector_config(config)

    assert lineage.source_database == "postgres"
    assert lineage.source_tables == [("public", "mytable")]
    assert lineage.destination_database == "MY_DB"
    assert lineage.schema_strategy == "SOURCE_SCHEMA"
    assert lineage.table_pattern is None


# --- _lineage_for_connector / _read_connector_config: the orchestration ----
# --- layer that turns parsed config into report calls and URNs. Driven ----
# --- through the same _query_rows seam test_source.py uses -- no network. --
# --- _read_connector_config issues GET, whose real side effect is writing a
# --- file into the local directory named in the query's 'file://<dir>'
# --- argument -- so the fake below reproduces exactly that side effect
# --- rather than returning file content from _query_rows itself.

MINIMAL_CONNECTION = {
    "connection": {
        "account_id": "abc12345",
        "username": "user",
        "password": "pass",
    }
}


def _make_source(**config_overrides: Any) -> SnowflakeOpenflowSource:
    config = SnowflakeOpenflowSourceConfig.model_validate(
        {**MINIMAL_CONNECTION, **config_overrides}
    )
    source = object.__new__(SnowflakeOpenflowSource)
    source.config = config
    source.platform = "openflow"
    source.report = SnowflakeOpenflowReport()
    return source


def _connector(
    version_location_uri: Optional[str] = "@stage/v1/",
) -> OpenflowConnector:
    return OpenflowConnector(
        name="pg_cdc",
        runtime_name="MyRuntime",
        connector_id="1",
        connector_definition="OPENFLOW_POSTGRES_CDC",
        version_location_uri=version_location_uri,
    )


def _fake_get(content: Optional[bytes]) -> Callable[[str], List[Dict[str, Any]]]:
    # Real GET writes a file into the local_dir named in the query string
    # (`... 'file://<local_dir>'`) and returns audit-trail rows, not content.
    # content=None simulates GET reporting success while placing no file --
    # not observed against a live account, but the code must not crash on it.
    def fake(query: str) -> List[Dict[str, Any]]:
        local_dir = query.rsplit("'file://", 1)[1].rstrip("'")
        if content is not None:
            (pathlib.Path(local_dir) / CONFIG_FILENAME).write_bytes(content)
        return [{"file": CONFIG_FILENAME, "status": "DOWNLOADED"}]

    return fake


def _warning_titles(report: SnowflakeOpenflowReport) -> List[Optional[str]]:
    return [entry.title for entry in report.warnings]


def _flat(
    source: SnowflakeOpenflowSource, connector: OpenflowConnector
) -> Tuple[List[str], List[str]]:
    """(inlets, outlets) across every table, for tests that assert URN shape.

    Lineage is now per-table, so the pairing is the interesting part and is
    asserted directly in test_each_replicated_table_gets_its_own_job. These
    tests predate that and are about how a single URN is composed, which the
    flattened view states just as well.
    """
    pairs = source._lineage_for_connector(connector)
    return (
        [pair.inlet for pair in pairs if pair.inlet],
        [pair.outlet for pair in pairs],
    )


def test_lineage_for_connector_happy_path_returns_inlets_and_outlets():
    source = _make_source()
    connector = _connector()
    source._query_rows = _fake_get(json.dumps(CONFIG_JSON).encode())  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]
    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.public.mytable,PROD)"
    ]
    assert source.report.num_lineage_edges == 1
    assert source.report.num_lineage_edges_skipped == 0


def test_lineage_for_connector_keeps_upstream_case_while_folding_destination():
    # convert_urns_to_lowercase is one-sided by design: the destination Snowflake
    # identifier folds, the upstream one does not. postgres/mysql/mssql default
    # convert_urns_to_lowercase to False, so their sources write '"Public"."MyTable"'
    # verbatim and a folded inlet would name a dataset that does not exist.
    source = _make_source(convert_urns_to_lowercase=True)
    connector = _connector()
    config: Dict[str, Any] = copy.deepcopy(CONFIG_JSON)
    config["configuration"][1]["properties"][
        "Included Comma Separated Source Table Names"
    ] = _wrap('"Public"."MyTable"')
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.Public.MyTable,PROD)"
    ]
    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]


def test_lowercase_urns_processor_is_excluded():
    # The pipeline-level processor folds EVERY dataset URN in the stream, with no
    # per-platform or per-aspect exemption, which would fold the upstream inlets
    # above. The destination fold happens in SnowflakeIdentifierBuilder instead,
    # so excluding it loses nothing.
    source = _make_source(convert_urns_to_lowercase=True)

    assert AutoLowercaseUrnsProcessor in source.get_excluded_workunit_processors()


def test_lineage_for_connector_handles_gzip_compressed_config():
    # Whether a staged file arrives gzip-compressed depends on Snowflake's
    # AUTO_COMPRESS staging behaviour, not on anything this connector
    # controls. Detected via the gzip magic bytes, not a ".gz" filename
    # suffix, since GET's own suffix convention is not a guarantee.
    source = _make_source()
    connector = _connector()
    compressed = gzip.compress(json.dumps(CONFIG_JSON).encode())
    source._query_rows = _fake_get(compressed)  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]
    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.public.mytable,PROD)"
    ]
    assert source.report.num_config_reads_failed == 0


def test_lineage_for_connector_reports_config_read_failure():
    # A raising _query_rows -- e.g. missing READ on the version stage.
    source = _make_source()
    connector = _connector()

    def raise_error(query: str) -> List[Dict[str, Any]]:
        raise RuntimeError("boom")

    source._query_rows = raise_error  # type: ignore[method-assign]

    inlets, outlets = _flat(source, connector)

    assert inlets == []
    assert outlets == []
    assert source.report.num_config_reads_failed == 1
    assert "Could not read connector configuration" in _warning_titles(source.report)


def test_lineage_for_connector_reports_malformed_json_as_config_read_failure():
    # The exact shape hit in production: the earlier SELECT $1 FROM stage
    # approach parsed the file under Snowflake's default CSV file format, so
    # $1 was only the text up to the first comma -- 24 of 2921 real bytes,
    # ending mid-field as `{"configFormatVersion":1`. GET fixes the read
    # itself, but json.loads must still stay inside the same try/except as
    # the download: invalid content must degrade to a per-connector warning,
    # not crash the whole ingestion run the way an uncaught JSONDecodeError
    # inside get_workunits_internal's generator would.
    source = _make_source()
    connector = _connector()
    truncated = b'{"configFormatVersion":1'
    source._query_rows = _fake_get(truncated)  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert inlets == []
    assert outlets == []
    assert source.report.num_config_reads_failed == 1
    assert "Could not read connector configuration" in _warning_titles(source.report)


def test_lineage_for_connector_reports_missing_download_as_config_read_failure():
    # GET reporting success while placing no file in local_dir is not a shape
    # confirmed against a live account, but nothing rules it out either
    # (a permission edge case, a stage inconsistency). It must degrade to the
    # same warning as any other failed read, not raise out of the generator
    # that drives ingestion.
    source = _make_source()
    connector = _connector()
    source._query_rows = _fake_get(None)  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert inlets == []
    assert outlets == []
    assert source.report.num_config_reads_failed == 1
    assert "Could not read connector configuration" in _warning_titles(source.report)


def test_lineage_for_connector_warns_on_unparseable_source_table_name():
    # R13: partial lineage loss must not be silent. A connector that yields nine
    # of its ten tables is indistinguishable from one that genuinely has nine, so
    # removing this warning has to fail a test. Asserted by title -- a truthiness
    # check on report.warnings would also pass on an unrelated warning.
    source = _make_source()
    connector = _connector()
    config = {
        "configuration": [
            {
                "name": "Replication table schema",
                "properties": {
                    "Included Comma Separated Source Table Names": _wrap(
                        '"public"."mytable",noschema'
                    )
                },
            },
            {
                "name": "Destination details",
                "properties": {
                    "Snowflake Destination Database": _wrap("MY_DB"),
                    "Destination Schema Strategy": _wrap("SOURCE_SCHEMA"),
                },
            },
        ]
    }
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]

    _, outlets = _flat(source, connector)

    assert "Unparseable source table name" in _warning_titles(source.report)
    assert source.report.num_lineage_edges_skipped == 1
    # Only the unqualified entry is lost -- the qualified one still emits.
    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]


def test_lineage_for_connector_warns_on_unrecognised_source_url_property():
    # R20: an unknown Source-section URL key means no upstream dataset can be
    # derived, while the downstream half still emits. Without the warning the
    # run looks complete and half the lineage is quietly missing.
    source = _make_source()
    connector = _connector()
    config = {
        "configuration": [
            {
                "name": "Source",
                "properties": {
                    "Some Future Url Property": _wrap("jdbc:postgresql://host/mydb")
                },
            },
            {
                "name": "Replication table schema",
                "properties": {
                    "Included Comma Separated Source Table Names": _wrap(
                        '"public"."mytable"'
                    )
                },
            },
            {
                "name": "Destination details",
                "properties": {
                    "Snowflake Destination Database": _wrap("MY_DB"),
                    "Destination Schema Strategy": _wrap("SOURCE_SCHEMA"),
                },
            },
        ]
    }
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert "Source connection URL property not recognised" in _warning_titles(
        source.report
    )
    assert inlets == []
    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]


def test_lineage_for_connector_skips_unrecognised_schema_strategy():
    source = _make_source()
    connector = _connector()
    config = {
        "configuration": [
            {
                "name": "Replication table schema",
                "properties": {
                    "Included Comma Separated Source Table Names": _wrap(
                        '"public"."mytable"'
                    )
                },
            },
            {
                "name": "Destination details",
                "properties": {
                    "Snowflake Destination Database": _wrap("MY_DB"),
                    "Destination Schema Strategy": _wrap("PREFIX"),
                },
            },
        ]
    }
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert inlets == []
    assert outlets == []
    assert source.report.num_lineage_edges_skipped == 1
    assert "Unrecognised destination schema strategy" in _warning_titles(source.report)


def test_lineage_for_connector_counts_pattern_configured_connector():
    source = _make_source()
    connector = _connector()
    config = {
        "configuration": [
            {
                "name": "Replication table schema",
                "properties": {"Included Source Table Pattern": _wrap("public\\..*")},
            },
            {
                "name": "Destination details",
                "properties": {
                    "Snowflake Destination Database": _wrap("MY_DB"),
                    "Destination Schema Strategy": _wrap("SOURCE_SCHEMA"),
                },
            },
        ]
    }
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert inlets == []
    assert outlets == []
    assert source.report.num_connectors_without_enumerable_tables == 1


def test_include_table_lineage_false_skips_lineage_entirely():
    # The gate lives in get_workunits_internal, not in _lineage_for_connector
    # itself, so this drives the full method rather than the helper directly.
    # _lineage_for_connector is stubbed to raise: if the gate were removed or
    # inverted, this test fails on that AssertionError rather than passing
    # vacuously.
    source = _make_source(include_table_lineage=False)
    connector = _connector()

    def fail_if_called(connector: OpenflowConnector) -> Any:
        raise AssertionError("_lineage_for_connector should not be called")

    source._lineage_for_connector = fail_if_called  # type: ignore[method-assign]

    connector_show = [
        {
            "name": connector.name,
            "runtime": connector.runtime_name,
            "connector_id": connector.connector_id,
            "connector_definition": connector.connector_definition,
        }
    ]

    def fake_query_rows(query: str) -> List[Dict[str, Any]]:
        if query == SnowflakeOpenflowQuery.show_deployments():
            return []
        if query == SnowflakeOpenflowQuery.show_runtimes():
            return []
        if query == SnowflakeOpenflowQuery.show_connectors():
            return connector_show
        if (
            DEPLOYMENT_HISTORY in query
            or RUNTIME_HISTORY in query
            or CONNECTOR_HISTORY in query
        ):
            return []
        raise AssertionError(f"unexpected query: {query!r}")

    source._query_rows = fake_query_rows  # type: ignore[method-assign]

    workunits = list(source.get_workunits_internal())  # must not raise

    assert workunits  # the flow and job workunits are still emitted


def test_lineage_inlet_uses_configured_source_platform_instance():
    # A Postgres recipe that sets platform_instance/env produces upstream URNs
    # carrying those coordinates. Without threading them through here, the inlet
    # is well-formed but names a dataset that recipe never emitted.
    source = _make_source(source_platform_instance="pg_prod", source_env="DEV")
    connector = _connector()
    source._query_rows = _fake_get(json.dumps(CONFIG_JSON).encode())  # type: ignore[assignment]

    inlets, outlets = _flat(source, connector)

    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,pg_prod.mysourcedb.public.mytable,DEV)"
    ]
    # The upstream coordinates must not leak into the destination side, which
    # keeps following snowflake_platform_instance / snowflake_env.
    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]


def test_lineage_inlet_urn_unchanged_when_source_coordinates_unset():
    # Back-compatibility pin: with both new fields unset, the inlet URN must be
    # byte-identical to what shipped before they existed -- no platform_instance
    # segment, env from the source's own `env`. This is the assertion that
    # protects already-ingested lineage from being re-keyed.
    source = _make_source()
    connector = _connector()
    source._query_rows = _fake_get(json.dumps(CONFIG_JSON).encode())  # type: ignore[assignment]

    inlets, _ = _flat(source, connector)

    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.public.mytable,PROD)"
    ]


def test_lineage_inlet_env_follows_openflow_env_when_source_env_unset():
    source = _make_source(env="DEV")
    connector = _connector()
    source._query_rows = _fake_get(json.dumps(CONFIG_JSON).encode())  # type: ignore[assignment]

    inlets, _ = _flat(source, connector)

    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.public.mytable,DEV)"
    ]


# --- Upstream dataset naming, per platform tier -----------------------------
# A dataset URN whose name is at the wrong tier is still a well-formed URN, so
# nothing raises -- the edge just points at a dataset that cannot exist. These
# pin the shape DataHub's own sources use for each upstream platform.


def _cdc_config(
    source_url: Optional[str],
    included_tables: str,
    url_key: str = "Source Database Connection URL",
) -> Dict[str, Any]:
    source_properties: Dict[str, Any] = {}
    if source_url is not None:
        source_properties[url_key] = _wrap(source_url)
    return {
        "configuration": [
            {"name": "Source", "properties": source_properties},
            {
                "name": "Replication table schema",
                "properties": {
                    "Included Comma Separated Source Table Names": _wrap(
                        included_tables
                    )
                },
            },
            {
                "name": "Destination details",
                "properties": {
                    "Snowflake Destination Database": _wrap("MY_DB"),
                    "Destination Schema Strategy": _wrap("SOURCE_SCHEMA"),
                },
            },
        ]
    }


def _inlets_for(
    connector_definition: str,
    config_json: Dict[str, Any],
) -> Tuple[List[str], List[str], SnowflakeOpenflowReport]:
    source = _make_source()
    connector = OpenflowConnector(
        name="cdc",
        runtime_name="MyRuntime",
        connector_id="1",
        connector_definition=connector_definition,
        version_location_uri="@stage/v1/",
    )
    source._query_rows = _fake_get(json.dumps(config_json).encode())  # type: ignore[assignment]
    inlets, outlets = _flat(source, connector)
    return inlets, outlets, source.report


def test_postgres_upstream_is_database_schema_table():
    # PostgresSource.get_identifier (sql/postgres/source.py) composes
    # f"{database}.{schema}.{entity}". This is also the shape covered by the
    # committed golden file and by the live M3 milestone, so it must not move.
    inlets, outlets, _ = _inlets_for(
        "OPENFLOW_POSTGRES_CDC",
        _cdc_config("jdbc:postgresql://host:5432/mysourcedb", '"public"."mytable"'),
    )

    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.public.mytable,PROD)"
    ]
    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]


def test_mssql_upstream_is_database_schema_table():
    # SQLServerSource.get_identifier (sql/mssql/source.py) composes
    # f"{current_database}.{schema}.{entity}".
    inlets, _, _ = _inlets_for(
        "OPENFLOW_SQLSERVER_CDC",
        _cdc_config("jdbc:sqlserver://host:1433/mysourcedb", '"dbo"."mytable"'),
    )

    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,mysourcedb.dbo.mytable,PROD)"
    ]


def test_mysql_upstream_is_two_tier_database_table():
    # MySQL is modelled by TwoTierSQLAlchemySource: get_allowed_schemas yields
    # db_name as the "schema" and MySQLConfig.get_identifier returns
    # f"{schema}.{table}", so the URN name has two parts. The three-tier formula
    # this replaced emitted "mysourcedb.mysourcedb.mytable", which joins to
    # nothing.
    inlets, outlets, _ = _inlets_for(
        "OPENFLOW_MYSQL_CDC",
        _cdc_config("jdbc:mysql://host:3306/mysourcedb", '"mysourcedb"."mytable"'),
    )

    assert inlets == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,mysourcedb.mytable,PROD)"
    ]
    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.mysourcedb.mytable,PROD)"
    ]


def test_mysql_upstream_uses_the_table_qualifier_not_the_jdbc_database():
    # The per-table qualifier is the MySQL database, so lineage stays correct
    # for a CDC connector replicating tables from more than one database -- and
    # is still derivable when the JDBC URL names no database at all.
    inlets, _, _ = _inlets_for(
        "OPENFLOW_MYSQL_CDC",
        _cdc_config("jdbc:mysql://host:3306/", '"otherdb"."mytable"'),
    )

    assert inlets == ["urn:li:dataset:(urn:li:dataPlatform:mysql,otherdb.mytable,PROD)"]


def test_kafka_connector_definition_builds_no_upstream_and_warns():
    # Kafka dataset names are the bare topic (source/kafka/kafka.py), which this
    # connector cannot derive: the upstream side is reconstructed from a `jdbc:`
    # Source URL and a schema-qualified table list, neither of which a Kafka
    # connector carries. NOT verified against a live Kafka connector -- none
    # exists in the test account -- so the contract asserted here is that the
    # definition is reported as unsupported rather than guessed at.
    inlets, outlets, report = _inlets_for(
        "OPENFLOW_KAFKA",
        _cdc_config("jdbc:postgresql://host:5432/mysourcedb", '"public"."mytable"'),
    )

    assert inlets == []
    # Destination lineage still flows.
    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]
    assert "Unsupported connector definition for upstream lineage" in _warning_titles(
        report
    )


def test_unknown_connector_definition_degrades_to_destination_only():
    inlets, outlets, report = _inlets_for(
        "OPENFLOW_SOME_FUTURE_SOURCE",
        _cdc_config("jdbc:postgresql://host:5432/mysourcedb", '"public"."mytable"'),
    )

    assert inlets == []
    assert outlets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
    ]
    assert "Unsupported connector definition for upstream lineage" in _warning_titles(
        report
    )


def test_three_tier_upstream_omits_the_inlet_when_no_database_was_parsed():
    # A missing database would otherwise produce "None.public.mytable".
    assert (
        upstream_identifier(
            CONNECTOR_DEFINITION_PLATFORM["OPENFLOW_POSTGRES_CDC"],
            source_database=None,
            source_schema="public",
            source_table="mytable",
        )
        is None
    )


# --- Stage GET retry --------------------------------------------------------
# The per-connector GET runs through SnowflakeConnection.query(), whose only
# retry path gates on "ACCOUNT_USAGE" appearing in the query text. A GET never
# matches, so without a retry of its own a single transient blip drops that
# connector's lineage for the whole run.


class _FlakyGet:
    # Raises `error` for the first `failures` calls, then downloads normally.
    def __init__(self, failures: int, error: BaseException) -> None:
        self.failures = failures
        self.error = error
        self.calls = 0
        self._download = _fake_get(json.dumps(CONFIG_JSON).encode())

    def __call__(self, query: str) -> List[Dict[str, Any]]:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return self._download(query)


EXPECTED_OUTLETS = [
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
]


@pytest.fixture
def no_retry_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # The real backoff waits ~1s then ~2s, which is right for a network blip and
    # wrong for a unit test. Only the wait is neutralised; which exceptions
    # retry, and how many attempts there are, stay exactly as shipped.
    monkeypatch.setattr(snowflake_openflow, "_RETRY_BACKOFF_MULTIPLIER", 0)


def test_stage_get_retries_a_transient_connection_error(
    no_retry_backoff: None,
) -> None:
    source = _make_source()
    flaky = _FlakyGet(
        failures=2, error=OperationalError(msg="connection reset by peer")
    )
    source._query_rows = flaky  # type: ignore[assignment]

    _, outlets = _flat(source, _connector())

    assert outlets == EXPECTED_OUTLETS
    assert flaky.calls == 3
    assert source.report.num_config_reads_failed == 0
    assert "Could not read connector configuration" not in _warning_titles(
        source.report
    )


def test_stage_get_gives_up_after_a_bounded_number_of_attempts(
    no_retry_backoff: None,
) -> None:
    # Bounded: a stage that is genuinely unreachable must not retry forever, and
    # the failure must be counted exactly once -- not once per attempt.
    source = _make_source()
    flaky = _FlakyGet(failures=99, error=OperationalError(msg="connection reset"))
    source._query_rows = flaky  # type: ignore[assignment]

    inlets, outlets = _flat(source, _connector())

    assert (inlets, outlets) == ([], [])
    assert flaky.calls == 3
    assert source.report.num_config_reads_failed == 1
    assert "Could not read connector configuration" in _warning_titles(source.report)


def test_stage_get_does_not_retry_a_deterministic_error() -> None:
    # A missing READ grant, or no config.json on the stage, fails identically on
    # every attempt. Retrying it only triples the time to the same warning, and
    # a retry gate wide enough to catch it would also mask real problems.
    source = _make_source()
    flaky = _FlakyGet(
        failures=99,
        error=ProgrammingError(msg="File not found or not authorized", errno=2003),
    )
    source._query_rows = flaky  # type: ignore[assignment]

    inlets, outlets = _flat(source, _connector())

    assert (inlets, outlets) == ([], [])
    assert flaky.calls == 1
    assert source.report.num_config_reads_failed == 1


def _inlets_with_config_overrides(**overrides):
    # _inlets_for builds its source with no overrides, so the folding knob needs a
    # source built directly.
    source = _make_source(**overrides)
    connector = OpenflowConnector(
        name="cdc",
        runtime_name="MyRuntime",
        connector_id="1",
        connector_definition="OPENFLOW_POSTGRES_CDC",
        version_location_uri="@stage/v1/",
    )
    config_json = _cdc_config(
        "jdbc:postgresql://host:5432/mysourcedb", '"Public"."MyTable"'
    )
    source._query_rows = _fake_get(json.dumps(config_json).encode())  # type: ignore[assignment]
    inlets, _ = _flat(source, connector)
    return inlets


def test_upstream_urn_folds_when_the_upstream_recipe_folds():
    # DataHub's own MSSQL source warns operators to set convert_urns_to_lowercase
    # for lineage (sql/mssql/source.py), so an upstream that folds is a realistic
    # configuration rather than a hypothetical one. Without this knob the inlet
    # stays verbatim and cannot match that recipe.
    assert _inlets_with_config_overrides(source_convert_urns_to_lowercase=True) == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.public.mytable,PROD)"
    ]


def test_upstream_urn_stays_verbatim_by_default():
    # The default must not move: postgres/mysql/mssql all preserve case unless told
    # otherwise, and both the golden file and the live M3 milestone depend on the
    # verbatim form.
    assert _inlets_with_config_overrides() == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.Public.MyTable,PROD)"
    ]


def test_connector_without_a_config_uri_is_counted_and_warned():
    # SHOW does not carry version_location_uri, so a connector reaches this path
    # when the history side could not supply it. The SHOW-authority rule created a
    # new way in: a live connector whose only history row is closed contributes
    # nothing, so the URI is absent. Lineage is genuinely underivable here -- the
    # defect would be losing it with nothing for an operator to see.
    source = _make_source()
    connector = OpenflowConnector(
        name="cdc",
        runtime_name="MyRuntime",
        connector_id="1",
        connector_definition="OPENFLOW_POSTGRES_CDC",
        version_location_uri=None,
    )

    inlets, outlets = _flat(source, connector)

    assert inlets == [] and outlets == []
    assert source.report.num_connectors_without_config_uri == 1
    assert "Connector has no config location" in [
        entry.title for entry in source.report.warnings
    ]


def test_upstream_instance_prefix_folds_with_the_identifier():
    # The flag is only correct to set when the upstream recipe spells
    # convert_urns_to_lowercase out, and that recipe also gets the pipeline-level
    # pass, which folds the WHOLE urn name -- and the composed name is
    # `<instance>.<identifier>`. So folding the identifier while leaving the
    # instance verbatim matches no upstream shape at all.
    #
    # Nothing caught this before for a subtler reason than "the field was never
    # set": test_lineage.py:691 and test_config.py:59 both set it. They set it to
    # `pg_prod`, which is ALREADY lowercase, so folding it is a no-op and the fold
    # site was unobservable. A value that cannot distinguish the two behaviours
    # gives the coverage report a hit and gives the fold no test at all.
    assert _inlets_with_config_overrides(
        source_convert_urns_to_lowercase=True,
        source_platform_instance="PG_Prod",
    ) == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,pg_prod.mysourcedb.public.mytable,PROD)"
    ]


def test_upstream_instance_prefix_kept_verbatim_when_not_folding():
    # Default path: an upstream that preserves case (the postgres/mysql/mssql
    # default) keeps both halves exactly as written.
    assert _inlets_with_config_overrides(
        source_platform_instance="PG_Prod",
    ) == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,PG_Prod.mysourcedb.Public.MyTable,PROD)"
    ]


@pytest.mark.parametrize(
    ("source_url", "expected"),
    [
        # SQL Server does not put the database in the path. Parsing only the
        # path dropped every OPENFLOW_SQLSERVER_CDC upstream silently.
        ("jdbc:sqlserver://h:1433;databaseName=mydb", "mydb"),
        ("jdbc:sqlserver://h:1433;encrypt=true;databaseName=mydb", "mydb"),
        ("jdbc:postgresql://h:5432/mydb", "mydb"),
        ("jdbc:mysql://h:3306/mydb", "mydb"),
        # No database named at all -- better None than a guess, since a
        # wrong-tier URN is well-formed and points at nothing.
        ("jdbc:sqlserver://h:1433", None),
        # Not a JDBC URL: stripping the prefix that is not there would eat five
        # characters and yield a plausible but wrong database.
        ("notjdbc://h/mydb", None),
    ],
)
def test_jdbc_database_handles_both_url_shapes(
    source_url: str, expected: Optional[str]
) -> None:
    assert snowflake_openflow._jdbc_database(source_url) == expected


def test_missing_destination_database_is_counted_and_warned() -> None:
    # This branch returned empty-handed in silence while every sibling branch
    # counted or warned. The failure it hides is not one connector without
    # lineage -- it is a Snowflake-side property rename taking out every
    # connector at once behind a clean report.
    config: Dict[str, Any] = copy.deepcopy(CONFIG_JSON)
    for section in config["configuration"]:
        if section["name"] == snowflake_openflow.SECTION_DESTINATION:
            properties: Dict[str, Any] = section["properties"]
            properties.pop(snowflake_openflow.PROP_DESTINATION_DATABASE, None)

    source = _make_source()
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]

    inlets, outlets = _flat(source, _connector())

    assert (inlets, outlets) == ([], [])
    assert source.report.num_connectors_without_destination_database == 1
    assert _warning_titles(source.report) == ["Connector has no destination database"]


def test_unparseable_source_url_reports_the_missing_inlet() -> None:
    # The old unconditional `jdbc:` strip produced a wrong-but-present database;
    # _jdbc_database correctly returns None instead, which silently dropped the
    # upstream half of every edge until this counter and warning existed. The
    # URL shape reaches the report; the URL itself must not, since a JDBC URL
    # can carry a host and credentials.
    config: Dict[str, Any] = copy.deepcopy(CONFIG_JSON)
    for section in config["configuration"]:
        if section["name"] == snowflake_openflow.SECTION_SOURCE:
            properties: Dict[str, Any] = section["properties"]
            for key in list(properties):
                if "URL" in key.upper():
                    properties[key] = _wrap("mysql://secretuser:pw@host:3306/db")

    source = _make_source()
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]

    inlets, outlets = _flat(source, _connector())

    assert inlets == []
    assert outlets, "the destination half must survive a missing upstream"
    assert source.report.num_upstream_inlets_skipped == 1
    assert _warning_titles(source.report) == [
        "Upstream dataset could not be identified"
    ]
    context = str(source.report.warnings[0].context)
    assert "mysql://..." in context
    assert "secretuser" not in context and "pw" not in context


def test_no_tables_and_no_pattern_is_warned_not_silent() -> None:
    # Distinct from the pattern case, which is a documented steady state. This
    # shape is what a renamed source property looks like, and it used to fall
    # through the loop and return empty with nothing recorded.
    config: Dict[str, Any] = copy.deepcopy(CONFIG_JSON)
    config["configuration"] = [
        section
        for section in config["configuration"]
        if section["name"] != snowflake_openflow.SECTION_REPLICATION
    ]

    source = _make_source()
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]

    assert _flat(source, _connector()) == ([], [])
    assert source.report.num_connectors_without_table_configuration == 1
    assert _warning_titles(source.report) == ["Connector names no tables to replicate"]


def test_each_replicated_table_gets_its_own_job() -> None:
    # The reason this connector emits per-table jobs at all. Flattening three
    # tables onto one job's inlets/outlets asserts 3x3 = 9 edges, of which 6 are
    # fabricated; DataHub renders them identically to the 3 real ones.
    config: Dict[str, Any] = copy.deepcopy(CONFIG_JSON)
    for section in config["configuration"]:
        if section["name"] == snowflake_openflow.SECTION_REPLICATION:
            properties: Dict[str, Any] = section["properties"]
            properties[snowflake_openflow.PROP_INCLUDED_TABLE_NAMES] = _wrap(
                '"public"."a","public"."b","public"."c"'
            )

    source = _make_source()
    source._query_rows = _fake_get(json.dumps(config).encode())  # type: ignore[assignment]
    pairs = source._lineage_for_connector(_connector())

    assert [pair.source_table for pair in pairs] == ["a", "b", "c"]
    for pair in pairs:
        # Each pair keeps its own 1:1 edge: the inlet and the outlet name the
        # SAME table, which is the invariant a flattened list cannot express.
        assert pair.inlet is not None
        assert f".{pair.source_table}," in pair.inlet
        assert f".{pair.source_table}," in pair.outlet


@pytest.mark.parametrize(
    ("source_url", "expected"),
    [
        pytest.param("jdbc:postgresql://h/db", "jdbc:postgresql://...", id="jdbc"),
        pytest.param("mysql://user:pw@h/db", "mysql://...", id="credentials in a url"),
        pytest.param(None, "<absent>", id="absent"),
        pytest.param("", "<absent>", id="empty"),
    ],
)
def test_url_shape_reduces_a_url_to_its_subprotocol(
    source_url: Optional[str], expected: str
) -> None:
    assert _url_shape(source_url) == expected


@pytest.mark.parametrize(
    "source_url",
    [
        pytest.param("user=admin;password=hunter2;host=h", id="property string"),
        pytest.param("oracle:thin:scott/tiger@//h:1521/db", id="oracle thin"),
        pytest.param("admin:hunter2@dbhost:5432", id="dsn style"),
    ],
)
def test_url_shape_echoes_nothing_from_a_url_it_cannot_parse(source_url: str) -> None:
    # This branch handles exactly the input whose layout cannot be reasoned
    # about, so it must reveal none of it. An earlier revision returned
    # source_url[:16], which rendered a connection string as
    # `user=admin;passw...` into a report that is persisted and
    # operator-visible -- from a function whose docstring promises "never the
    # whole URL". The fix shipped without a test; this is that test.
    shape = _url_shape(source_url)

    assert shape == f"<unrecognised, {len(source_url)} chars>"
    for secret in ("hunter2", "admin", "tiger", "scott"):
        assert secret not in shape


def test_a_connector_url_without_a_host_yields_no_canvas_link() -> None:
    # DESCRIBE returns CONNECTOR_URL as free text; a value that reaches /nifi/
    # but parses to no scheme or host cannot be turned into a link, and the
    # caller counts that rather than emitting a broken externalUrl.
    assert _canvas_url("/nifi/#/process-groups/abc") is None


def test_a_trailing_separator_in_the_table_list_is_not_an_unparseable_table() -> None:
    # `a.b,` is one table and a stray comma, not one table and one bad entry.
    # Counting the empty entry would inflate num_lineage_edges_skipped and send
    # an operator looking for a malformed name that does not exist.
    tables, unparseable = _parse_table_names("public.a, ,public.b,")

    assert tables == [("public", "a"), ("public", "b")]
    assert unparseable == []


def test_table_names_that_fail_to_parse_are_not_reported_as_absent() -> None:
    # Two different faults with two different remedies. Names present but
    # unqualified is a schema-qualifier problem, already counted and warned as
    # "Unparseable source table name". Telling the operator the connector
    # "listed neither table names nor a table pattern" on top of that is false
    # and points at the wrong property.
    lineage = parse_connector_config(
        _cdc_config("jdbc:postgresql://h/db", "nodots, alsonodots")
    )

    assert lineage.source_tables == []
    assert lineage.unparseable_tables == ["nodots", "alsonodots"]


def _long_pair(
    schema: str, table: str, inlet: Optional[str] = None
) -> ConnectorTableLineage:
    name = f"destdb.{schema}.{table}"
    return ConnectorTableLineage(
        source_schema=schema,
        source_table=table,
        outlet=f"urn:li:dataset:(urn:li:dataPlatform:snowflake,{name},PROD)",
        inlet=inlet,
    )


def test_an_over_long_destination_urn_skips_the_edge_rather_than_emitting_it() -> None:
    # These urns are FOREIGN -- the source points at what the warehouse
    # ingestion emitted -- so they cannot be shortened the way a connector name
    # can: a shortened urn joins to nothing. Three 255-character Snowflake
    # identifiers already make an 839-byte urn. Skipping and saying so beats
    # emitting an aspect GMS discards.
    source = _make_source()
    connector = OpenflowConnector(name="c", runtime_name="rt")

    assert (
        source._edge_within_urn_limits(_long_pair("s" * 255, "t" * 255), connector)
        is None
    )
    assert source.report.num_urns_too_long == 1
    assert "Lineage edge skipped: destination urn too long" in [
        e.title for e in source.report.warnings
    ]


def test_an_over_long_upstream_urn_costs_only_the_upstream_half() -> None:
    source = _make_source()
    connector = OpenflowConnector(name="c", runtime_name="rt")
    huge_inlet = "urn:li:dataset:(urn:li:dataPlatform:postgres," + "u" * 600 + ",PROD)"

    kept = source._edge_within_urn_limits(
        _long_pair("public", "t", inlet=huge_inlet), connector
    )

    assert kept is not None
    assert kept.inlet is None, "the downstream half must survive"
    assert source.report.num_upstream_inlets_skipped == 1
    assert "Upstream dropped: source urn too long" in [
        e.title for e in source.report.warnings
    ]
