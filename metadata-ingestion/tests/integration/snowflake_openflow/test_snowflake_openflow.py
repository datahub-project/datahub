"""Golden-file integration test for the ``snowflake-openflow`` source.

Every unit test in ``tests/unit/snowflake_openflow/`` builds its source with
``object.__new__``, bypassing ``__init__`` and ``PipelineContext``. That covers
the extraction logic thoroughly but leaves the assembled run untested: the
workunit-processor chain (browse paths, stale-entity removal), the SDK v2
entity emission and the sink never execute. This module closes that hole by
driving a real ``Pipeline``.

No live account is needed. Every statement the source issues -- the three
``SHOW OPENFLOW ...`` commands, the three ACCOUNT_USAGE history reads and the
per-connector stage ``GET`` -- goes through ``SnowflakeConnection.query()``, so
one dispatch table over the cursor covers all of them.
"""

import copy
import json
import pathlib
from functools import partial
from typing import Any, Dict, List, Optional, cast
from unittest import mock

from datahub.configuration.common import DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_openflow import CONFIG_FILENAME
from datahub.ingestion.source.snowflake.snowflake_openflow_query import (
    CONNECTOR_HISTORY,
    DEPLOYMENT_HISTORY,
    RUNTIME_HISTORY,
    SnowflakeOpenflowQuery,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_report import (
    SnowflakeOpenflowReport,
)
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import RowCountList

# --- Fixture inventory ------------------------------------------------------
#
# All identifiers are generic placeholders. CONNECTOR_DEFINITION is the one
# exception: OPENFLOW_POSTGRES_CDC is a Snowflake product value, and the source
# keys its upstream-platform lookup on it.

DEPLOYMENT_KEY = "mydeployment-1"
DEPLOYMENT_NAME = "MyDeployment"
RUNTIME_KEY = "myruntime-1"
RUNTIME_NAME = "MyRuntime"
CONNECTOR_NAME = "pg_cdc"
CONNECTOR_ID = "abc12345-0000-0000-0000-000000000001"
OWNER_ROLE = "MY_OPENFLOW_ROLE"
STAGE_URI = "@MY_DB.MY_SCHEMA.MY_STAGE/versions/1.0.0/"

# The four entities this fixture produces. The container guids are the
# ContainerKey hashes of the keys above -- stated literally rather than
# recomputed from OpenflowDeploymentKey/OpenflowRuntimeKey, so that a change to
# how they are derived fails here instead of silently re-keying every container.
DEPLOYMENT_URN = "urn:li:container:1a0e6286b2b666d963c21e6360d532a0"
RUNTIME_URN = "urn:li:container:d638d9f8db756f7c21271bf68113e7e8"
FLOW_URN = f"urn:li:dataFlow:(openflow,{RUNTIME_NAME}/{CONNECTOR_NAME},PROD)"
JOB_URN = f"urn:li:dataJob:({FLOW_URN},{RUNTIME_NAME}/{CONNECTOR_NAME})"
# One job per replicated table, beside the connector-level anchor above.
TABLE_JOB_URNS = {
    f"urn:li:dataJob:({FLOW_URN},{RUNTIME_NAME}/{CONNECTOR_NAME}/public.{table})"
    for table in ("mytable", "othertable")
}

# SHOW returns lowercase column names; the ACCOUNT_USAGE views return uppercase.
# The fixtures below preserve that difference rather than normalising it, since
# reading each surface by its own casing is exactly what OpenflowX.from_row does.

DEPLOYMENT_SHOW_ROWS = [
    {
        "key": DEPLOYMENT_KEY,
        "name": DEPLOYMENT_NAME,
        "status": "ACTIVE",
        "owner": OWNER_ROLE,
    }
]

RUNTIME_SHOW_ROWS = [
    {
        "key": RUNTIME_KEY,
        "name": RUNTIME_NAME,
        "deployment": DEPLOYMENT_NAME,
        "status": "ACTIVE",
        "owner": OWNER_ROLE,
        # The runtime object's own home, not a data destination.
        "database_name": "MY_DB",
        "schema_name": "MY_SCHEMA",
    }
]

# The NiFi canvas deep link DESCRIBE returns. Only DESCRIBE carries it -- SHOW
# returns 16 columns, DESCRIBE 20 (measured against a live account).
# What DESCRIBE actually reports -- including the fragment that does not
# resolve, so the fixture exercises the derivation rather than assuming it.
CONNECTOR_URL = (
    "https://openflow.example.snowflakecomputing.app:443/"
    f"{RUNTIME_KEY}/nifi/#/connectors/00000000-0000-0000-0000-000000000001/"
)
# What the connector emits: the runtime canvas, which is the part that resolves.
CANVAS_URL = f"https://openflow.example.snowflakecomputing.app/{RUNTIME_KEY}/nifi/"

CONNECTOR_SHOW_ROWS = [
    {
        "name": CONNECTOR_NAME,
        "runtime": RUNTIME_NAME,
        "connector_definition": "OPENFLOW_POSTGRES_CDC",
        "default_version": "1.0.0",
        "default_version_location_uri": STAGE_URI,
        "status": "RUNNING",
        "owner": OWNER_ROLE,
        # DESCRIBE addresses the connector by three-part name, and only SHOW
        # supplies these two.
        "database_name": "MY_DB",
        "schema_name": "MY_SCHEMA",
    }
]

CONNECTOR_DESCRIBE_ROWS = [{"CONNECTOR_URL": CONNECTOR_URL}]

DEPLOYMENT_HISTORY_ROWS = [
    {
        "DEPLOYMENT_KEY": DEPLOYMENT_KEY,
        "NAME": DEPLOYMENT_NAME,
        "CREATED_ON": "2024-01-01 00:00:00.000",
        "DELETED_ON": None,
    }
]

RUNTIME_HISTORY_ROWS = [
    {
        "RUNTIME_KEY": RUNTIME_KEY,
        "NAME": RUNTIME_NAME,
        "DEPLOYMENT_NAME": DEPLOYMENT_NAME,
        # Carried only by the view. SHOW has no such column, so this row is what
        # puts execute_as_role into the runtime container's properties -- i.e. the
        # golden file proves merge_show_and_history actually ran.
        "EXECUTE_AS_ROLE_NAME": OWNER_ROLE,
        "CREATED_ON": "2024-01-01 00:00:00.000",
        "DELETED_ON": None,
    }
]

CONNECTOR_HISTORY_ROWS = [
    {
        "NAME": CONNECTOR_NAME,
        "RUNTIME_NAME": RUNTIME_NAME,
        # SHOW OPENFLOW CONNECTORS has no id column at all, so connector_id in the
        # golden file's custom properties can only have come from this row.
        "CONNECTOR_ID": CONNECTOR_ID,
        "CREATED_ON": "2024-01-01 00:00:00.000",
        "DELETED_ON": None,
    },
    {
        # View-only and deleted: absent from SHOW, DELETED_ON set. Must not reach
        # the golden file at all.
        "NAME": "retired_cdc",
        "RUNTIME_NAME": RUNTIME_NAME,
        "CONNECTOR_ID": "abc12345-0000-0000-0000-000000000002",
        "CREATED_ON": "2024-01-01 00:00:00.000",
        "DELETED_ON": "2024-02-01 00:00:00.000",
    },
]


def _wrap(value: str) -> Dict[str, Any]:
    # Openflow wraps every config property as {"valueType": ..., "value": ...}.
    return {"valueType": "STRING_LITERAL", "value": value}


CONNECTOR_CONFIG_JSON: Dict[str, Any] = {
    "configFormatVersion": 1,
    "configuration": [
        {
            "name": "Source",
            "properties": {
                "Source Database Connection URL": _wrap(
                    "jdbc:postgresql://myhost:5432/mysourcedb"
                ),
            },
        },
        {
            "name": "Replication table schema",
            "properties": {
                # TWO tables deliberately: with one, a flattened
                # inlets/outlets DataJob and per-table DataJobs are
                # indistinguishable, so the fan-out defect this fixture guards
                # against could not appear in the golden at all.
                "Included Comma Separated Source Table Names": _wrap(
                    '"public"."mytable","public"."othertable"'
                ),
            },
        },
        {
            "name": "Destination details",
            "properties": {
                "Snowflake Destination Database": _wrap("MY_DB"),
                "Destination Schema Strategy": _wrap("SOURCE_SCHEMA"),
            },
        },
    ],
}


def default_query_results(
    query: str, connector_config: Optional[Dict[str, Any]] = None
) -> RowCountList:
    """Dispatch every statement the source issues, keyed on the query text.

    ``SnowflakeConnection._execute_query_with_retry`` reads ``.rowcount`` off
    whatever ``execute()`` returns, so results are wrapped in ``RowCountList``.
    """
    connector_config = (
        CONNECTOR_CONFIG_JSON if connector_config is None else connector_config
    )
    if query == SnowflakeOpenflowQuery.show_deployments():
        return RowCountList(DEPLOYMENT_SHOW_ROWS)
    if query == SnowflakeOpenflowQuery.show_runtimes():
        return RowCountList(RUNTIME_SHOW_ROWS)
    if query == SnowflakeOpenflowQuery.show_connectors():
        return RowCountList(CONNECTOR_SHOW_ROWS)
    if DEPLOYMENT_HISTORY in query:
        return RowCountList(DEPLOYMENT_HISTORY_ROWS)
    if RUNTIME_HISTORY in query:
        return RowCountList(RUNTIME_HISTORY_ROWS)
    if CONNECTOR_HISTORY in query:
        return RowCountList(CONNECTOR_HISTORY_ROWS)
    if query.startswith("DESCRIBE OPENFLOW CONNECTOR"):
        return RowCountList(CONNECTOR_DESCRIBE_ROWS)
    if query.startswith("GET "):
        # A real GET downloads the file into the directory named in the query's
        # 'file://<dir>' argument and returns audit rows, not content. The source
        # reads the file back off disk, so the fake must reproduce that side
        # effect rather than returning the config as a row.
        assert STAGE_URI in query, f"GET against an unexpected stage: {query!r}"
        local_dir = query.rsplit("'file://", 1)[1].rstrip("'")
        (pathlib.Path(local_dir) / CONFIG_FILENAME).write_bytes(
            json.dumps(connector_config).encode()
        )
        return RowCountList(
            [{"file": CONFIG_FILENAME, "size": 1, "status": "DOWNLOADED"}]
        )
    raise AssertionError(f"unexpected query: {query!r}")


def _source_config(stateful: bool, lowercase_urns: bool = True) -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "connection": {
            "account_id": "abc12345",
            "username": "user",
            "password": "pass",
        },
        "env": "PROD",
        "include_openflow_lineage": True,
    }
    if lowercase_urns:
        # Set explicitly, exactly as the fixture recipes do. The source excludes
        # AutoLowercaseUrnsProcessor (which gates on the key being PRESENT IN THE
        # RECIPE, not on the parsed value), so setting it must be equivalent to
        # omitting it -- pinned by
        # test_convert_urns_to_lowercase_key_presence_is_irrelevant below.
        config["convert_urns_to_lowercase"] = True
    if stateful:
        config["stateful_ingestion"] = {
            "enabled": True,
            "remove_stale_metadata": True,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": "http://localhost:8080"}},
            },
        }
    return config


def _pipeline_config(
    output_file: pathlib.Path,
    stateful: bool = False,
    pipeline_name: Optional[str] = None,
    lowercase_urns: bool = True,
) -> PipelineConfig:
    return PipelineConfig(
        pipeline_name=pipeline_name,
        source=SourceConfig(
            type="snowflake-openflow",
            config=_source_config(stateful=stateful, lowercase_urns=lowercase_urns),
        ),
        sink=DynamicTypedConfig(type="file", config={"filename": str(output_file)}),
    )


def _run_pipeline(
    config: PipelineConfig, connector_config: Optional[Dict[str, Any]] = None
) -> Pipeline:
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = partial(
            default_query_results, connector_config=connector_config
        )

        pipeline = Pipeline(config=config)
        pipeline.run()
        pipeline.raise_from_status()
        return pipeline


def _records(output_file: pathlib.Path) -> List[Dict[str, Any]]:
    return json.loads(output_file.read_text())


def _aspect(
    records: List[Dict[str, Any]], entity_urn: str, aspect_name: str
) -> Dict[str, Any]:
    matches = [
        record["aspect"]["json"]
        for record in records
        if record["entityUrn"] == entity_urn and record["aspectName"] == aspect_name
    ]
    assert len(matches) == 1, f"expected one {aspect_name} on {entity_urn}: {matches}"
    return matches[0]


def test_snowflake_openflow_golden(pytestconfig, tmp_path):
    output_file = tmp_path / "snowflake_openflow_mces.json"
    golden_file = (
        pytestconfig.rootpath
        / "tests/integration/snowflake_openflow/snowflake_openflow_mces_golden.json"
    )

    pipeline = _run_pipeline(_pipeline_config(output_file))

    report = cast(SnowflakeOpenflowReport, pipeline.source.get_report())
    assert not report.warnings
    assert not report.failures
    assert report.num_deployments == 1
    assert report.num_runtimes == 1
    # The deleted, view-only connector is filtered before it is ever counted.
    assert report.num_connectors == 1
    # Two, because the fixture replicates two tables -- see the comment on
    # the table-names property for why one would hide the fan-out defect.
    assert report.num_lineage_edges == 2
    assert report.num_table_jobs == 2

    # Asserted here as well as frozen in the golden file, so that re-blessing the
    # golden without reading it cannot quietly accept a regression in the parts
    # that carry the connector's meaning.
    records = _records(output_file)
    assert {record["entityUrn"] for record in records} == {
        DEPLOYMENT_URN,
        RUNTIME_URN,
        FLOW_URN,
        JOB_URN,
        *TABLE_JOB_URNS,
    }

    # Container nesting: runtime under deployment, connector DataFlow under runtime.
    assert _aspect(records, RUNTIME_URN, "container")["container"] == DEPLOYMENT_URN
    assert _aspect(records, FLOW_URN, "container")["container"] == RUNTIME_URN

    # Ownership: the Snowflake OWNER is a role, so a corpGroup, on every entity.
    for urn in (DEPLOYMENT_URN, RUNTIME_URN, FLOW_URN, JOB_URN):
        assert _aspect(records, urn, "ownership")["owners"] == [
            {"owner": f"urn:li:corpGroup:{OWNER_ROLE}", "type": "TECHNICAL_OWNER"}
        ]

    # The connector-level anchor carries NO lineage, and says so explicitly
    # rather than omitting the aspect -- an empty aspect is what clears a
    # flattened fan-out written by an earlier release.
    assert _aspect(records, JOB_URN, "dataJobInputOutput") == {
        "inputDatasets": [],
        "outputDatasets": [],
    }

    # One job per replicated table, each carrying its own 1:1 edge. Flattened
    # onto a single job these two pairs would assert four edges.
    for table in ("mytable", "othertable"):
        table_job = f"{JOB_URN[:-1]}/public.{table})"
        assert _aspect(records, table_job, "dataJobInputOutput") == {
            "inputDatasets": [
                "urn:li:dataset:(urn:li:dataPlatform:postgres,"
                f"mysourcedb.public.{table},PROD)"
            ],
            "outputDatasets": [
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
                f"my_db.public.{table},PROD)"
            ],
        }

    # CONNECTOR_ID exists only in CONNECTOR_HISTORY, never in SHOW OPENFLOW
    # CONNECTORS -- so its presence proves merge_show_and_history ran.
    assert (
        _aspect(records, FLOW_URN, "dataFlowInfo")["customProperties"]["connector_id"]
        == CONNECTOR_ID
    )

    mce_helpers.check_golden_file(
        pytestconfig, output_path=output_file, golden_path=golden_file
    )


def test_stateful_ingestion_emits_the_same_workunits(tmp_path, mock_datahub_graph):
    """Stateful ingestion must not suppress the run's own aspects.

    A live run with ``stateful_ingestion`` enabled once appeared to emit nothing,
    which no unit test could have caught: none of them build a ``Pipeline``, and
    stale-entity removal only exists as a workunit processor on one. The cause
    turned out to be the sink (``mode: SYNC`` writes without counting -- see
    ``tests/unit/snowflake_openflow/test_fixtures.py``), not the connector, and
    this test pins that down: with the state provider mocked and everything else
    identical, the enabled run must produce exactly the same records as the
    golden run.
    """
    plain_output = tmp_path / "plain.json"
    stateful_output = tmp_path / "stateful.json"

    _run_pipeline(_pipeline_config(plain_output))

    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph
        pipeline = _run_pipeline(
            _pipeline_config(
                stateful_output, stateful=True, pipeline_name="test_snowflake_openflow"
            )
        )

    stateful_records = _records(stateful_output)
    assert len(stateful_records) == len(_records(plain_output))
    assert pipeline.sink.get_report().total_records_written == len(stateful_records)


def test_convert_urns_to_lowercase_key_presence_is_irrelevant(tmp_path):
    """Setting the key and omitting it must produce byte-identical records.

    ``AutoLowercaseUrnsProcessor.should_enable`` reads the raw recipe rather than
    the parsed config, so for a source that overrides the mixin default to True
    the key's mere presence used to flip behaviour. This source excludes that
    processor, so the only thing ``convert_urns_to_lowercase`` still drives is
    the in-source fold in ``SnowflakeIdentifierBuilder`` -- which reads the
    parsed value and therefore cannot see the difference.
    """
    with_key = tmp_path / "with_key.json"
    without_key = tmp_path / "without_key.json"

    pipeline = _run_pipeline(_pipeline_config(with_key))
    _run_pipeline(_pipeline_config(without_key, lowercase_urns=False))

    # systemMetadata carries a per-run runId and timestamp, so only the aspects
    # themselves are comparable across two runs.
    def _aspects(path: pathlib.Path) -> List[Dict[str, Any]]:
        return [
            {key: value for key, value in record.items() if key != "systemMetadata"}
            for record in _records(path)
        ]

    assert _aspects(with_key) == _aspects(without_key)
    # The framework's "leaving it disabled" warning belongs to the processor we
    # exclude; it must not reach an operator who never had that behaviour.
    titles = [entry.title for entry in pipeline.source.get_report().warnings]
    assert "URN lowercasing not applied" not in titles


def test_upstream_urns_keep_their_case(tmp_path):
    """A mixed-case upstream table must reach DataHub with its case intact.

    Postgres, MySQL and SQL Server all default ``convert_urns_to_lowercase`` to
    False, so their sources write ``"Public"."MyTable"`` verbatim. Folding this
    connector's inlet would point the edge at a dataset that does not exist, and
    nothing downstream reports that. The rest of the fixture is all-lowercase, so
    only a mixed-case identifier can detect a regression here; the destination in
    the same assertion proves the fold still applies to the Snowflake side.
    """
    output_file = tmp_path / "mixed_case.json"
    mixed_case_config = copy.deepcopy(CONNECTOR_CONFIG_JSON)
    mixed_case_config["configuration"][1]["properties"][
        "Included Comma Separated Source Table Names"
    ] = _wrap('"Public"."MyTable"')

    _run_pipeline(_pipeline_config(output_file), connector_config=mixed_case_config)

    # The edge now lives on that table's own job, not on the connector anchor.
    table_job = (
        f"urn:li:dataJob:({FLOW_URN},{RUNTIME_NAME}/{CONNECTOR_NAME}/Public.MyTable)"
    )
    assert _aspect(_records(output_file), table_job, "dataJobInputOutput") == {
        "inputDatasets": [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,mysourcedb.Public.MyTable,PROD)"
        ],
        "outputDatasets": [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.mytable,PROD)"
        ],
    }
