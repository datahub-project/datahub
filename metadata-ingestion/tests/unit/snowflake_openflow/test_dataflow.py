from datahub.ingestion.source.common.subtypes import DataFlowSubTypes, DataJobSubTypes
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    _MAX_READABLE_JOB_NAME,
    ConnectorTableLineage,
    _table_job_name,
    build_connector_flow,
    build_connector_table_job,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_models import (
    OpenflowConnector,
)

CONNECTOR = OpenflowConnector(
    connector_id="1",
    name="pg_cdc",
    runtime_name="MyRuntime",
    connector_definition="OPENFLOW_POSTGRES_CDC",
    default_version="3",
)


def test_flow_urn_uses_the_composite_runtime_and_connector_name():
    flow = build_connector_flow(CONNECTOR, platform_instance=None, env="PROD")
    assert str(flow.urn) == "urn:li:dataFlow:(openflow,MyRuntime/pg_cdc,PROD)"


def test_flow_urn_is_stable_when_the_view_has_not_caught_up():
    # The ACCOUNT_USAGE views lag ~20 min, so CONNECTOR_ID is absent for a freshly
    # created connector. Identity must not depend on it, or the same connector gets
    # two URNs either side of the lag.
    # connector_id omitted entirely — that is what a lagging view actually looks
    # like. Passing a stand-in value here would test nothing.
    without_id = OpenflowConnector(name="pg_cdc", runtime_name="MyRuntime")
    assert without_id.connector_id is None
    assert (
        str(build_connector_flow(without_id, platform_instance=None, env="PROD").urn)
        == "urn:li:dataFlow:(openflow,MyRuntime/pg_cdc,PROD)"
    )


def test_connectors_sharing_a_name_in_different_runtimes_do_not_collide():
    a = OpenflowConnector(connector_id="1", name="pg_cdc", runtime_name="RuntimeA")
    b = OpenflowConnector(connector_id="2", name="pg_cdc", runtime_name="RuntimeB")
    urn_a = str(build_connector_flow(a, platform_instance=None, env="PROD").urn)
    urn_b = str(build_connector_flow(b, platform_instance=None, env="PROD").urn)
    assert urn_a != urn_b


def test_flow_carries_the_connector_subtype_and_definition():
    flow = build_connector_flow(CONNECTOR, platform_instance=None, env="PROD")
    assert flow.subtype == DataFlowSubTypes.OPENFLOW_CONNECTOR
    assert flow.custom_properties["connector_definition"] == "OPENFLOW_POSTGRES_CDC"
    assert flow.custom_properties["default_version"] == "3"


def test_table_job_is_nested_in_its_flow():
    # A job is keyed on the flow's composite connector.key plus the table, so
    # its URN nests inside the flow URN -- not on CONNECTOR_ID, for the
    # identical reasons the flow isn't.
    flow = build_connector_flow(CONNECTOR, platform_instance=None, env="PROD")
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )
    job = build_connector_table_job(CONNECTOR, flow, pair)
    assert str(job.urn) == (
        "urn:li:dataJob:(urn:li:dataFlow:(openflow,MyRuntime/pg_cdc,PROD),"
        "MyRuntime/pg_cdc/public.t)"
    )
    assert job.subtype == DataJobSubTypes.OPENFLOW_CONNECTOR_SYNC


def test_a_table_job_with_no_upstream_still_carries_its_outlet():
    # Losing the upstream half must not lose the edge: the destination is what
    # the connector definitely did, and is emitted either way.
    flow = build_connector_flow(CONNECTOR, platform_instance=None, env="PROD")
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )
    job = build_connector_table_job(CONNECTOR, flow, pair)
    assert job.inlets == []
    assert [str(outlet) for outlet in job.outlets] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)"
    ]


def test_display_name_prefers_the_human_label():
    connector = OpenflowConnector(
        name="pg_cdc",
        runtime_name="MyRuntime",
        connector_id="1",
        display_name="Postgres CDC",
    )
    flow = build_connector_flow(connector, platform_instance=None, env="PROD")
    # The URN stays keyed on the composite runtime/name; only the label changes.
    assert str(flow.urn) == "urn:li:dataFlow:(openflow,MyRuntime/pg_cdc,PROD)"
    assert flow.display_name == "Postgres CDC"


def test_a_long_table_name_falls_back_to_a_stable_digest() -> None:
    # The readable id would blow the urn-length budget, so it degrades to a
    # hash. hashlib rather than the builtin salted hash() specifically so the
    # id is the SAME on the next run -- a per-process salt would rename the
    # entity every ingest and orphan the previous one.
    connector = OpenflowConnector(name="c" * 120, runtime_name="r" * 120)
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )

    first = _table_job_name(connector, pair)
    second = _table_job_name(connector, pair)

    assert first == second, "the fallback id must be stable across runs"
    # The point of the budget: the result must actually fit inside it.
    assert len(first) <= _MAX_READABLE_JOB_NAME
    # The key prefix is truncated, not carried whole -- that is what keeps the
    # result inside the budget when the key alone would exceed it.
    prefix, digest = first.rsplit("/", 1)
    assert connector.key.startswith(prefix)
    assert len(digest) == 16 and digest != "public.t"


def test_a_short_table_name_stays_readable() -> None:
    connector = OpenflowConnector(name="conn", runtime_name="rt")
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )
    assert _table_job_name(connector, pair) == "rt/conn/public.t"
