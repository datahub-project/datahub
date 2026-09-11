import urllib.parse

import pytest

from datahub.ingestion.source.common.subtypes import DataFlowSubTypes, DataJobSubTypes
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    ConnectorTableLineage,
    _owner_group_urn,
    build_connector_flow,
    build_connector_table_job,
    encoded_urn_len,
    owner_classes_for,
    urn_fits,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_models import (
    OpenflowConnector,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_urns import (
    MAX_PLATFORM_INSTANCE_BYTES,
)

CONNECTOR = OpenflowConnector(
    connector_id="1",
    name="pg_cdc",
    runtime_name="MyRuntime",
    connector_definition="OPENFLOW_POSTGRES_CDC",
    default_version="3",
)


def test_flow_urn_uses_the_composite_runtime_and_connector_name():
    flow = build_connector_flow(
        CONNECTOR,
        owner_classes_for(_owner_group_urn(CONNECTOR.owner)),
        platform_instance=None,
        env="PROD",
    )
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
        str(
            build_connector_flow(
                without_id,
                owner_classes_for(_owner_group_urn(without_id.owner)),
                platform_instance=None,
                env="PROD",
            ).urn
        )
        == "urn:li:dataFlow:(openflow,MyRuntime/pg_cdc,PROD)"
    )


def test_connectors_sharing_a_name_in_different_runtimes_do_not_collide():
    a = OpenflowConnector(connector_id="1", name="pg_cdc", runtime_name="RuntimeA")
    b = OpenflowConnector(connector_id="2", name="pg_cdc", runtime_name="RuntimeB")
    urn_a = str(
        build_connector_flow(
            a,
            owner_classes_for(_owner_group_urn(a.owner)),
            platform_instance=None,
            env="PROD",
        ).urn
    )
    urn_b = str(
        build_connector_flow(
            b,
            owner_classes_for(_owner_group_urn(b.owner)),
            platform_instance=None,
            env="PROD",
        ).urn
    )
    assert urn_a != urn_b


def test_flow_carries_the_connector_subtype_and_definition():
    flow = build_connector_flow(
        CONNECTOR,
        owner_classes_for(_owner_group_urn(CONNECTOR.owner)),
        platform_instance=None,
        env="PROD",
    )
    assert flow.subtype == DataFlowSubTypes.OPENFLOW_CONNECTOR
    assert flow.custom_properties["connector_definition"] == "OPENFLOW_POSTGRES_CDC"
    assert flow.custom_properties["default_version"] == "3"


def test_table_job_is_nested_in_its_flow():
    # A job is keyed on the flow's composite connector.key plus the table, so
    # its URN nests inside the flow URN -- not on CONNECTOR_ID, for the
    # identical reasons the flow isn't.
    flow = build_connector_flow(
        CONNECTOR,
        owner_classes_for(_owner_group_urn(CONNECTOR.owner)),
        platform_instance=None,
        env="PROD",
    )
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )
    job = build_connector_table_job(
        CONNECTOR, flow, pair, owner_classes_for(_owner_group_urn(CONNECTOR.owner))
    )
    assert str(job.urn) == (
        "urn:li:dataJob:(urn:li:dataFlow:(openflow,MyRuntime/pg_cdc,PROD),"
        "MyRuntime/pg_cdc/public.t)"
    )
    assert job.subtype == DataJobSubTypes.OPENFLOW_CONNECTOR_SYNC


def test_a_table_job_with_no_upstream_still_carries_its_outlet():
    # Losing the upstream half must not lose the edge: the destination is what
    # the connector definitely did, and is emitted either way.
    flow = build_connector_flow(
        CONNECTOR,
        owner_classes_for(_owner_group_urn(CONNECTOR.owner)),
        platform_instance=None,
        env="PROD",
    )
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )
    job = build_connector_table_job(
        CONNECTOR, flow, pair, owner_classes_for(_owner_group_urn(CONNECTOR.owner))
    )
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
    flow = build_connector_flow(
        connector,
        owner_classes_for(_owner_group_urn(connector.owner)),
        platform_instance=None,
        env="PROD",
    )
    # The URN stays keyed on the composite runtime/name; only the label changes.
    assert str(flow.urn) == "urn:li:dataFlow:(openflow,MyRuntime/pg_cdc,PROD)"
    assert flow.display_name == "Postgres CDC"


@pytest.mark.parametrize(
    "char", ["c", "\u6570", "\U0001f600"], ids=["ascii", "cjk", "emoji"]
)
@pytest.mark.parametrize(
    ("name_len", "runtime_len"),
    [
        pytest.param(120, 120, id="two 120-char identifiers"),
        pytest.param(255, 255, id="two max-length Snowflake identifiers"),
    ],
)
def test_a_long_name_keeps_both_urns_inside_what_gms_accepts(
    name_len: int, runtime_len: int, char: str
) -> None:
    # The limit GMS enforces is on the URL-ENCODED urn (512 bytes,
    # UrnValidationUtil.URN_NUM_BYTES_LIMIT), not on any component of it. An
    # earlier guard bounded the job NAME to 200 characters, was measured as
    # correct against that number, and still produced a 573-byte DataJob urn --
    # because the job urn nests the flow urn, so connector.key appears twice.
    # GMS rejects the aspect and that table's lineage is lost. Assert the thing
    # the server measures.
    # Non-ASCII is the case a character budget silently gets wrong: one CJK
    # character URL-encodes to nine bytes and one emoji to twelve, so an
    # 80-character prefix is 720 or 960 bytes of urn by itself. Shortening once
    # and returning produced a 1550-byte DataJob urn for a CJK name -- inside
    # the character budget, far outside what GMS stores.
    connector = OpenflowConnector(name=char * name_len, runtime_name=char * runtime_len)
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )

    flow = build_connector_flow(
        connector,
        owner_classes_for(_owner_group_urn(connector.owner)),
        platform_instance=None,
        env="PROD",
    )
    job = build_connector_table_job(
        connector, flow, pair, owner_classes_for(_owner_group_urn(connector.owner))
    )

    assert urn_fits(flow.urn), len(urllib.parse.quote_plus(str(flow.urn)))
    assert urn_fits(job.urn), len(urllib.parse.quote_plus(str(job.urn)))
    # Stable across runs: a per-process salt would rename the entity every
    # ingest and orphan the previous one.
    assert str(job.urn) == str(
        build_connector_table_job(
            connector, flow, pair, owner_classes_for(_owner_group_urn(connector.owner))
        ).urn
    )


def test_an_ordinary_name_is_left_readable() -> None:
    # The shortening must not fire for normal names, or every urn becomes a hash.
    connector = OpenflowConnector(name="pg_cdc", runtime_name="MyRuntime")
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="mytable",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.mytable,PROD)",
    )

    flow = build_connector_flow(
        connector,
        owner_classes_for(_owner_group_urn(connector.owner)),
        platform_instance=None,
        env="PROD",
    )
    job = build_connector_table_job(
        connector, flow, pair, owner_classes_for(_owner_group_urn(connector.owner))
    )

    assert str(flow.urn) == "urn:li:dataFlow:(openflow,MyRuntime/pg_cdc,PROD)"
    assert str(job.urn).endswith("MyRuntime/pg_cdc/public.mytable)")


def test_a_short_table_name_stays_readable() -> None:
    connector = OpenflowConnector(name="conn", runtime_name="rt")
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )
    flow = build_connector_flow(
        connector,
        owner_classes_for(_owner_group_urn(connector.owner)),
        platform_instance=None,
        env="PROD",
    )
    job = build_connector_table_job(
        connector, flow, pair, owner_classes_for(_owner_group_urn(connector.owner))
    )
    assert str(job.urn).endswith("rt/conn/public.t)")


@pytest.mark.parametrize(
    "char",
    ["c", "\u6570", "\U0001f600", "a b", "n%m", "x~y"],
    ids=["ascii", "cjk", "emoji", "space", "percent", "tilde"],
)
def test_no_identifier_length_produces_an_urn_gms_would_reject(char: str) -> None:
    # Swept rather than sampled, because every previous attempt at this guard
    # passed its own chosen example and failed elsewhere: a 200-char budget that
    # emitted 258, then an encoded-byte check whose 80-CHARACTER shortening blew
    # up on CJK, then a digest floor that could not help the DataJob because its
    # urn nests the whole DataFlow urn -- a 467-byte flow admitted no job at all.
    #
    # "~" is in the alphabet deliberately: Python's quote_plus passes it through
    # while java.net.URLEncoder writes %7E, so measuring with quote_plus
    # under-counted exactly the character the shortener used to insert.
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )

    for length in (1, 50, 120, 200, 209, 230, 255):
        connector = OpenflowConnector(name=char * length, runtime_name=char * length)
        flow = build_connector_flow(
            connector,
            owner_classes_for(_owner_group_urn(connector.owner)),
            platform_instance=None,
            env="PROD",
        )
        job = build_connector_table_job(
            connector, flow, pair, owner_classes_for(_owner_group_urn(connector.owner))
        )

        assert encoded_urn_len(flow.urn) <= 512, (char, length, "flow")
        assert encoded_urn_len(job.urn) <= 512, (char, length, "job")


def test_the_length_measured_is_the_one_gms_measures() -> None:
    # Pins the encoder itself against java.net.URLEncoder's rules, since the
    # sweep above is only as good as what it measures with.
    assert encoded_urn_len("a~b") == 5  # quote_plus says 3
    assert encoded_urn_len("a*b") == 3  # quote_plus says 5
    assert encoded_urn_len("a b") == 3  # space is "+", one byte
    assert encoded_urn_len("\u6570") == 9  # one CJK char, three utf-8 bytes


def test_the_platform_instance_limit_matches_what_the_builders_actually_accept() -> (
    None
):
    # The config limit is derived from constants, and a derivation can drift
    # from the thing it describes. This binary-searches the REAL builders for
    # the longest platform_instance whose urns still fit, and asserts the
    # config rejects exactly one byte past it -- so the constant cannot become
    # either too strict (rejecting recipes that would work, which a hardcoded
    # 200 did, refusing 203 valid bytes) or too loose (accepting recipes whose
    # every aspect GMS discards).
    pair = ConnectorTableLineage(
        source_schema="public",
        source_table="t",
        outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
    )

    def both_fit(instance: str) -> bool:
        connector = OpenflowConnector(name="c" * 255, runtime_name="r" * 255)
        flow = build_connector_flow(
            connector,
            owner_classes_for(_owner_group_urn(connector.owner)),
            platform_instance=instance,
            env="PROD",
        )
        job = build_connector_table_job(
            connector, flow, pair, owner_classes_for(_owner_group_urn(connector.owner))
        )
        return urn_fits(flow.urn) and urn_fits(job.urn)

    low, high = 0, 600
    while low < high:
        mid = (low + high + 1) // 2
        if both_fit("x" * mid):
            low = mid
        else:
            high = mid - 1

    assert low == MAX_PLATFORM_INSTANCE_BYTES, (
        f"the config limit is {MAX_PLATFORM_INSTANCE_BYTES} but the builders "
        f"accept up to {low}"
    )
