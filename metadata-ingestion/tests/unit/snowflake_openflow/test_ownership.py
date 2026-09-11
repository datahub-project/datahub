import ast
import pathlib
from typing import Any, Dict, Iterable, List, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    ConnectorTableLineage,
    SnowflakeOpenflowSource,
    build_connector_flow,
    build_connector_table_job,
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
from datahub.metadata.schema_classes import OwnershipClass, OwnershipTypeClass

MINIMAL_CONNECTION = {
    "connection": {
        "account_id": "abc12345",
        "username": "user",
        "password": "pass",
    }
}

# Snowflake's OWNER column holds a ROLE name, so the only correct entity type is
# corpGroup. Asserting the literal urn (rather than just calling make_group_urn
# in the test too) is what makes the expectation independent of the code path.
OWNER_ROLE = "MY_ROLE"
OWNER_GROUP_URN = "urn:li:corpGroup:MY_ROLE"
CORP_GROUP_PREFIX = "urn:li:corpGroup:"


def _make_source(**config_overrides: Any) -> SnowflakeOpenflowSource:
    # Same seam as test_source.py: bypass __init__ so no Snowflake connection is
    # opened, and drive extraction through _query_rows.
    config = SnowflakeOpenflowSourceConfig.model_validate(
        {**MINIMAL_CONNECTION, **config_overrides}
    )
    source = object.__new__(SnowflakeOpenflowSource)
    source.config = config
    source.platform = "openflow"
    source.report = SnowflakeOpenflowReport()
    return source


def _fake_query_rows(
    deployment_show: List[Dict[str, Any]],
    runtime_show: List[Dict[str, Any]],
    connector_show: List[Dict[str, Any]],
) -> Any:
    def fake(query: str) -> List[Dict[str, Any]]:
        if query == SnowflakeOpenflowQuery.show_deployments():
            return deployment_show
        if query == SnowflakeOpenflowQuery.show_runtimes():
            return runtime_show
        if query == SnowflakeOpenflowQuery.show_connectors():
            return connector_show
        if (
            DEPLOYMENT_HISTORY in query
            or RUNTIME_HISTORY in query
            or CONNECTOR_HISTORY in query
        ):
            return []
        raise AssertionError(f"unexpected query: {query!r}")

    return fake


def _ownership_aspects(
    workunits: Iterable[MetadataWorkUnit],
) -> List[OwnershipClass]:
    aspects: List[OwnershipClass] = []
    for workunit in workunits:
        aspect: Optional[OwnershipClass] = workunit.get_aspect_of_type(OwnershipClass)
        if aspect is not None:
            aspects.append(aspect)
    return aspects


# --- Containers -------------------------------------------------------------


def test_deployment_container_owner_is_a_corp_group():
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[method-assign]
        [{"key": "dep-a", "name": "dep-a", "owner": OWNER_ROLE}], [], []
    )

    ownership = _ownership_aspects(source.get_workunits_internal())

    assert len(ownership) == 1
    owners = ownership[0].owners
    assert [owner.owner for owner in owners] == [OWNER_GROUP_URN]
    # The trap this catches: routing the role through make_user_urn (which is
    # what a bare string does) yields urn:li:corpuser:MY_ROLE -- a well-formed
    # urn naming a user that does not exist.
    assert owners[0].owner.startswith(CORP_GROUP_PREFIX)
    assert source.report.num_owners_emitted == 1


def test_runtime_container_owner_is_a_corp_group():
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[method-assign]
        [{"key": "dep-a", "name": "dep-a"}],
        [{"key": "rt-a", "name": "rt-a", "deployment": "dep-a", "owner": OWNER_ROLE}],
        [],
    )

    ownership = _ownership_aspects(source.get_workunits_internal())

    # Only the runtime carries an owner here, so the deployment contributes none.
    assert len(ownership) == 1
    assert [owner.owner for owner in ownership[0].owners] == [OWNER_GROUP_URN]
    assert source.report.num_owners_emitted == 1


def test_container_emits_no_ownership_aspect_when_owner_is_absent():
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[method-assign]
        [{"key": "dep-a", "name": "dep-a"}],
        [{"key": "rt-a", "name": "rt-a", "deployment": "dep-a"}],
        [],
    )

    # No aspect at all, rather than an OwnershipClass with an empty owners list:
    # an empty aspect would overwrite an owner set in DataHub by hand.
    assert _ownership_aspects(source.get_workunits_internal()) == []
    assert source.report.num_owners_emitted == 0


# --- DataFlow / DataJob (SDK v2) --------------------------------------------


def test_connector_flow_owner_is_a_technical_corp_group_owner():
    connector = OpenflowConnector(
        name="pg_cdc", runtime_name="my_runtime", owner=OWNER_ROLE
    )
    flow = build_connector_flow(connector, platform_instance=None, env="PROD")

    ownership = _ownership_aspects(flow.as_workunits())

    assert len(ownership) == 1
    owners = ownership[0].owners
    assert len(owners) == 1
    assert owners[0].owner == OWNER_GROUP_URN
    assert owners[0].owner.startswith(CORP_GROUP_PREFIX)
    assert owners[0].type == OwnershipTypeClass.TECHNICAL_OWNER


_PAIR = ConnectorTableLineage(
    source_schema="public",
    source_table="t",
    outlet="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.t,PROD)",
)


def test_connector_job_owner_is_a_technical_corp_group_owner():
    connector = OpenflowConnector(
        name="pg_cdc", runtime_name="my_runtime", owner=OWNER_ROLE
    )
    flow = build_connector_flow(connector, platform_instance=None, env="PROD")
    job = build_connector_table_job(connector, flow, _PAIR)

    ownership = _ownership_aspects(job.as_workunits())

    assert len(ownership) == 1
    owners = ownership[0].owners
    assert len(owners) == 1
    assert owners[0].owner == OWNER_GROUP_URN
    assert owners[0].owner.startswith(CORP_GROUP_PREFIX)
    assert owners[0].type == OwnershipTypeClass.TECHNICAL_OWNER


def test_connector_emits_no_ownership_aspect_when_owner_is_absent():
    connector = OpenflowConnector(name="pg_cdc", runtime_name="my_runtime")
    flow = build_connector_flow(connector, platform_instance=None, env="PROD")
    job = build_connector_table_job(connector, flow, _PAIR)

    assert _ownership_aspects(flow.as_workunits()) == []
    assert _ownership_aspects(job.as_workunits()) == []


def test_connector_owner_is_counted_on_the_report():
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[method-assign]
        [],
        [],
        [
            {
                "name": "pg_cdc",
                "runtime": "my_runtime",
                "owner": OWNER_ROLE,
            }
        ],
    )

    list(source.get_workunits_internal())

    # One, on the DataFlow. There is no connector-level DataJob any more -- it
    # duplicated the DataFlow -- and this connector replicates no tables, so no
    # per-table job carries an owner either. A connector WITH tables is covered
    # by the integration golden, which asserts ownership on each table job.
    assert source.report.num_owners_emitted == 1


def test_an_owning_role_whose_urn_is_too_long_drops_ownership_and_says_so() -> None:
    # Owner.owner is a Urn-typed field, so UrnAnnotationValidator applies the
    # same 512-byte limit as an entity's own urn -- an over-long one costs the
    # WHOLE Ownership aspect, not just the owner. A Snowflake role is an
    # identifier of up to 255 characters: fine in ASCII (278 bytes), not in
    # CJK, where 165 characters already encode to 1508. Not shortened, for the
    # same reason as a dataset urn: a truncated corpGroup names a different
    # group or none.
    source = _make_source()

    source._account_for_owner("数" * 165, "rt/c")

    assert source.report.num_owners_dropped_urn_too_long == 1
    assert source.report.num_owners_emitted == 0
    assert "Ownership dropped: owner urn too long" in [
        e.title for e in source.report.warnings
    ]


def test_an_ordinary_owning_role_is_counted_not_dropped() -> None:
    source = _make_source()

    source._account_for_owner("ANALYST", "rt/c")

    assert source.report.num_owners_emitted == 1
    assert source.report.num_owners_dropped_urn_too_long == 0


def test_every_ownership_bearing_entity_routes_through_one_accounting_path() -> None:
    # The counters lied for three of four call sites: deployments, runtimes and
    # per-table jobs incremented num_owners_emitted on a truthy owner string
    # without checking the urn actually fit, so a dropped owner was reported as
    # emitted. Pinned structurally rather than with four near-identical
    # behavioural tests, because a fifth call site would not get its own test
    # but would break a count.
    #
    # Via the AST, not a string search: the first version of this asserted the
    # literal "num_owners_emitted += 1" appeared once, which would have broken
    # on reformatting and passed on `+=1`. What matters is the number of
    # augmented assignments to that attribute, which is a property of the
    # parse tree, not of the spelling.
    source_file = (
        pathlib.Path(__file__).resolve().parents[3]
        / "src/datahub/ingestion/source/snowflake/snowflake_openflow.py"
    )
    COUNTER = "num_owners_emitted"
    tree = ast.parse(source_file.read_text())

    def writes_the_counter(node: ast.AST) -> bool:
        # Every shape that assigns to it, not just `+= 1`. Probed the narrow
        # version: it missed `x = x + 1` and setattr(). `+= 1` is the idiom
        # used throughout this file, so those are unlikely -- but a guard whose
        # own blind spots are known and unclosed is the thing this connector
        # has repeatedly been bitten by.
        if isinstance(node, ast.AugAssign):
            target: ast.AST = node.target
            return isinstance(target, ast.Attribute) and target.attr == COUNTER
        if isinstance(node, ast.Assign):
            return any(
                isinstance(t, ast.Attribute) and t.attr == COUNTER for t in node.targets
            )
        if isinstance(node, ast.Call):
            return (
                isinstance(node.func, ast.Name)
                and node.func.id == "setattr"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == COUNTER
            )
        return False

    sites = [node for node in ast.walk(tree) if writes_the_counter(node)]

    assert len(sites) == 1, (
        f"ownership is counted at {len(sites)} sites (lines "
        f"{[getattr(n, 'lineno', '?') for n in sites]}); it must be one, "
        "because a second site "
        "is a site that can forget to check whether the owner urn fits"
    )
