from typing import Any, Dict, Iterable, List, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    SnowflakeOpenflowSource,
    build_connector_flow,
    build_connector_job,
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


def test_connector_job_owner_is_a_technical_corp_group_owner():
    connector = OpenflowConnector(
        name="pg_cdc", runtime_name="my_runtime", owner=OWNER_ROLE
    )
    flow = build_connector_flow(connector, platform_instance=None, env="PROD")
    job = build_connector_job(connector, flow, inlets=[], outlets=[])

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
    job = build_connector_job(connector, flow, inlets=[], outlets=[])

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

    # One owner on the DataFlow and one on the DataJob.
    assert source.report.num_owners_emitted == 2
