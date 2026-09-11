from typing import Any, Dict, Iterable, List, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    OpenflowConnector,
    OpenflowDeploymentKey,
    OpenflowRuntimeKey,
    SnowflakeOpenflowSource,
    build_connector_flow,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_config import (
    SnowflakeOpenflowSourceConfig,
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
from datahub.metadata.schema_classes import ContainerClass

PLATFORM = "openflow"
DEPLOYMENT_KEY = "abc12345"
RUNTIME_KEY = "myruntime-1"


def test_deployment_container_urn_is_stable():
    key = OpenflowDeploymentKey(
        platform=PLATFORM, env="PROD", deployment=DEPLOYMENT_KEY
    )
    assert key.as_urn() == "urn:li:container:9786181216cff25b930990059c0c10d4"


def test_runtime_container_urn_is_stable():
    key = OpenflowRuntimeKey(
        platform=PLATFORM, env="PROD", deployment=DEPLOYMENT_KEY, runtime=RUNTIME_KEY
    )
    assert key.as_urn() == "urn:li:container:76708ac37ec74e94ae446ad979f9bec1"


def test_env_does_not_affect_the_container_guid():
    # ContainerKey.guid_dict() excludes env, so a PROD and a DEV deployment with
    # the same key are the SAME container. This is inherited behaviour, asserted
    # here so nobody "fixes" it by adding env to the key and re-keying every URN.
    prod = OpenflowDeploymentKey(
        platform=PLATFORM, env="PROD", deployment=DEPLOYMENT_KEY
    )
    dev = OpenflowDeploymentKey(platform=PLATFORM, env="DEV", deployment=DEPLOYMENT_KEY)
    assert prod.as_urn() == dev.as_urn()


def test_runtime_key_resolves_its_parent_deployment():
    runtime = OpenflowRuntimeKey(
        platform=PLATFORM, env="PROD", deployment=DEPLOYMENT_KEY, runtime=RUNTIME_KEY
    )
    parent = runtime.parent_key()
    assert parent is not None
    assert parent.as_urn() == "urn:li:container:9786181216cff25b930990059c0c10d4"


def test_platform_instance_changes_the_urn():
    # A deployment key is only unique within one Snowflake account, so two
    # accounts can each expose a deployment under the same key. Without a
    # platform_instance in the key their containers would merge into one.
    plain = OpenflowDeploymentKey(
        platform=PLATFORM, env="PROD", deployment=DEPLOYMENT_KEY
    )
    scoped = OpenflowDeploymentKey(
        platform=PLATFORM, env="PROD", instance="acct1", deployment=DEPLOYMENT_KEY
    )
    assert plain.as_urn() != scoped.as_urn()


# --- Connector nesting ------------------------------------------------------

DEPLOYMENT_NAME = "dep-a"
DATAFLOW_URN_PREFIX = "urn:li:dataFlow:"
CONTAINER_URN_PREFIX = "urn:li:container:"
RUNTIME_NAME = "MyRuntime"
MINIMAL_CONNECTION = {
    "connection": {
        "account_id": "abc12345",
        "username": "user",
        "password": "pass",
    }
}


def _make_source(**config_overrides: Any) -> SnowflakeOpenflowSource:
    # Same seam as test_source.py / test_ownership.py: bypass __init__ so no
    # Snowflake connection is opened, and drive extraction through _query_rows.
    config = SnowflakeOpenflowSourceConfig.model_validate(
        {**MINIMAL_CONNECTION, **config_overrides}
    )
    source = object.__new__(SnowflakeOpenflowSource)
    source.config = config
    source.platform = PLATFORM
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


def _source_with_one_connector(connector_runtime_name: str) -> SnowflakeOpenflowSource:
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[method-assign]
        [{"key": DEPLOYMENT_KEY, "name": DEPLOYMENT_NAME}],
        [{"key": RUNTIME_KEY, "name": RUNTIME_NAME, "deployment": DEPLOYMENT_NAME}],
        [{"name": "pg_cdc", "runtime": connector_runtime_name}],
    )
    return source


def _container_aspects(
    workunits: Iterable[MetadataWorkUnit], urn_prefix: str
) -> List[str]:
    return [
        aspect.container
        for workunit in workunits
        if workunit.get_urn().startswith(urn_prefix)
        for aspect in [workunit.get_aspect_of_type(ContainerClass)]
        if aspect is not None
    ]


def test_connector_flow_is_nested_under_its_runtime_container():
    # Without a container the dataFlow has no parent, so AutoBrowsePathV2Processor
    # emits no browse path for it and the connector never appears under its
    # runtime in the browse tree.
    workunits = list(_source_with_one_connector(RUNTIME_NAME).get_workunits_internal())

    expected_urn = OpenflowRuntimeKey(
        platform=PLATFORM,
        env="PROD",
        deployment=DEPLOYMENT_KEY,
        runtime=RUNTIME_KEY,
    ).as_urn()
    assert _container_aspects(workunits, DATAFLOW_URN_PREFIX) == [expected_urn]


def test_connector_flow_container_is_the_runtime_container_actually_emitted():
    # The parent the flow points at must be the very container the run emitted --
    # not a separately rebuilt key that could drift from it.
    workunits = list(_source_with_one_connector(RUNTIME_NAME).get_workunits_internal())

    emitted_container_urns = {
        workunit.get_urn()
        for workunit in workunits
        if workunit.get_urn().startswith(CONTAINER_URN_PREFIX)
    }
    flow_parents = _container_aspects(workunits, DATAFLOW_URN_PREFIX)

    assert len(flow_parents) == 1
    assert flow_parents[0] in emitted_container_urns


def test_connector_flow_has_no_container_when_its_runtime_is_not_visible():
    # SHOW OPENFLOW CONNECTORS is account-wide while runtimes are privilege- and
    # pattern-filtered, so a connector can name a runtime this run never saw. It
    # is still emitted, just un-nested -- dropping it would lose the connector.
    workunits = list(
        _source_with_one_connector("a-runtime-we-cannot-see").get_workunits_internal()
    )

    assert _container_aspects(workunits, DATAFLOW_URN_PREFIX) == []
    assert any(
        workunit.get_urn().startswith(DATAFLOW_URN_PREFIX) for workunit in workunits
    )


def test_an_unnested_flow_still_gets_a_browse_path() -> None:
    # parent_container is passed as `unset`, not None, when the runtime is not
    # visible. None makes the SDK write an EMPTY browsePathsV2, which then
    # suppresses the one auto_browse_path_v2 would derive -- a bug already hit
    # once. Asserting only "no container aspect" cannot tell the two apart, so
    # reverting `unset` to None passed the whole suite.
    flow = build_connector_flow(
        OpenflowConnector(name="conn", runtime_name="rt"),
        None,
        platform_instance=None,
        env="PROD",
        parent_container=None,
    )
    aspects = [
        wu.metadata.aspectName
        for wu in flow.as_workunits()
        if hasattr(wu.metadata, "aspectName")
    ]
    assert "browsePathsV2" not in aspects, (
        "an un-nested flow must leave browsePathsV2 off entirely so "
        "auto_browse_path_v2 can derive one; an empty aspect suppresses it"
    )


# --- Orphaned connectors ----------------------------------------------------

ORPHAN_WARNING_TITLE = "Connector with no visible parent runtime"


def _warning_titles(report: SnowflakeOpenflowReport) -> List[Optional[str]]:
    return [entry.title for entry in report.warnings]


def test_connector_orphaned_by_an_unseen_runtime_is_counted_and_warned():
    # A connector naming a runtime this run never saw at all is an anomaly: the
    # runtime is invisible to the role, or the two surfaces disagree on its
    # name. Without a counter and a warning the flow is silently un-nested and
    # an operator cannot tell that from an intentional filter.
    source = _source_with_one_connector("a-runtime-we-cannot-see")

    list(source.get_workunits_internal())

    assert source.report.num_connectors_without_runtime_parent == 1
    assert ORPHAN_WARNING_TITLE in _warning_titles(source.report)


def test_connector_orphaned_by_the_runtime_pattern_is_counted_but_not_warned():
    # The operator asked not to ingest this runtime, so its connectors being
    # un-nested is the requested outcome, not an anomaly -- warning on every run
    # would train operators to ignore the warning. The counter still moves, so
    # the total is visible either way.
    source = _make_source(runtime_pattern={"deny": [RUNTIME_NAME]})
    source._query_rows = _fake_query_rows(  # type: ignore[method-assign]
        [{"key": DEPLOYMENT_KEY, "name": DEPLOYMENT_NAME}],
        [{"key": RUNTIME_KEY, "name": RUNTIME_NAME, "deployment": DEPLOYMENT_NAME}],
        [{"name": "pg_cdc", "runtime": RUNTIME_NAME}],
    )

    list(source.get_workunits_internal())

    assert source.report.num_connectors_without_runtime_parent == 1
    assert ORPHAN_WARNING_TITLE not in _warning_titles(source.report)


def test_nested_connector_is_not_counted_as_orphaned():
    source = _source_with_one_connector(RUNTIME_NAME)

    list(source.get_workunits_internal())

    assert source.report.num_connectors_without_runtime_parent == 0


AMBIGUOUS_WARNING_TITLE = "Runtime name is not unique across deployments"


def test_runtime_name_shared_by_two_deployments_leaves_connectors_unnested():
    # Runtime names are scoped to their deployment, so two deployments may each
    # hold a runtime called `default`. A connector row carries only the runtime
    # NAME, never a deployment, so the name cannot be resolved to one of them.
    #
    # The failure this prevents is silent in the worst way: a flat name->key map
    # takes the last writer, the lookup SUCCEEDS, and every connector in the first
    # deployment nests under the second deployment's runtime. Nothing fires,
    # because num_connectors_without_runtime_parent only counts lookup misses.
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[method-assign]
        [
            {"key": "dep-a", "name": "DeploymentA"},
            {"key": "dep-b", "name": "DeploymentB"},
        ],
        [
            {"key": "rt-a", "name": "default", "deployment": "DeploymentA"},
            {"key": "rt-b", "name": "default", "deployment": "DeploymentB"},
        ],
        [{"name": "pg_cdc", "runtime": "default"}],
    )

    workunits = list(source.get_workunits_internal())

    assert source.report.num_connectors_with_ambiguous_runtime == 1
    assert AMBIGUOUS_WARNING_TITLE in _warning_titles(source.report)
    # Un-nested rather than nested under a guess: no container aspect on the flow.
    assert _container_aspects(workunits, "urn:li:dataFlow:") == []


def test_unique_runtime_names_across_deployments_still_nest():
    # The guard must not fire on the ordinary shape, or every multi-deployment
    # account loses its nesting.
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[method-assign]
        [
            {"key": "dep-a", "name": "DeploymentA"},
            {"key": "dep-b", "name": "DeploymentB"},
        ],
        [
            {"key": "rt-a", "name": "alpha", "deployment": "DeploymentA"},
            {"key": "rt-b", "name": "beta", "deployment": "DeploymentB"},
        ],
        [{"name": "pg_cdc", "runtime": "beta"}],
    )

    workunits = list(source.get_workunits_internal())

    assert source.report.num_connectors_with_ambiguous_runtime == 0
    assert AMBIGUOUS_WARNING_TITLE not in _warning_titles(source.report)
    assert len(_container_aspects(workunits, "urn:li:dataFlow:")) == 1
