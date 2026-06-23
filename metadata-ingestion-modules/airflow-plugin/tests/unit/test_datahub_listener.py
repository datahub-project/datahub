"""Unit tests for the Airflow 3 DataHub listener."""

from types import SimpleNamespace
from typing import Any, List
from unittest.mock import MagicMock, patch

from datahub_airflow_plugin.airflow3.datahub_listener import DataHubListener


class AssetAlias:
    """Minimal AssetAlias stand-in.

    is_airflow_asset_alias() walks the MRO looking for class name 'AssetAlias',
    so the class itself must be named AssetAlias. It must NOT have a 'uri'
    attribute (presence of uri excludes the object from is_airflow_asset_alias).
    """

    def __init__(self, name: str):
        self.name = name


class Asset:
    """Minimal Airflow Asset stand-in (has uri)."""

    def __init__(self, uri: str):
        self.uri = uri


def _make_listener_stub(capture_airflow_assets: bool = True) -> Any:
    """Return a SimpleNamespace bound to DataHubListener._get_outlet_urns.

    We avoid instantiating DataHubListener to skip its emitter / log-level
    setup. _get_outlet_urns only reads self.config.{capture_airflow_assets,
    cluster}, so a SimpleNamespace is sufficient.
    """
    config = SimpleNamespace(
        capture_airflow_assets=capture_airflow_assets,
        cluster="PROD",
    )
    return SimpleNamespace(config=config)


def _call_get_outlet_urns(
    stub: Any,
    outlets: List[Any],
    complete: bool,
    task_instance: Any = None,
    session: Any = None,
) -> List[str]:
    """Invoke the unbound _get_outlet_urns method against the stub."""
    task = MagicMock()
    task.task_id = "my_task"
    dagrun = MagicMock()
    dagrun.dag_id = "my_dag"
    dagrun.run_id = "run_1"
    if task_instance is None:
        task_instance = MagicMock()
        task_instance.map_index = -1

    with patch(
        "datahub_airflow_plugin.airflow3.datahub_listener.get_task_outlets",
        return_value=outlets,
    ):
        return DataHubListener._get_outlet_urns(
            stub,
            task,
            dagrun,
            task_instance,
            complete=complete,
            session=session,
        )


class TestGetOutletUrnsAliasOmittedAtStart:
    """AssetAlias outlets must NOT contribute URNs at task start.

    The real Asset URI is only known once the task body runs, so emitting a
    placeholder URN derived from the alias name would create a phantom
    lineage edge that is later orphaned when the task completes with the
    resolved URN.
    """

    def test_alias_only_outlet_returns_empty_when_not_complete(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [AssetAlias("parsed_data")]

        with (
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events"
            ) as mock_in_proc,
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_resolved_alias_events"
            ) as mock_db,
        ):
            urns = _call_get_outlet_urns(stub, outlets, complete=False)

        assert urns == []
        # Neither resolution path should run at task start
        mock_in_proc.assert_not_called()
        mock_db.assert_not_called()

    def test_mixed_outlets_only_emit_non_alias_at_start(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [Asset("s3://bucket/known"), AssetAlias("parsed_data")]

        with patch(
            "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events"
        ) as mock_in_proc:
            urns = _call_get_outlet_urns(stub, outlets, complete=False)

        # The concrete Asset is emitted; the alias is silently dropped.
        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/known,PROD)"]
        mock_in_proc.assert_not_called()


class TestGetOutletUrnsAliasResolutionAtCompletion:
    """At task completion the listener must resolve alias outlets via
    runtime context (or DB fallback) and forward the Airflow-provided
    session."""

    def test_in_process_resolution_used_when_available(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [AssetAlias("parsed_data")]
        resolved = ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/runtime,PROD)"]

        with (
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events",
                return_value=resolved,
            ) as mock_in_proc,
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_resolved_alias_events"
            ) as mock_db,
        ):
            urns = _call_get_outlet_urns(stub, outlets, complete=True)

        assert urns == resolved
        mock_in_proc.assert_called_once()
        # DB fallback must NOT run when in-process resolution succeeded —
        # avoids querying the Airflow DB on the hot path.
        mock_db.assert_not_called()

    def test_db_fallback_used_when_in_process_returns_empty(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [AssetAlias("parsed_data")]
        resolved = ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/fromdb,PROD)"]
        fake_session = MagicMock(name="airflow_session")
        task_instance = MagicMock()
        task_instance.map_index = -1

        with (
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events",
                return_value=[],
            ),
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_resolved_alias_events",
                return_value=resolved,
            ) as mock_db,
        ):
            urns = _call_get_outlet_urns(
                stub,
                outlets,
                complete=True,
                task_instance=task_instance,
                session=fake_session,
            )

        assert urns == resolved
        # Caller-provided session must be forwarded — create_session() is
        # blocked inside Airflow 3.0 listener threads, so without forwarding
        # the DB query path would always fail.
        mock_db.assert_called_once()
        call_kwargs = mock_db.call_args.kwargs
        assert call_kwargs["session"] is fake_session
        assert call_kwargs["dag_id"] == "my_dag"
        assert call_kwargs["task_id"] == "my_task"
        assert call_kwargs["run_id"] == "run_1"

    def test_no_alias_outlets_skips_resolution_entirely(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [Asset("s3://bucket/static")]

        with (
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events"
            ) as mock_in_proc,
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_resolved_alias_events"
            ) as mock_db,
        ):
            urns = _call_get_outlet_urns(stub, outlets, complete=True)

        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/static,PROD)"]
        mock_in_proc.assert_not_called()
        mock_db.assert_not_called()
