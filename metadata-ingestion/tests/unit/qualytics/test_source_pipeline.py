"""End-to-end tests of the ingestion walk against a mocked Qualytics deployment.

The mappers are tested in isolation elsewhere. What matters here is the wiring: that
a recipe produces the aspects a user expects, on the URNs their warehouse source
already emitted, and that one bad object does not take the run down with it.
"""

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from requests_mock import Mocker

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.qualytics.client import QualyticsAuthError
from datahub.ingestion.source.qualytics.source import QualyticsSource

BASE = "https://acme.qualytics.io/api"
ORDERS_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.public.orders,PROD)"

DATASTORE = {
    "id": 1,
    "name": "warehouse",
    "store_type": "jdbc",
    "type": "snowflake",
    "jdbc_url": "jdbc:snowflake://acme/",
    "database": "SALES",
    "schema": "PUBLIC",
}
DATASTORE_STUB = {
    "id": 1,
    "name": "warehouse",
    "store_type": "jdbc",
    "type": "snowflake",
}
CONTAINER = {
    "id": 10,
    "name": "ORDERS",
    "container_type": "table",
    "table_type": "table",
    "status": "Available",
    "datastore": DATASTORE_STUB,
}
CHECK = {
    "id": 100,
    "rule_type": "notNull",
    "description": "amount is always present",
    "fields": [{"id": 5, "name": "amount"}],
    "is_passing": False,
    "last_asserted": "2026-09-08T10:00:00Z",
    "active_anomaly_count": 1,
}
ANOMALY = {
    "id": 900,
    "uuid": "abc-123",
    "type": "record",
    "status": "Active",
    "created": "2026-09-08T10:00:00Z",
    "anomalous_records_count": 17,
    "datastore": DATASTORE_STUB,
    "container": CONTAINER,
    "failed_checks": [{"quality_check": CHECK, "message": "17 rows had a null amount"}],
}
CONTAINER_PROFILE = {
    "id": 50,
    "created": "2026-09-08T09:00:00Z",
    "operation_id": 3,
    "records_count": 1000,
    "records_processed": 1000,
    "result": "success",
}
FIELD_PROFILE = {
    "id": 60,
    "name": "amount",
    "created": "2026-09-08T09:00:00Z",
    "field_type": "Fractional",
    "completeness": 0.983,
    "min": 1.0,
    "max": 999.0,
}


def _page(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"items": items, "page": 1, "pages": 1, "size": 100, "total": len(items)}


def _mock_deployment(
    m: Mocker,
    *,
    datastores: list[dict[str, Any]] | None = None,
    containers: list[dict[str, Any]] | None = None,
    checks: list[dict[str, Any]] | None = None,
    anomalies: list[dict[str, Any]] | None = None,
    profile: dict[str, Any] | None = None,
    field_profiles: list[dict[str, Any]] | None = None,
) -> None:
    m.get(
        f"{BASE}/openapi.json", json={"info": {"version": "20260909-test"}, "paths": {}}
    )
    m.get(
        f"{BASE}/datastores",
        json=_page(datastores if datastores is not None else [DATASTORE]),
    )
    m.get(
        f"{BASE}/containers",
        json=_page(containers if containers is not None else [CONTAINER]),
    )
    m.get(
        f"{BASE}/quality-checks", json=_page(checks if checks is not None else [CHECK])
    )
    m.get(
        f"{BASE}/anomalies",
        json=_page(anomalies if anomalies is not None else [ANOMALY]),
    )
    m.get(
        f"{BASE}/containers/10/profile",
        json=profile if profile is not None else CONTAINER_PROFILE,
    )
    m.get(
        f"{BASE}/containers/10/field-profiles",
        json=_page(field_profiles if field_profiles is not None else [FIELD_PROFILE]),
    )


def _run(
    graph: DataHubGraph | None = None, **config_overrides: Any
) -> tuple[list[Any], QualyticsSource]:
    config = {
        "base_url": BASE,
        "token": "t",
        "platform_instance": "acme",
        **config_overrides,
    }
    source = QualyticsSource.create(config, PipelineContext(run_id="test", graph=graph))
    workunits = list(source.get_workunits_internal())
    return workunits, source


def _aspects(workunits: list[Any]) -> list[str]:
    return [type(wu.metadata.aspect).__name__ for wu in workunits]


# --- the happy path ----------------------------------------------------------------


def test_a_full_run_emits_a_profile_an_assertion_and_its_results() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        workunits, source = _run()

    names = _aspects(workunits)
    assert "DatasetProfileClass" in names
    assert "AssertionInfoClass" in names
    # Two run events: the check's current verdict, and the anomaly that caused it.
    assert names.count("AssertionRunEventClass") == 2

    report = source.get_report()
    assert report.datastores_scanned == 1
    assert report.containers_scanned == 1
    assert report.quality_checks_scanned == 1
    assert report.anomalies_scanned == 1
    assert report.urns_resolved == 1


def test_everything_attaches_to_the_warehouse_sources_dataset_urn() -> None:
    # The entire premise of the connector. If this regresses, the metadata lands on
    # datasets nobody is looking at.
    with Mocker() as m:
        _mock_deployment(m)
        workunits, _ = _run()

    profile = next(
        wu
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "DatasetProfileClass"
    )
    # Note the absence of "acme": platform_instance names the Qualytics deployment,
    # not the warehouse, so it must not appear in the source dataset URN.
    assert profile.metadata.entityUrn == ORDERS_URN

    assertion = next(
        wu
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "AssertionInfoClass"
    )
    # The assertion is its own entity, but it must point back at their dataset.
    assert assertion.metadata.aspect.customAssertion.entity == ORDERS_URN

    run_event = next(
        wu
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "AssertionRunEventClass"
    )
    assert run_event.metadata.aspect.asserteeUrn == ORDERS_URN


def test_the_anomaly_result_carries_qualytics_message_and_record_count() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        workunits, _ = _run()

    events = [
        wu.metadata.aspect
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "AssertionRunEventClass"
    ]
    from_anomaly = next(
        e for e in events if "qualytics_anomaly_id" in e.result.nativeResults
    )

    assert from_anomaly.result.unexpectedCount == 17
    assert from_anomaly.result.nativeResults["message"] == "17 rows had a null amount"


def test_assertions_link_back_to_the_container_in_qualytics() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        workunits, _ = _run()

    assertion = next(
        wu
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "AssertionInfoClass"
    )
    assert (
        assertion.metadata.aspect.externalUrl
        == "https://acme.qualytics.io/datastores/1/containers/10/overview"
    )


def test_the_qualytics_build_version_is_recorded_on_the_report() -> None:
    # Every deployment runs its own build, so this is the first thing worth knowing
    # when a mapping misbehaves.
    with Mocker() as m:
        _mock_deployment(m)
        _, source = _run()

    assert source.get_report().qualytics_version == "20260909-test"


# --- emission toggles --------------------------------------------------------------


def test_disabling_profiles_skips_the_profile_calls_entirely() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        workunits, _ = _run(emit_profiles=False)

    assert "DatasetProfileClass" not in _aspects(workunits)
    assert not any("/profile" in r.path for r in m.request_history)


def test_disabling_assertions_also_suppresses_their_results() -> None:
    # Results without assertions would be orphaned events pointing at URNs that no
    # assertionInfo describes.
    with Mocker() as m:
        _mock_deployment(m)
        workunits, _ = _run(emit_assertions=False)

    names = _aspects(workunits)
    assert "AssertionInfoClass" not in names
    assert "AssertionRunEventClass" not in names


def test_disabling_results_keeps_the_assertions() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        workunits, _ = _run(emit_assertion_results=False)

    names = _aspects(workunits)
    assert "AssertionInfoClass" in names
    assert "AssertionRunEventClass" not in names


def test_the_anomaly_window_is_sent_to_the_api_not_filtered_locally() -> None:
    # Fetching all history and discarding it is how a large tenant's run becomes an
    # hour long.
    with Mocker() as m:
        _mock_deployment(m)
        _run(assertion_results={"start_time": "2026-08-01T00:00:00Z"})

    anomaly_request = next(
        r for r in m.request_history if r.path.endswith("/anomalies")
    )
    assert anomaly_request.qs["start_date"] == ["2026-08-01"]


# --- filtering ---------------------------------------------------------------------


def test_a_denied_datastore_is_skipped_before_any_container_call() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        workunits, source = _run(datastore_pattern={"deny": ["^warehouse$"]})

    assert workunits == []
    assert source.get_report().datastores_dropped == 1
    assert not any(r.path.endswith("/containers") for r in m.request_history)


def test_a_denied_container_is_counted_and_produces_nothing() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        workunits, source = _run(container_pattern={"deny": ["^ORDERS$"]})

    assert workunits == []
    assert source.get_report().containers_dropped == 1


# --- resilience --------------------------------------------------------------------


def test_an_unresolvable_datastore_is_skipped_without_touching_its_containers() -> None:
    with Mocker() as m:
        _mock_deployment(m, datastores=[{**DATASTORE, "type": "fabric"}])
        workunits, source = _run()

    assert workunits == []
    assert source.get_report().datastores_unresolved == 1


def test_one_failing_container_does_not_abort_the_others() -> None:
    # A per-item failure must cost that item, not the whole run.
    second = {**CONTAINER, "id": 11, "name": "CUSTOMERS"}
    with Mocker() as m:
        _mock_deployment(m, containers=[CONTAINER, second])
        m.get(f"{BASE}/containers/11/profile", status_code=500, text="boom")
        workunits, source = _run()

    # The healthy container still produced its metadata.
    assert "DatasetProfileClass" in _aspects(workunits)
    assert any("CUSTOMERS" in str(w) for w in source.get_report().warnings)


def test_an_unrecognised_container_type_is_skipped_with_a_warning() -> None:
    with Mocker() as m:
        _mock_deployment(m, containers=[{**CONTAINER, "container_type": "hologram"}])
        workunits, source = _run()

    assert workunits == []
    assert any("container type" in str(w).lower() for w in source.get_report().warnings)


def test_a_container_that_was_never_profiled_is_not_an_error() -> None:
    # Normal for a newly catalogued container; it must not warn or fail. Qualytics
    # says so with a 404 -- this test used to mock an empty body, which no deployment
    # returns, so the real case warned on every run while the test passed.
    with Mocker() as m:
        _mock_deployment(m)
        m.get(
            f"{BASE}/containers/10/profile",
            status_code=404,
            json={"detail": "Container id: 10 has not been profiled"},
        )
        workunits, source = _run()

    report = source.get_report()
    assert "DatasetProfileClass" not in _aspects(workunits)
    assert "AssertionInfoClass" in _aspects(workunits)
    assert report.containers_unprofiled == 1
    assert report.profiles_failed == 0
    assert report.warnings == []
    assert report.failures == []


def test_an_auth_failure_aborts_rather_than_warning_per_container() -> None:
    # Every subsequent request would fail identically; a warning storm would bury
    # the cause.
    with Mocker() as m:
        _mock_deployment(m)
        m.get(f"{BASE}/quality-checks", status_code=401, json={"detail": "nope"})

        with pytest.raises(QualyticsAuthError):
            _run()


def test_an_unrecognised_datastore_type_skips_its_containers_with_a_warning() -> None:
    # Symmetric with the container case above, and the more consequential of the two:
    # this skips an entire datastore's worth of metadata.
    with Mocker() as m:
        _mock_deployment(m, datastores=[{**DATASTORE, "store_type": "quantum"}])
        workunits, source = _run()

    assert workunits == []
    assert not any(r.path.endswith("/containers") for r in m.request_history)
    assert source.get_report().datastores_unrecognised == 1


def test_a_malformed_record_skips_that_item_rather_than_the_run() -> None:
    # The per-item rule the module docstring promises. A container missing a required
    # field used to raise ValidationError straight out of the source and kill the
    # pipeline, losing every other container's metadata.
    good = CONTAINER
    bad = {"id": 99, "container_type": "table"}  # no name, status or datastore
    with Mocker() as m:
        _mock_deployment(m, containers=[bad, good])
        workunits, source = _run()

    assert "AssertionInfoClass" in _aspects(workunits)
    assert source.get_report().items_unparseable == 1


def test_a_malformed_anomaly_costs_only_itself() -> None:
    # The live failure behind test_model_spec_alignment: anomalies were parsed bare, so
    # one bad record raised out of the container and took every other anomaly -- and
    # every later result -- with it.
    malformed = {k: v for k, v in ANOMALY.items() if k != "uuid"}
    second = {**ANOMALY, "id": 901, "uuid": "def-456"}
    with Mocker() as m:
        _mock_deployment(m, anomalies=[malformed, second])
        workunits, source = _run()

    report = source.get_report()
    anomaly_ids = [
        wu.metadata.aspect.result.nativeResults.get("qualytics_anomaly_id")
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "AssertionRunEventClass"
    ]
    assert "901" in anomaly_ids
    assert report.items_unparseable == 1
    assert report.containers_failed == 0


def test_every_assertion_carries_the_qualytics_platform_and_deployment() -> None:
    # dataPlatformInstance is expected on every entity, and on an assertion it is the
    # only visible trace of which Qualytics deployment it came from.
    with Mocker() as m:
        _mock_deployment(m)
        workunits, _ = _run()

    assertion_urns = {
        wu.metadata.entityUrn
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "AssertionInfoClass"
    }
    instances = {
        wu.metadata.entityUrn: wu.metadata.aspect
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "DataPlatformInstanceClass"
    }
    assert set(instances) == assertion_urns
    aspect = next(iter(instances.values()))
    assert aspect.platform == "urn:li:dataPlatform:qualytics"
    assert (
        aspect.instance
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:qualytics,acme)"
    )


def test_a_profile_failure_does_not_cost_the_container_its_assertions() -> None:
    # A profile that will not load says nothing about the container's checks.
    with Mocker() as m:
        _mock_deployment(m)
        m.get(f"{BASE}/containers/10/profile", status_code=500, text="boom")
        workunits, source = _run()

    names = _aspects(workunits)
    assert "DatasetProfileClass" not in names
    assert "AssertionInfoClass" in names
    assert source.get_report().profiles_failed == 1


def _graph(exists: bool | Exception) -> MagicMock:
    graph = MagicMock(spec=DataHubGraph)
    if isinstance(exists, Exception):
        graph.exists.side_effect = exists
    else:
        graph.exists.return_value = exists
    return graph


def test_no_profile_is_written_to_a_dataset_datahub_does_not_have() -> None:
    # Writing it would create the dataset: a stub holding nothing but
    # our profile. The assertions still go out -- they reference the dataset without
    # creating it.
    with Mocker() as m:
        _mock_deployment(m)
        workunits, source = _run(graph=_graph(False))

    names = _aspects(workunits)
    assert "DatasetProfileClass" not in names
    assert "AssertionInfoClass" in names
    assert source.get_report().profiles_skipped_dataset_missing == 1
    assert list(source.get_report().datasets_missing) == [ORDERS_URN]
    assert not any("/profile" in r.path for r in m.request_history)


def test_a_dataset_datahub_has_gets_its_profile() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        workunits, source = _run(graph=_graph(True))

    assert "DatasetProfileClass" in _aspects(workunits)
    assert source.get_report().profiles_skipped_dataset_missing == 0


def test_a_failed_existence_check_withholds_the_profile_and_says_so() -> None:
    # Fails closed: a profile skipped once returns next run; a stub dataset stays.
    with Mocker() as m:
        _mock_deployment(m)
        workunits, source = _run(graph=_graph(ConnectionError("gms down")))

    assert "DatasetProfileClass" not in _aspects(workunits)
    assert "AssertionInfoClass" in _aspects(workunits)
    report = source.get_report()
    assert any("exist in DataHub" in str(w) for w in report.warnings)
    assert report.profiles_existence_check_failed == 1
    assert report.profiles_skipped_dataset_missing == 0


def test_profile_workunits_are_not_primary_source() -> None:
    # Load-bearing. Primary-source URNs enter the stale-entity checkpoint, and this
    # URN is the customer's warehouse dataset -- so a container dropping out of scope
    # would soft-delete a dataset we do not own. Assertions are ours and stay primary.
    with Mocker() as m:
        _mock_deployment(m)
        workunits, _ = _run()

    profile = next(
        wu
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "DatasetProfileClass"
    )
    assertion = next(
        wu
        for wu in workunits
        if type(wu.metadata.aspect).__name__ == "AssertionInfoClass"
    )

    assert profile.is_primary_source is False
    assert assertion.is_primary_source is True


# --- what is a failure, and what is only a warning ----------------------------------
#
# The line is stale-entity removal: DataHub's handler stands down only when the source
# reports a failure. Anything that leaves the run's assertions incomplete must be one;
# test_stateful.py shows the deletions this prevents.


def test_a_failed_anomaly_listing_keeps_the_assertions_and_only_warns() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        m.get(f"{BASE}/anomalies", status_code=500, text="boom")
        workunits, source = _run()

    report = source.get_report()
    names = _aspects(workunits)
    assert "AssertionInfoClass" in names
    # The check's current verdict still goes out; only the anomaly history is lost.
    assert names.count("AssertionRunEventClass") == 1
    assert report.anomaly_listings_failed == 1
    assert report.containers_failed == 0
    assert report.failures == []


def test_an_unexpected_error_in_one_container_is_a_failure_not_an_abort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second = {**CONTAINER, "id": 11, "name": "CUSTOMERS"}
    with Mocker() as m:
        _mock_deployment(m, containers=[CONTAINER, second])
        source = QualyticsSource.create(
            {"base_url": BASE, "token": "t", "emit_profiles": False},
            PipelineContext(run_id="test"),
        )
        real = source.assertions.workunits
        calls: list[int] = []

        def explode_once(*args: Any, **kwargs: Any) -> Any:
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("an error nobody anticipated")
            return real(*args, **kwargs)

        monkeypatch.setattr(source.assertions, "workunits", explode_once)
        workunits = list(source.get_workunits_internal())

    report = source.get_report()
    assert report.containers_failed == 1
    assert len(report.failures) == 1
    assert "AssertionInfoClass" in _aspects(workunits)  # CUSTOMERS still made it


def test_a_failed_container_listing_is_a_failure() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        m.get(f"{BASE}/containers", status_code=500, text="boom")
        _, source = _run()

    assert any("containers" in str(f) for f in source.get_report().failures)


def test_an_unparseable_datastore_is_a_failure() -> None:
    with Mocker() as m:
        _mock_deployment(m, datastores=[{"id": 2, "store_type": "jdbc"}, DATASTORE])
        workunits, source = _run()

    report = source.get_report()
    assert report.items_unparseable == 1
    assert len(report.failures) == 1
    assert "AssertionInfoClass" in _aspects(workunits)


def test_a_rejected_token_during_a_profile_fetch_ends_the_run() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        m.get(f"{BASE}/containers/10/profile", status_code=401, json={"detail": "no"})

        with pytest.raises(QualyticsAuthError):
            _run()


@pytest.mark.parametrize(
    "response",
    [
        # A route this deployment does not have: FastAPI's own 404, not "not profiled".
        {"status_code": 404, "json": {"detail": "Not Found"}},
        # A shape we cannot read.
        {"json": [{"records_count": 1}]},
        # An empty object: only the 404 means "never profiled".
        {"json": {}},
    ],
)
def test_a_profile_response_that_is_not_never_profiled_is_a_failed_profile(
    response: dict[str, Any],
) -> None:
    # Treating these as "never profiled" turned a missing endpoint into a tenant where
    # nothing had ever been profiled -- a clean, empty, wrong run.
    with Mocker() as m:
        _mock_deployment(m)
        m.get(f"{BASE}/containers/10/profile", **response)
        _, source = _run()

    report = source.get_report()
    assert report.profiles_failed == 1
    assert report.containers_unprofiled == 0


# --- configuration mistakes that would otherwise be silent ---------------------------


def test_a_map_key_that_matches_no_datastore_is_reported() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        _, source = _run(
            datastore_to_platform_map={"warehose": {"platform": "snowflake"}}
        )

    assert any("warehose" in str(w) for w in source.get_report().warnings)


def test_a_map_key_matching_a_filtered_out_datastore_is_not_reported() -> None:
    with Mocker() as m:
        _mock_deployment(m)
        _, source = _run(
            datastore_pattern={"deny": ["^warehouse$"]},
            datastore_to_platform_map={"warehouse": {"platform": "snowflake"}},
        )

    assert source.get_report().warnings == []


def test_a_denied_datastore_of_an_unknown_type_is_not_warned_about() -> None:
    # The user excluded it; warning that it is unrecognised is noise about something
    # they already said they do not want.
    with Mocker() as m:
        _mock_deployment(m, datastores=[{**DATASTORE, "store_type": "quantum"}])
        _, source = _run(datastore_pattern={"deny": ["^warehouse$"]})

    report = source.get_report()
    assert report.warnings == []
    assert report.datastores_unrecognised == 0


def test_an_unknown_rule_type_is_reported_once_however_many_checks_use_it() -> None:
    checks = [{**CHECK, "id": i, "rule_type": "inventedNextQuarter"} for i in range(5)]
    with Mocker() as m:
        _mock_deployment(m, checks=checks)
        workunits, source = _run()

    report = source.get_report()
    assert _aspects(workunits).count("AssertionInfoClass") == 5
    assert list(report.unmapped_rule_types) == ["inventedNextQuarter"]
    # One warning entry with one context: DataHub merges same-titled warnings into a
    # single entry, so the entry count alone would not notice a warning per check.
    [warning] = [
        w for w in report.warnings if w.title == "Unrecognised Qualytics rule type"
    ]
    assert len(warning.context) == 1


def test_a_per_datastore_casing_override_survives_the_full_processor_chain(
    tmp_path: Path,
) -> None:
    # The framework's lowercasing processor switches on whenever the *recipe* sets
    # convert_urns_to_lowercase -- it reads the raw pipeline config, so only a real
    # Pipeline exercises it -- and lowercased every URN after the resolver had
    # honoured a map entry that turned it off.
    pg = {**DATASTORE, "type": "postgresql", "database": "Sales", "schema": "Public"}
    out = tmp_path / "out.json"
    with Mocker() as m:
        _mock_deployment(m, datastores=[pg])
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "qualytics",
                    "config": {
                        "base_url": BASE,
                        "token": "t",
                        "convert_urns_to_lowercase": True,
                        "datastore_to_platform_map": {
                            "warehouse": {
                                "platform": "postgres",
                                "convert_urns_to_lowercase": False,
                            }
                        },
                    },
                },
                "sink": {"type": "file", "config": {"filename": str(out)}},
            }
        )
        pipeline.run()

    entities = {
        r["aspect"]["json"]["customAssertion"]["entity"]
        for r in json.loads(out.read_text())
        if r.get("aspectName") == "assertionInfo"
    }
    assert entities == {
        "urn:li:dataset:(urn:li:dataPlatform:postgres,Sales.Public.ORDERS,PROD)"
    }
