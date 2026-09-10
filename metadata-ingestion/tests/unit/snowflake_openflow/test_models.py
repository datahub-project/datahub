from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pytest

from datahub.ingestion.source.snowflake.snowflake_openflow_models import (
    OpenflowConnector,
    OpenflowDeployment,
    OpenflowRuntime,
    _resolve_per_key,
    get_datetime,
    merge_show_and_history,
    timestamp_shape,
)


def test_deployment_from_show_row():
    deployment = OpenflowDeployment.from_row(
        {"name": "MyDeployment", "key": "abc12345", "status": "ACTIVE", "owner": "R"}
    )
    assert deployment is not None
    assert deployment.key == "abc12345"
    assert deployment.name == "MyDeployment"
    assert deployment.owner == "R"


def test_deployment_from_history_row_uses_uppercase_column_names():
    # The ACCOUNT_USAGE views return uppercase keys; SHOW returns lowercase.
    # One dataclass must read both without the caller normalising first.
    deployment = OpenflowDeployment.from_row(
        {"NAME": "MyDeployment", "DEPLOYMENT_KEY": "abc12345", "DELETED_ON": None}
    )
    assert deployment is not None
    assert deployment.key == "abc12345"
    assert deployment.name == "MyDeployment"


def test_absent_optional_columns_do_not_raise():
    # The views are still evolving; a dropped optional column must degrade, not crash.
    deployment = OpenflowDeployment.from_row({"name": "D", "key": "k"})
    assert deployment is not None
    assert deployment.status is None
    assert deployment.created_at is None


def test_row_without_identity_key_returns_none():
    assert OpenflowDeployment.from_row({"status": "ACTIVE"}) is None


def test_runtime_carries_parent_deployment():
    runtime = OpenflowRuntime.from_row(
        {
            "name": "MyRuntime",
            "key": "myruntime-1",
            "deployment": "MyDeployment",
            "database_name": "MY_DB",
            "schema_name": "MY_SCHEMA",
        }
    )
    assert runtime is not None
    assert runtime.key == "myruntime-1"
    assert runtime.deployment_name == "MyDeployment"
    # database_name/schema_name are the runtime OBJECT's own location, never a
    # data destination. Kept for the object's properties only.
    assert runtime.object_database == "MY_DB"


def test_connector_from_row_populates_connector_id_field():
    # CONNECTOR_ID is carried as an ordinary field when the view supplies it,
    # but is no longer the identity — see test_connector_key_* below.
    connector = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "pg_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CONNECTOR_DEFINITION": "OPENFLOW_POSTGRES_CDC",
        }
    )
    assert connector is not None
    assert connector.connector_id == "1"
    assert connector.connector_definition == "OPENFLOW_POSTGRES_CDC"


def test_merge_prefers_show_for_location_and_history_for_ids():
    show = OpenflowRuntime.from_row(
        {
            "name": "R",
            "key": "r-100",
            "deployment": "D",
            "database_name": "MY_DB",
        }
    )
    history = OpenflowRuntime.from_row(
        {
            "NAME": "R",
            "RUNTIME_KEY": "r-100",
            "DATABASE_NAME": None,
            "EXECUTE_AS_ROLE_NAME": "RUNTIME_ROLE",
            "CREATED_ON": "2026-09-03T00:00:00",
        }
    )
    assert show is not None
    assert history is not None
    merged, _ = merge_show_and_history([show], [history])
    assert len(merged) == 1
    # SHOW wins for location: the view returned NULL where SHOW was populated.
    assert merged[0].object_database == "MY_DB"
    # The view wins for what only it carries.
    assert merged[0].execute_as_role == "RUNTIME_ROLE"
    assert merged[0].created_at == datetime(2026, 9, 3, tzinfo=timezone.utc)


def test_merge_treats_show_only_object_as_new_not_deleted():
    # Measured: a runtime visible to SHOW was absent from the view ~20 min after
    # creation. Treating that as a deletion would drop a brand-new object.
    show = OpenflowRuntime.from_row({"name": "R", "key": "r-100", "deployment": "D"})
    assert show is not None
    merged, _ = merge_show_and_history([show], [])
    assert len(merged) == 1
    assert merged[0].key == "r-100"


def test_merge_does_not_mutate_caller_owned_show_row():
    # show_rows is caller-owned; a downstream caller keeps its own reference to
    # the objects it passes in, so the merge must not setattr onto them.
    show = OpenflowRuntime.from_row(
        {
            "name": "R",
            "key": "r-100",
            "deployment": "D",
            "database_name": "MY_DB",
        }
    )
    history = OpenflowRuntime.from_row(
        {"NAME": "R", "RUNTIME_KEY": "r-100", "EXECUTE_AS_ROLE_NAME": "RUNTIME_ROLE"}
    )
    assert show is not None
    assert history is not None
    merge_show_and_history([show], [history])
    # The original object passed in show_rows must be untouched by the merge.
    assert show.execute_as_role is None
    assert show.object_database == "MY_DB"


def test_connector_key_disambiguates_same_name_across_runtimes():
    # SHOW OPENFLOW CONNECTORS is account-wide, so a merge is genuinely called
    # with connectors of the same name under different runtimes; they must not
    # collide into a single identity.
    first = OpenflowConnector.from_row({"name": "pg_cdc", "runtime": "MyRuntime"})
    second = OpenflowConnector.from_row({"name": "pg_cdc", "runtime": "OtherRuntime"})
    assert first is not None
    assert second is not None
    assert first.key != second.key


def test_connector_key_stable_without_connector_id():
    # A newly created connector has no CONNECTOR_ID yet (the view lags ~20
    # minutes behind SHOW), so identity must not depend on it.
    connector = OpenflowConnector.from_row({"name": "pg_cdc", "runtime": "MyRuntime"})
    assert connector is not None
    assert connector.connector_id is None
    assert connector.key == "MyRuntime/pg_cdc"


def test_recreated_object_is_not_reported_as_deleted():
    # These views are append-style lifecycle records and `key` is not
    # per-incarnation: a connector's key is the composite <runtime>/<name>, stable
    # across a drop and re-create under the same name. So one key can own several
    # rows, the older ones carrying DELETED_ON.
    #
    # Without newest-wins resolution the superseded incarnation's DELETED_ON merges
    # onto the live object (the field merge fills any None field, and deleted_on on
    # a live row IS None), the caller's `deleted_on is None` filter then drops an
    # object that exists, and stale-entity removal soft-deletes it in DataHub. No
    # counter reflects that, which is why it needs a test rather than a comment.
    show = OpenflowConnector.from_row({"name": "pg_cdc", "runtime": "MyRuntime"})
    deleted_incarnation = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "pg_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-01-01T00:00:00",
            "DELETED_ON": "2026-02-01T00:00:00",
        }
    )
    live_incarnation = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 2,
            "NAME": "pg_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-03-01T00:00:00",
        }
    )
    assert show is not None and deleted_incarnation is not None
    assert live_incarnation is not None

    # Both iteration orders, because the defect was order-independent.
    for history in (
        [deleted_incarnation, live_incarnation],
        [live_incarnation, deleted_incarnation],
    ):
        merged, _ = merge_show_and_history([show], history)
        assert len(merged) == 1
        assert merged[0].deleted_on is None, (
            "a re-created object must not inherit the DELETED_ON of the "
            "incarnation it replaced"
        )
        # The newest incarnation's surrogate id wins too, not the dead one's.
        assert merged[0].connector_id == "2"


def test_object_deleted_and_not_recreated_is_still_reported_deleted():
    # The mirror case: newest-wins must not make deletion undetectable, or the
    # deletion-detection feature the unfiltered DELETED_ON exists for is lost.
    deleted = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "gone_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-01-01T00:00:00",
            "DELETED_ON": "2026-02-01T00:00:00",
        }
    )
    assert deleted is not None
    merged, _ = merge_show_and_history([], [deleted])
    assert len(merged) == 1
    assert merged[0].deleted_on == "2026-02-01T00:00:00"


def test_show_present_object_is_never_marked_deleted_by_a_history_row():
    # SHOW is authoritative for existence. This is the invariant that makes the
    # whole "live object soft-deleted by a stale lifecycle row" class unreachable,
    # independently of whether CREATED_ON is populated at all -- which is the case
    # the timestamp-ordering approach got wrong, because the view populates
    # CREATED_ON with a lag (the pager has a guard for NULL CREATED_ON precisely
    # because it happens).
    show = OpenflowConnector.from_row({"name": "pg_cdc", "runtime": "MyRuntime"})
    deleted_untimestamped = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "pg_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "DELETED_ON": "2026-02-01T00:00:00",
        }
    )
    assert show is not None and deleted_untimestamped is not None
    merged, _ = merge_show_and_history([show], [deleted_untimestamped])
    assert len(merged) == 1
    assert merged[0].deleted_on is None
    # And nothing else is taken from the dead incarnation either. Excluding only
    # deleted_on would have shipped that row's `status` ("DELETED") on a live
    # entity, and its version_location_uri would have pointed the config read at a
    # superseded version of the connector.
    assert merged[0].connector_id is None


def test_show_present_object_survives_a_created_on_tie():
    # The reviewer reproduced a tie resolving by iteration order. With SHOW
    # authoritative for existence, order cannot matter for liveness.
    show = OpenflowConnector.from_row({"name": "pg_cdc", "runtime": "MyRuntime"})
    same_ts_open = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 2,
            "NAME": "pg_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-03-01T00:00:00",
        }
    )
    same_ts_closed = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "pg_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-03-01T00:00:00",
            "DELETED_ON": "2026-03-02T00:00:00",
        }
    )
    assert show is not None and same_ts_open is not None and same_ts_closed is not None
    for history in ([same_ts_open, same_ts_closed], [same_ts_closed, same_ts_open]):
        merged, _ = merge_show_and_history([show], history)
        assert len(merged) == 1
        assert merged[0].deleted_on is None


def test_view_only_deleted_object_still_reports_deleted_on_a_tie():
    # The mirror risk: SHOW authority must not make deletion undetectable for an
    # object SHOW no longer lists. An open and a closed row of the same key with
    # the SAME timestamp must resolve to open only when SHOW vouches for it; with
    # no SHOW row, a closed-only history must stay closed.
    closed = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "gone_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-03-01T00:00:00",
            "DELETED_ON": "2026-03-02T00:00:00",
        }
    )
    assert closed is not None
    merged, _ = merge_show_and_history([], [closed])
    assert len(merged) == 1
    assert merged[0].deleted_on == "2026-03-02T00:00:00"


def test_view_only_key_reports_the_newest_lifecycle_row():
    # This path IS deletion detection: a deleted object is SHOW-absent by
    # definition, so every deletion resolves here. Newest CREATED_ON wins, and on a
    # tie CLOSED beats OPEN.
    #
    # An earlier revision preferred OPEN outright and a test asserted that,
    # locking in a deterministic miss: under an event-style reading of these views
    # the create row stays open forever, so every deleted object would have been
    # reported live and DELETION_DETECTION -- declared supported by this source --
    # would silently never fire. Preferring open existed to protect live objects,
    # which the SHOW-authority rule above already does.
    #
    # Newest-wins is correct under BOTH readings of the view grain. The grain has
    # since been measured as incarnation-style (probes Round 16), but on one
    # object per view, so this pins the rule that holds either way:
    # incarnation-style, a re-created object carries the later timestamp;
    # event-style, the delete event does.
    open_older = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 2,
            "NAME": "priv_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-04-01T00:00:00",
        }
    )
    closed_newer = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "priv_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-05-01T00:00:00",
            "DELETED_ON": "2026-05-02T00:00:00",
        }
    )
    assert open_older is not None and closed_newer is not None
    for history in ([open_older, closed_newer], [closed_newer, open_older]):
        merged, _ = merge_show_and_history([], history)
        assert len(merged) == 1
        assert merged[0].deleted_on == "2026-05-02T00:00:00"


def test_view_only_recreation_is_reported_live():
    # The mirror: a genuine re-creation carries the LATER timestamp under either
    # reading, so newest-wins keeps it live without needing an open-beats-closed
    # rule at all.
    closed_older = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "priv_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-01-01T00:00:00",
            "DELETED_ON": "2026-02-01T00:00:00",
        }
    )
    open_newer = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 2,
            "NAME": "priv_cdc",
            "RUNTIME_NAME": "MyRuntime",
            "CREATED_ON": "2026-03-01T00:00:00",
        }
    )
    assert closed_older is not None and open_newer is not None
    for history in ([closed_older, open_newer], [open_newer, closed_older]):
        merged, _ = merge_show_and_history([], history)
        assert len(merged) == 1
        assert merged[0].deleted_on is None
        assert merged[0].connector_id == "2"


def test_mixed_lifecycle_keys_are_counted():
    # The resolver is written to hold whichever way these views are grained --
    # one row per object life, or one row per state change. Measured as the
    # former (probes Round 16) on a single object per view, so the counter stays
    # as the thing that would surface a contradiction. This counter is the
    # direction-neutral signal for it -- zero while the assumption holds, non-zero
    # the moment a key owns both an open and a closed row, which is the only
    # condition under which the grain question changes any answer. An operator can
    # read it; the assumption alone cannot be read.
    open_row = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 2,
            "NAME": "c",
            "RUNTIME_NAME": "R",
            "CREATED_ON": "2026-03-01T00:00:00",
        }
    )
    closed_row = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 1,
            "NAME": "c",
            "RUNTIME_NAME": "R",
            "CREATED_ON": "2026-01-01T00:00:00",
            "DELETED_ON": "2026-02-01T00:00:00",
        }
    )
    assert open_row is not None and closed_row is not None

    _, mixed = merge_show_and_history([], [open_row, closed_row])
    assert mixed == 1

    # Two rows of the same liveness are not mixed, whichever way round.
    _, unmixed = merge_show_and_history([], [open_row, open_row])
    assert unmixed == 0

    # Three rows for ONE key is the shape that separates counting keys from
    # counting transitions, and it is the only shape that does: at two rows the
    # two definitions coincide, so a test using two rows passes under both and
    # cannot detect a regression to per-transition counting.
    second_closed = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 3,
            "NAME": "c",
            "RUNTIME_NAME": "R",
            "CREATED_ON": "2026-02-15T00:00:00",
            "DELETED_ON": "2026-02-16T00:00:00",
        }
    )
    assert second_closed is not None
    _, three_rows = merge_show_and_history([], [closed_row, open_row, second_closed])
    assert three_rows == 1, (
        "one key with three rows is ONE ambiguous key; counting liveness "
        "transitions would report 2 here"
    )

    # And two distinct ambiguous keys are two, so the counter is not merely
    # clamped to 1.
    other_open = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 4,
            "NAME": "d",
            "RUNTIME_NAME": "R",
            "CREATED_ON": "2026-03-01T00:00:00",
        }
    )
    other_closed = OpenflowConnector.from_row(
        {
            "CONNECTOR_ID": 5,
            "NAME": "d",
            "RUNTIME_NAME": "R",
            "CREATED_ON": "2026-01-01T00:00:00",
            "DELETED_ON": "2026-02-01T00:00:00",
        }
    )
    assert other_open is not None and other_closed is not None
    _, two_keys = merge_show_and_history(
        [], [open_row, closed_row, other_open, other_closed]
    )
    assert two_keys == 2


def test_fqn_quotes_every_part_and_doubles_embedded_quotes():
    # Openflow names are case-sensitive and Snowsight allows characters an
    # unquoted identifier cannot carry. A name containing a double quote would
    # otherwise terminate the identifier early and change which object DESCRIBE
    # addresses.
    connector = OpenflowConnector(
        name='we"ird',
        runtime_name="rt",
        database_name="My_DB",
        schema_name="My_Schema",
    )
    assert connector.fqn == '"My_DB"."My_Schema"."we""ird"'


def test_fqn_is_none_when_show_did_not_supply_the_parts():
    # History rows carry no DATABASE_NAME / SCHEMA_NAME, so a connector known
    # only from the view cannot be addressed by DESCRIBE at all.
    assert OpenflowConnector(name="c", runtime_name="rt").fqn is None


def test_a_deletion_across_a_dst_boundary_is_not_lost() -> None:
    # The concrete failure of ordering by rendering. Around a fall-back the
    # same hour is emitted in two offsets, so wall-clock order and real order
    # come apart: 02:00-07:00 is 09:00Z and 01:30-08:00 is 09:30Z, so the
    # SECOND is later -- while as strings "...02:00:00-07:00" sorts after
    # "...01:30:00-08:00" and reverses them. The resolver would then keep the
    # older OPEN row and the deletion would vanish, with nothing malformed to
    # notice.
    created = datetime(2026, 11, 1, 2, 0, tzinfo=timezone(timedelta(hours=-7)))
    deleted = datetime(2026, 11, 1, 1, 30, tzinfo=timezone(timedelta(hours=-8)))
    assert str(created) > str(deleted), "the string ordering must be the wrong way"
    assert created < deleted, "the instants must order the other way"

    rows = [
        OpenflowConnector.from_row(
            {"NAME": "c", "RUNTIME_NAME": "rt", "CREATED_ON": created}
        ),
        OpenflowConnector.from_row(
            {
                "NAME": "c",
                "RUNTIME_NAME": "rt",
                "CREATED_ON": deleted,
                "DELETED_ON": deleted,
            }
        ),
    ]
    resolved, _ = _resolve_per_key([row for row in rows if row])

    assert len(resolved) == 1
    assert resolved[0].deleted_on is not None, (
        "the later row is the deletion; ordering by rendering loses it"
    )


def test_an_unparseable_created_on_never_displaces_a_usable_one() -> None:
    # get_datetime returns None rather than raising, so such a row sorts at the
    # epoch -- present, but never winning against a row that has a real time.
    rows = [
        OpenflowConnector.from_row(
            {"NAME": "c", "RUNTIME_NAME": "rt", "CREATED_ON": "not a timestamp"}
        ),
        OpenflowConnector.from_row(
            {"NAME": "c", "RUNTIME_NAME": "rt", "CREATED_ON": "2024-01-01 00:00:00"}
        ),
    ]
    resolved, _ = _resolve_per_key([row for row in rows if row])

    assert len(resolved) == 1
    # Asserted on the PARSED value: the raw string is no longer carried, and it
    # was the parsed one that decided this ordering anyway.
    assert resolved[0].created_at == datetime(2024, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("rendering", "expected_utc_hour"),
    [
        # Snowflake's DEFAULT TIMESTAMP_OUTPUT_FORMAT: a space before the offset
        # and no colon inside it. An earlier revision only swapped the date/time
        # separator, so the platform's own default did not parse -- see below
        # for why that was not merely cosmetic.
        pytest.param("2024-01-01 12:00:00.000 -0800", 20, id="snowflake default"),
        pytest.param("2024-01-01 12:00:00.000000-08:00", 20, id="offset with colon"),
        pytest.param("2024-01-01T12:00:00Z", 12, id="iso with Z"),
        pytest.param("2024-01-01 12:00:00", 12, id="naive, read as UTC"),
    ],
)
def test_snowflake_timestamp_renderings_all_parse(
    rendering: str, expected_utc_hour: int
) -> None:
    parsed = get_datetime({"CREATED_ON": rendering}, "CREATED_ON")
    assert parsed is not None, f"{rendering!r} must parse"
    assert parsed.astimezone(timezone.utc).hour == expected_utc_hour


def test_an_unparsed_timestamp_cannot_fabricate_a_deletion() -> None:
    # Why the parser mattering is not cosmetic. Unparsed rows all sort at the
    # same sentinel, so they TIE -- and a tie resolves CLOSED over OPEN. If the
    # account's rendering stopped parsing, every key holding any closed row
    # would resolve to that row and be filtered out as deleted. This pins the
    # live-wins outcome for the rendering Snowflake actually emits.
    live = {
        "NAME": "c",
        "RUNTIME_NAME": "rt",
        "CREATED_ON": "2024-06-01 12:00:00.000 -0800",
    }
    closed = {
        "NAME": "c",
        "RUNTIME_NAME": "rt",
        "CREATED_ON": "2024-01-01 12:00:00.000 -0800",
        "DELETED_ON": "2024-01-02 12:00:00.000 -0800",
    }
    rows = [OpenflowConnector.from_row(live), OpenflowConnector.from_row(closed)]
    resolved, _ = _resolve_per_key([row for row in rows if row])

    assert len(resolved) == 1
    assert resolved[0].deleted_on is None, (
        "the later row is live; only an unparsed timestamp would tie and let "
        "the older closed row win"
    )


# The exact column sets the three ACCOUNT_USAGE views return, captured from a
# live account. The point of writing them out is that the two surfaces disagree:
# SHOW calls an object's own fields `name` and `status`, while every view
# prefixes them with the object type and has NO bare NAME or STATUS column at
# all. Fixtures written from the SHOW spelling parse fine against code that
# makes the same assumption, so nothing in the suite disagreed with reality
# until these rows were taken from the system itself.
_DEPLOYMENT_HISTORY_ROW = {
    "DEPLOYMENT_ID": "1",
    "DEPLOYMENT_NAME": "prod_deployment",
    "DEPLOYMENT_KEY": "prod-deployment-1",
    "DEPLOYMENT_STATUS": "ACTIVE",
    "DISPLAY_NAME": "Prod Deployment",
    "OWNER": "ACCOUNTADMIN",
    "CREATED_ON": "2024-01-01 00:00:00.000 -0800",
    "DELETED_ON": None,
}
_RUNTIME_HISTORY_ROW = {
    "RUNTIME_ID": "1",
    "RUNTIME_NAME": "prod_runtime",
    "RUNTIME_KEY": "prod-runtime-1",
    "RUNTIME_STATUS": "ACTIVE",
    "DEPLOYMENT_NAME": "prod_deployment",
    "EXECUTE_AS_ROLE_NAME": "OPENFLOW_ROLE",
    "DATABASE_NAME": "MY_DB",
    "SCHEMA_NAME": "MY_SCHEMA",
    "OWNER": "ACCOUNTADMIN",
    "CREATED_ON": "2024-01-01 00:00:00.000 -0800",
    "DELETED_ON": None,
}
_CONNECTOR_HISTORY_ROW = {
    "CONNECTOR_ID": "1",
    "CONNECTOR_NAME": "pg_cdc",
    "RUNTIME_NAME": "prod_runtime",
    "CONNECTOR_DEFINITION": "OPENFLOW_POSTGRES_CDC",
    "DATABASE_NAME": "MY_DB",
    "SCHEMA_NAME": "MY_SCHEMA",
    "OWNER": "ACCOUNTADMIN",
    "CREATED_ON": "2024-01-01 00:00:00.000 -0800",
    "DELETED_ON": None,
}


def test_a_real_deployment_history_row_keeps_its_name_and_status() -> None:
    parsed = OpenflowDeployment.from_row(_DEPLOYMENT_HISTORY_ROW)
    assert parsed is not None
    assert parsed.key == "prod-deployment-1"
    assert parsed.name == "prod_deployment"
    assert parsed.status == "ACTIVE"


def test_a_real_runtime_history_row_keeps_its_name_and_status() -> None:
    parsed = OpenflowRuntime.from_row(_RUNTIME_HISTORY_ROW)
    assert parsed is not None
    assert parsed.key == "prod-runtime-1"
    assert parsed.name == "prod_runtime"
    assert parsed.status == "ACTIVE"
    # The cross-reference already worked; asserted here so the row is pinned whole.
    assert parsed.deployment_name == "prod_deployment"


def test_a_real_connector_history_row_parses_at_all() -> None:
    # The regression this guards is total, not partial: the connector's name is
    # half its identity, so looking for a bare NAME column meant from_row
    # returned None for EVERY history row and the view contributed nothing --
    # no surrogate ids, no timestamps, no DELETED_ON.
    parsed = OpenflowConnector.from_row(_CONNECTOR_HISTORY_ROW)
    assert parsed is not None
    assert parsed.name == "pg_cdc"
    assert parsed.runtime_name == "prod_runtime"
    assert parsed.connector_id == "1"


def test_no_history_view_row_relies_on_a_bare_name_column() -> None:
    # Pins the asymmetry itself, so a future fixture written in the SHOW
    # spelling cannot quietly reintroduce the assumption.
    for row in (
        _DEPLOYMENT_HISTORY_ROW,
        _RUNTIME_HISTORY_ROW,
        _CONNECTOR_HISTORY_ROW,
    ):
        assert "NAME" not in row
        assert "STATUS" not in row
        assert "KEY" not in row


def test_timestamp_shape_masks_digits_so_the_report_names_a_format_not_a_value() -> (
    None
):
    # The report has to say WHICH rendering it could not read without echoing
    # the row. The digits are the value; the punctuation is the format.
    assert timestamp_shape("01/02/2024 15:04:05") == "NN/NN/NNNN NN:NN:NN"
    assert timestamp_shape("  2024-01-01T00:00:00Z  ") == "NNNN-NN-NNTNN:NN:NNZ"
    # Bounded, so a pathological value cannot flood the report.
    assert len(timestamp_shape("9" * 500)) == 40


@pytest.mark.parametrize(
    "row",
    [
        pytest.param({"RUNTIME_NAME": "rt"}, id="no name"),
        pytest.param({"CONNECTOR_NAME": "c"}, id="no runtime"),
        pytest.param({}, id="neither"),
    ],
)
def test_a_connector_row_missing_half_its_identity_is_not_usable(
    row: Dict[str, Any],
) -> None:
    # A connector is keyed on the COMPOSITE runtime/name, so a row carrying only
    # one half cannot be given a degraded key -- it is not a connector. The
    # caller counts these; see _parse_rows.
    assert OpenflowConnector.from_row(row) is None


def test_a_blank_deleted_on_leaves_the_object_live() -> None:
    # get_col treats "" as absent, and this is the branch that matters:
    # deleted_on is the deletion predicate, tested with `is None`, so a blank
    # DELETED_ON would mark a live object deleted and stateful ingestion would
    # soft-delete it.
    row = {"RUNTIME_KEY": "rt-1", "RUNTIME_NAME": "one", "DELETED_ON": ""}

    parsed = OpenflowRuntime.from_row(row)

    assert parsed is not None
    assert parsed.deleted_on is None


def test_a_blank_first_spelling_falls_through_to_the_next() -> None:
    # The other half of the same branch. A row carrying both spellings with the
    # first one blank must resolve to the second, not short-circuit on the
    # blank -- otherwise a view that starts emitting "" for a column the source
    # also knows by another name loses the value entirely.
    row = {"NAME": "", "RUNTIME_NAME": "the_real_name", "RUNTIME_KEY": "rt-1"}

    parsed = OpenflowRuntime.from_row(row)

    assert parsed is not None
    assert parsed.name == "the_real_name"
