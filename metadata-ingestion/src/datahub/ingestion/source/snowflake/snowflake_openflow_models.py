import dataclasses
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, TypeVar

COL_NAME = "NAME"
COL_DEPLOYMENT_KEY = "DEPLOYMENT_KEY"
COL_RUNTIME_KEY = "RUNTIME_KEY"
COL_CONNECTOR_ID = "CONNECTOR_ID"
COL_KEY = "KEY"
COL_STATUS = "STATUS"
COL_OWNER = "OWNER"
COL_CREATED_ON = "CREATED_ON"
COL_DELETED_ON = "DELETED_ON"
COL_DEPLOYMENT = "DEPLOYMENT"
COL_DEPLOYMENT_NAME = "DEPLOYMENT_NAME"
COL_RUNTIME = "RUNTIME"
COL_RUNTIME_NAME = "RUNTIME_NAME"
COL_DATABASE_NAME = "DATABASE_NAME"
COL_SCHEMA_NAME = "SCHEMA_NAME"
COL_EXECUTE_AS_ROLE = "EXECUTE_AS_ROLE"
COL_EXECUTE_AS_ROLE_NAME = "EXECUTE_AS_ROLE_NAME"
COL_CONNECTOR_DEFINITION = "CONNECTOR_DEFINITION"
COL_DEFAULT_VERSION = "DEFAULT_VERSION"
COL_DEFAULT_VERSION_LOCATION_URI = "DEFAULT_VERSION_LOCATION_URI"
COL_DISPLAY_NAME = "DISPLAY_NAME"
COL_CONNECTOR_URL = "CONNECTOR_URL"


def get_col(row: Dict[str, Any], *names: str) -> Optional[Any]:
    # SHOW returns lowercase keys, the ACCOUNT_USAGE views uppercase, and the
    # views are still evolving, so read case-insensitively by name and tolerate
    # absence. Never read positionally.
    folded = {str(key).upper(): value for key, value in row.items()}
    for name in names:
        value = folded.get(name.upper())
        if value is not None:
            return value
    return None


def get_datetime(row: Dict[str, Any], *names: str) -> Optional[datetime]:
    """The column as an aware datetime, however the driver rendered it.

    Snowflake returns TIMESTAMP_LTZ as a datetime; other paths and fixtures
    carry strings. Ordering these AS STRINGS is wrong the moment two rows
    render in different UTC offsets: across a DST fall-back the later event can
    sort first, and _resolve_per_key would then keep the older OPEN row and
    lose the deletion entirely -- silently, since nothing about the result is
    malformed.

    Naive values are read as UTC. That is a choice, not a fact: it keeps them
    mutually comparable rather than raising, and the alternative (guessing the
    session timezone) would be a fabrication.
    """
    value = get_col(row, *names)
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace(" ", "T", 1))
        except ValueError:
            return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def get_str(row: Dict[str, Any], *names: str) -> Optional[str]:
    value = get_col(row, *names)
    return None if value is None else str(value)


@dataclasses.dataclass
class OpenflowDeployment:
    key: str
    name: Optional[str] = None
    status: Optional[str] = None
    owner: Optional[str] = None
    display_name: Optional[str] = None
    created_on: Optional[str] = None
    # Parsed form of created_on, used for ORDERING. The string is kept
    # for the pagination cursor, which Snowflake parses itself.
    created_at: Optional[datetime] = None
    deleted_on: Optional[str] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> Optional["OpenflowDeployment"]:
        key = get_str(row, COL_DEPLOYMENT_KEY, COL_KEY)
        if key is None:
            return None
        return cls(
            key=key,
            name=get_str(row, COL_NAME),
            status=get_str(row, COL_STATUS),
            owner=get_str(row, COL_OWNER),
            display_name=get_str(row, COL_DISPLAY_NAME),
            created_on=get_str(row, COL_CREATED_ON),
            created_at=get_datetime(row, COL_CREATED_ON),
            deleted_on=get_str(row, COL_DELETED_ON),
        )


@dataclasses.dataclass
class OpenflowRuntime:
    key: str
    name: Optional[str] = None
    deployment_name: Optional[str] = None
    status: Optional[str] = None
    owner: Optional[str] = None
    display_name: Optional[str] = None
    object_database: Optional[str] = None
    object_schema: Optional[str] = None
    execute_as_role: Optional[str] = None
    created_on: Optional[str] = None
    # Parsed form of created_on, used for ORDERING. The string is kept
    # for the pagination cursor, which Snowflake parses itself.
    created_at: Optional[datetime] = None
    deleted_on: Optional[str] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> Optional["OpenflowRuntime"]:
        key = get_str(row, COL_RUNTIME_KEY, COL_KEY)
        if key is None:
            return None
        return cls(
            key=key,
            name=get_str(row, COL_NAME),
            deployment_name=get_str(row, COL_DEPLOYMENT, COL_DEPLOYMENT_NAME),
            status=get_str(row, COL_STATUS),
            owner=get_str(row, COL_OWNER),
            display_name=get_str(row, COL_DISPLAY_NAME),
            object_database=get_str(row, COL_DATABASE_NAME),
            object_schema=get_str(row, COL_SCHEMA_NAME),
            execute_as_role=get_str(row, COL_EXECUTE_AS_ROLE, COL_EXECUTE_AS_ROLE_NAME),
            created_on=get_str(row, COL_CREATED_ON),
            created_at=get_datetime(row, COL_CREATED_ON),
            deleted_on=get_str(row, COL_DELETED_ON),
        )


@dataclasses.dataclass
class OpenflowConnector:
    # Neither surface has a usable single-column id: CONNECTOR_HISTORY has no
    # key column at all (CONNECTOR_ID is the only stable id it carries, and it
    # lags ~20 minutes behind SHOW), while SHOW OPENFLOW CONNECTORS carries no
    # id column at all. Connector names are also not unique account-wide (SHOW
    # OPENFLOW CONNECTORS is account-wide, spanning many runtimes), so identity
    # must be the composite of (runtime, name) that both surfaces agree on.
    name: str
    runtime_name: str
    connector_id: Optional[str] = None
    connector_definition: Optional[str] = None
    status: Optional[str] = None
    owner: Optional[str] = None
    display_name: Optional[str] = None
    default_version: Optional[str] = None
    version_location_uri: Optional[str] = None
    # Only SHOW carries these two; the history view does not. They exist to
    # address the connector in DESCRIBE, which is the only surface exposing
    # CONNECTOR_URL.
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    created_on: Optional[str] = None
    # Parsed form of created_on, used for ORDERING. The string is kept
    # for the pagination cursor, which Snowflake parses itself.
    created_at: Optional[datetime] = None
    deleted_on: Optional[str] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> Optional["OpenflowConnector"]:
        name = get_str(row, COL_NAME)
        runtime_name = get_str(row, COL_RUNTIME, COL_RUNTIME_NAME)
        # Both halves of the composite identity are required. Both surfaces
        # carry the runtime association (SHOW as `runtime`, the view as
        # RUNTIME_NAME), so a row missing it is not a usable connector rather
        # than one with a degraded key.
        if name is None or runtime_name is None:
            return None
        return cls(
            name=name,
            runtime_name=runtime_name,
            connector_id=get_str(row, COL_CONNECTOR_ID),
            connector_definition=get_str(row, COL_CONNECTOR_DEFINITION),
            status=get_str(row, COL_STATUS),
            owner=get_str(row, COL_OWNER),
            display_name=get_str(row, COL_DISPLAY_NAME),
            default_version=get_str(row, COL_DEFAULT_VERSION),
            version_location_uri=get_str(row, COL_DEFAULT_VERSION_LOCATION_URI),
            database_name=get_str(row, COL_DATABASE_NAME),
            schema_name=get_str(row, COL_SCHEMA_NAME),
            created_on=get_str(row, COL_CREATED_ON),
            created_at=get_datetime(row, COL_CREATED_ON),
            deleted_on=get_str(row, COL_DELETED_ON),
        )

    @property
    def key(self) -> str:
        # "/" is not among the URN reserved characters (see
        # datahub.utilities.urn_encoder.RESERVED_CHARS), so it needs no
        # escaping when this key ends up inside a DataFlow URN component.
        return f"{self.runtime_name}/{self.name}"

    @property
    def location(self) -> Optional[Tuple[str, str, str]]:
        """(database, schema, runtime), or None when SHOW did not supply it.

        The scope one DESCRIBE's answer is valid in. Both the canvas-URL cache
        and the gate that budgets those queries derive from this, so they
        cannot drift apart the way two hand-built keys did.
        """
        if not self.database_name or not self.schema_name or not self.runtime_name:
            return None
        return (self.database_name, self.schema_name, self.runtime_name)

    @property
    def fqn(self) -> Optional[str]:
        """Quoted three-part name, or None when SHOW did not supply the parts.

        Openflow object names are case-sensitive and may contain characters that
        an unquoted identifier cannot carry, so every part is double-quoted; an
        embedded double quote is doubled, which is how Snowflake escapes one
        inside a quoted identifier.
        """
        if not self.database_name or not self.schema_name:
            return None
        parts = (self.database_name, self.schema_name, self.name)
        return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


# Sorts before any real timestamp, so a row whose CREATED_ON was absent or
# unparseable never displaces one that has a usable value.
_EPOCH = datetime.min.replace(tzinfo=timezone.utc)

RowModel = TypeVar("RowModel", OpenflowDeployment, OpenflowRuntime, OpenflowConnector)


def _resolve_per_key(rows: List[RowModel]) -> Tuple[List[RowModel], int]:
    # Newest CREATED_ON wins; on a tie, CLOSED beats OPEN.
    #
    # This rule is deliberately correct under BOTH readings of these views, because
    # which one applies is unverified (the account available here holds one row per
    # view, so churn cannot be observed):
    #   incarnation-style (one row per object life) - two incarnations of a key have
    #     different CREATED_ON, so newest-wins picks the current one and the tie-break
    #     never fires.
    #   event-style (a row per state change) - the delete event carries the later
    #     timestamp, so newest-wins detects the deletion.
    #
    # An earlier revision preferred OPEN over CLOSED outright. That was wrong here and
    # the SHOW-authority rule in merge_show_and_history is why: this resolver governs
    # only keys SHOW did NOT list, and a SHOW-absent key is exactly what a deleted
    # object looks like -- so 100% of deletions resolve through this function.
    # Preferring open therefore reported every deleted object as live under the
    # event-style reading, deterministically and with nothing to notice it, silently
    # disabling the DELETION_DETECTION capability this source declares. Preferring
    # open was introduced to protect live objects, and the SHOW-authority rule already
    # does that, which removed the reason for it.
    #
    # Residual, accepted: a SHOW-invisible live object whose rows carry NO timestamps
    # and whose only other row is a deletion is reported gone. Its metadata is stale
    # regardless, and the alternative loses real deletions.
    resolved: Dict[str, RowModel] = {}
    mixed_keys: Set[str] = set()
    for row in rows:
        current = resolved.get(row.key)
        if current is None:
            resolved[row.key] = row
            continue
        if (row.deleted_on is None) != (current.deleted_on is None):
            # Direction-neutral signal, returned to the caller for the report.
            # Non-zero the instant a key owns BOTH an open and a closed lifecycle
            # row, which is the only condition under which the unverified view-grain
            # question can change the answer. Zero on an incarnation-style view with
            # no drop-and-recreate; the moment it is not zero, the assumption this
            # resolver rests on is worth re-checking against a real account.
            #
            # Counted per KEY, not per comparison: a key holding one open row and
            # five closed ones is one ambiguous key, and counting comparisons would
            # report it as 5 or 1 depending on arrival order and could exceed
            # num_connectors, which the field name promises it cannot.
            mixed_keys.add(row.key)
        # Compared as instants, never as their rendering: two rows in
        # different UTC offsets order correctly only this way.
        row_at = row.created_at or _EPOCH
        current_at = current.created_at or _EPOCH
        newer = row_at > current_at
        tied_and_closed = row_at == current_at and row.deleted_on is not None
        if newer or tied_and_closed:
            resolved[row.key] = row
    return list(resolved.values()), len(mixed_keys)


def merge_show_and_history(
    show_rows: List[RowModel], history_rows: List[RowModel]
) -> Tuple[List[RowModel], int]:
    # Returns the merged rows and the count of keys holding BOTH an open and a
    # closed lifecycle row -- see _resolve_per_key for why that number matters.
    #
    # SHOW is authoritative for object location (the views returned NULL
    # DATABASE_NAME where SHOW was populated). The views are authoritative for
    # what only they carry: surrogate ids, timestamps, EXECUTE_AS_ROLE_NAME.
    #
    # Present in SHOW but absent from the view means NEW, never deleted: a
    # runtime visible to SHOW was still missing from the view ~20 minutes after
    # creation, so a view-only reading would report zero runtimes to a user who
    # had just created one.
    # Non-mutating by design: `show_rows` items are caller-owned, and at least
    # one downstream caller keeps its own reference to them after calling this
    # function. dataclasses.replace() builds a new merged instance instead of
    # setattr-ing onto the caller's object, so show_rows and its elements are
    # left untouched.
    # Resolve the history side per key BEFORE merging. These views are append-style
    # lifecycle records, and `key` is not per-incarnation: a connector's key is the
    # composite <runtime_name>/<name> (see OpenflowConnector.key), which is stable
    # across a drop and re-create under the same name, and each incarnation carries
    # its own CONNECTOR_ID / RUNTIME_ID / DEPLOYMENT_ID surrogate. So one key can
    # legitimately own several rows, of which the older ones carry DELETED_ON.
    #
    # Merging them naively lets a superseded incarnation's DELETED_ON survive onto
    # the live object -- the field-level merge below fills any None field from the
    # history row, and `deleted_on` on a live row IS None -- after which the
    # caller's `deleted_on is None` filter drops an object that exists and stale
    # entity removal soft-deletes it. Newest CREATED_ON wins, so an object counts as
    # deleted only when its most recent lifecycle row says so.
    history_rows, mixed_lifecycle_keys = _resolve_per_key(history_rows)

    by_key: Dict[str, RowModel] = {row.key: row for row in show_rows}
    for history_row in history_rows:
        existing = by_key.get(history_row.key)
        if existing is None:
            # View-only: either genuinely deleted, or invisible to SHOW for
            # privilege reasons. Deleted rows are filtered by the caller.
            by_key[history_row.key] = history_row
            continue
        if history_row.deleted_on is not None:
            # SHOW listed this key, so the object exists; this history row describes
            # an incarnation that ended. Take NOTHING from it -- not `deleted_on`,
            # and not the other fields either.
            #
            # An earlier revision excluded only `deleted_on`. That closed the
            # dangerous field and left the class open: `status` still arrived as
            # 'DELETED' on a live entity (previously invisible, because that same row
            # also set deleted_on and the object was dropped before anyone saw it),
            # and `version_location_uri` could point _read_connector_config at a
            # superseded incarnation's config.json, yielding lineage for the wrong
            # version of the connector.
            #
            # SHOW is authoritative for existence, so this single condition makes
            # "a live object contradicted by a dead incarnation's row" unreachable,
            # independent of timestamp population, view grain, tie order or UTC
            # offsets. A newer OPEN row for the same key is still merged normally.
            continue
        updates = {
            field.name: getattr(history_row, field.name)
            for field in dataclasses.fields(existing)
            if getattr(existing, field.name) is None
            and getattr(history_row, field.name) is not None
        }
        if updates:
            by_key[history_row.key] = dataclasses.replace(existing, **updates)
    return list(by_key.values()), mixed_lifecycle_keys
