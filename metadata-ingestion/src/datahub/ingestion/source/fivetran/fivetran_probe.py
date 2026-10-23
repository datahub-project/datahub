import logging
from dataclasses import dataclass, field
from typing import Callable, ClassVar, Dict, List, Optional, Tuple, TypeVar

import pydantic
import requests

from datahub.ingestion.agent.error_policy import DEFECT_TYPES, foreign_label
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    UnloggedStructuredLogs,
    echoed,
    resolve_name,
    soft_listing,
    take,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeReadFailed,
    ProbeSoftError,
)
from datahub.ingestion.api.source import StructuredLogs
from datahub.ingestion.source.fivetran.config import (
    DESTINATION_ID_IS_NULL,
    FIVETRAN_CONNECTOR_KIND,
    FIVETRAN_DESTINATION_KIND,
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import Connector
from datahub.ingestion.source.fivetran.fivetran_log_db_reader import (
    FivetranLogDbReader,
    connector_from_log_row,
)
from datahub.ingestion.source.fivetran.fivetran_log_rest_reader import (
    LINEAGE_FROM_DB_LOG,
    FivetranLogRestReader,
    connector_from_listed,
)
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranListedConnection,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


# The package every reused reader logs under.
_FIVETRAN_LOGGER = "datahub.ingestion.source.fivetran"


@dataclass
class _ProbeReport(FivetranSourceReport):
    """The report the reused readers write into, recorded but never logged:
    their warnings carry the exception, whose text can quote a warehouse
    driver error or a Fivetran reply body, and report_log would write it to
    the console. The probe result shows `title: message` only."""

    _structured_logs: StructuredLogs = field(default_factory=UnloggedStructuredLogs)


def _listed_connector(listed: FivetranListedConnection) -> Connector:
    # No lineage fetch, and no connected_by: the probe never reports the user.
    return connector_from_listed(listed, lineage=[], user_id="")


class FivetranMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over the backend this recipe's ingestion reads.

    Which backend that is follows the resolved `log_source`, exactly as
    FivetranSource._build_log_reader decides it. The two backends see
    different connectors, name them from different fields, and apply
    connector_patterns differently, so answering from the other one would
    describe a run this recipe never makes.

    Reuses the readers' fetch layer and never their filtering:
    get_allowed_connectors_list applies both patterns itself, so delegating to
    it would make a denied connector vanish instead of being reported.
    """

    # Dropped, not scrubbed, while a probe runs (agent.log_guard): the reused
    # readers log query and reply details at debug level, and a value read
    # off the warehouse or the API has no recipe secret to mask. Every
    # degraded read still reaches the result through probe_report.
    silenced_loggers: ClassVar[Tuple[str, ...]] = (_FIVETRAN_LOGGER,)

    def __init__(
        self, config: FivetranSourceConfig, api_client: Optional[FivetranAPIClient]
    ) -> None:
        self._config = config
        self._report = _ProbeReport()
        self._api_client = api_client
        if api_client is not None:
            self._on_exit(api_client._session.close)
        # Groups whose connections could not be listed, so a lookup that
        # misses can say the connector may be on one of them.
        self._unlisted_groups: List[str] = []

    @classmethod
    def for_config(cls, config: FivetranSourceConfig) -> "FivetranMetadataProbe":
        # FivetranAPIClient.__init__ only builds a requests.Session; it sends
        # nothing, so building it here is free.
        api_client = (
            FivetranAPIClient(config.api_config)
            if config.api_config is not None
            else None
        )
        return cls(config, api_client)

    @property
    def probe_report(self) -> FivetranSourceReport:
        """The report the reused readers write into, so the warnings they
        record (a truncated lineage, a failed per-table column fetch) reach
        the caller instead of staying in an ingestion-only report."""
        return self._report

    @property
    def _uses_rest(self) -> bool:
        return self._config.log_source == "rest_api"

    def _db(self) -> FivetranLogDbReader:
        # Built on first use: FivetranLogDbReader.__init__ connects (it runs
        # SELECT @@project_id on BigQuery and builds the engine everywhere),
        # and `probe methods` or a REST-only command must not pay for that.
        return self._open_once(
            "log-db", self._open_db, close=lambda db: db.engine.dispose()
        )

    def _open_db(self) -> FivetranLogDbReader:
        log_config = self._config.fivetran_log_config
        # validate_log_source_credentials guarantees this in log_database
        # mode; REST-mode callers check fivetran_log_config before asking.
        assert log_config is not None
        try:
            return FivetranLogDbReader(
                log_config,
                self._report,
                max_jobs_per_connector=self._config.max_jobs_per_connector,
                max_table_lineage_per_connector=self._config.max_table_lineage_per_connector,
                max_column_lineage_per_connector=self._config.max_column_lineage_per_connector,
            )
        except DEFECT_TYPES:
            # Our code failing (config drift between this and the reader's
            # constructor), not the warehouse: exit 1, as a defect.
            raise
        except Exception as exc:
            # The recipe validated before we got here, so a failure opening
            # the warehouse is the source's, not the caller's. Mapped to
            # exit 3 for the same reason run_probe_method wraps a failing
            # for_config: Snowflake raises ConfigurationError for DNS and
            # auth failures alike, which would otherwise read as exit 2.
            # `from None`: a driver error can quote the connection URL.
            raise ProbeConnectionError(
                f"could not open the Fivetran log warehouse "
                f"({log_config.destination_platform}): {foreign_label(exc)}"
            ) from None

    def _db_connectors(self, destination: Optional[str]) -> List[Connector]:
        db = self._db()
        rows = db._query(db.fivetran_log_query.get_connectors_query())
        # get_allowed_connectors_list's mapping, minus its two pattern checks
        # and the connecting user, which the probe never reports.
        found = [
            connector_from_log_row(row, user_id="")
            for row in rows
            if destination is None or row[Constant.DESTINATION_ID] == destination
        ]
        self._warn_null_destinations(found)
        return found

    def _warn_null_destinations(self, connectors: List[Connector]) -> None:
        # Said at listing time as well as by `probe filter --from-run`
        # (fivetran_selection.NULL_DESTINATION_ID): get_allowed_connectors_list
        # matches destination_patterns against the NULL and raises TypeError,
        # which stops the whole run, not just this connector.
        unplaced = sorted(
            c.connector_id for c in connectors if c.destination_id is None
        )
        if unplaced:
            self._warn(
                f"{len(unplaced)} connector(s) in the log have no destination_id "
                f"({', '.join(echoed(c) for c in unplaced[:10])}); ingestion stops "
                f"with an error on such a connector unless connector_patterns "
                f"drops it first, because destination_patterns cannot be matched "
                f"against a missing destination"
            )

    def _api(self) -> FivetranAPIClient:
        # validate_log_source_credentials guarantees api_config in rest_api mode.
        assert self._api_client is not None
        return self._api_client

    def _rest(self, context: str, call: Callable[[], T]) -> T:
        """Run one REST read, keeping a bad reply apart from bad input.

        FivetranAPIClient raises ValueError for a reply it cannot use: a
        non-Success envelope, a missing `data`, or a pydantic ValidationError
        (a ValueError subclass). The CLI reads ValueError as exit 2 and would
        send the caller to fix an argument that was never wrong. HTTPError is
        not a ValueError and passes through to exit 3 unchanged."""
        try:
            return call()
        except pydantic.ValidationError as exc:
            # str(exc) quotes each rejected input value, and the payload can
            # carry user ids (connected_by). Name where and how the reply was
            # wrong -- in the debug log too, which --debug sends to stderr, so
            # no exc_info: its traceback would print str(exc).
            problems = "; ".join(
                f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: "
                f"{error['type']}"
                for error in exc.errors()
            )
            logger.debug("Fivetran reply failed validation: %s (%s)", context, problems)
            raise ProbeReadFailed(
                f"{context}: the reply did not have the expected shape ({problems})"
            ) from None
        except ValueError as exc:
            raise ProbeReadFailed(
                f"{context}: the reply was unusable ({type(exc).__name__})"
            ) from None

    def _group_connections(self, group_id: str) -> List[FivetranListedConnection]:
        """One destination's connections. A 404 (the destination is gone) is
        raised as a ProbeSoftError naming it, because the caller decides
        what that means: a skipped group in a walk over all of them, a wrong
        id when the caller named this one."""
        reasons: List[str] = []
        with soft_listing(
            reasons.append,
            404,
            context=f"connections listing for destination {echoed(group_id)}",
        ):
            return self._rest(
                f"listing connections of destination {echoed(group_id)}",
                lambda: list(self._api().list_connections(group_id)),
            )
        raise ProbeSoftError(reasons[0])

    def _rest_connectors(
        self, destination: Optional[str], stop_after: Optional[int]
    ) -> List[Connector]:
        if destination is not None:
            group_ids = [destination]
        else:
            group_ids = [
                g.id
                for g in self._rest(
                    "listing Fivetran groups", lambda: list(self._api().list_groups())
                )
            ]
        found: List[Connector] = []
        for group_id in group_ids:
            try:
                listed = self._group_connections(group_id)
            except ProbeSoftError as exc:
                if destination is not None:
                    raise ProbeArgumentError(
                        f"no Fivetran destination with id {echoed(destination)}; list "
                        f"them with `probe run destinations`"
                    ) from exc
                # A destination deleted between the groups listing and this
                # call. Ingestion skips the group with a warning too.
                self._warn(str(exc))
                continue
            except (ProbeReadFailed, requests.RequestException) as exc:
                # The errors ingestion recovers from per group
                # (_RECOVERABLE_REST_ERRORS; ValueError arrives here as
                # ProbeReadFailed). Asked for this one destination, the caller
                # gets the failure itself rather than an empty listing.
                if destination is not None:
                    raise
                self._unlisted_groups.append(group_id)
                self._warn(
                    f"could not list the connections of destination "
                    f"{echoed(group_id)} ({foreign_label(exc)}); skipped, as "
                    f"ingestion skips it"
                )
                continue
            found.extend(_listed_connector(item) for item in listed)
            # Every group is one more request, so stop once the limit is met.
            if stop_after is not None and len(found) >= stop_after:
                break
        return found

    def _list_connectors(
        self, destination: Optional[str], stop_after: Optional[int] = None
    ) -> List[Connector]:
        found = (
            self._rest_connectors(destination, stop_after)
            if self._uses_rest
            else self._db_connectors(destination)
        )
        return found if stop_after is None else found[:stop_after]

    def _resolve(self, connector: str) -> Connector:
        """A connector by id, else by name. Costs one listing.

        By id first because ids are unique and names are not: two destinations
        can each hold a connector writing to a schema of the same name."""

        def missed() -> None:
            # Not ProbeArgumentError (exit 2): the name may be right and on a
            # destination this key could not read.
            if self._unlisted_groups:
                raise ProbeReadFailed(
                    f"no connector with id or name {echoed(connector)} on the "
                    f"destinations that could be read; it may be on one that "
                    f"could not be listed ({', '.join(self._unlisted_groups)})"
                )

        return resolve_name(
            connector,
            self._list_connectors(destination=None),
            key=lambda c: c.connector_name,
            id_key=lambda c: c.connector_id,
            kind="connector",
            list_command="probe run connectors",
            on_ambiguous="pass the connector_id instead",
            on_miss=missed,
        ).record

    def _rest_reader(self) -> FivetranLogRestReader:
        """Ingestion's REST reader, wired as FivetranSource._build_log_reader
        wires it -- the log warehouse as lineage fallback when one is
        configured -- so the probe follows its fallback order. Its
        constructor sends nothing."""
        return self._open_once(
            "rest-reader",
            lambda: FivetranLogRestReader(
                self._api(),
                self._report,
                max_table_lineage_per_connector=self._config.max_table_lineage_per_connector,
                max_column_lineage_per_connector=self._config.max_column_lineage_per_connector,
                db_lineage_reader=(
                    self._db() if self._config.fivetran_log_config is not None else None
                ),
            ),
        )

    def _connector_record(self, connector: Connector) -> Dict[str, object]:
        return {
            "name": connector.connector_name,
            "connector_id": connector.connector_id,
            "connector_type": connector.connector_type,
            "destination_id": connector.destination_id,
            DESTINATION_ID_IS_NULL: connector.destination_id is None,
            "paused": connector.paused,
            "sync_frequency": connector.sync_frequency,
            # sources_to_platform_instance is keyed by connector id. An
            # unmapped connector gets default platform details, which is the
            # usual reason its upstream lineage URNs do not match another
            # source's, so make that visible before a run.
            "in_sources_to_platform_instance": connector.connector_id
            in self._config.sources_to_platform_instance,
        }

    def _destination_record(self, destination_id: str) -> Dict[str, object]:
        return {
            "name": destination_id,
            "in_destination_to_platform_instance": destination_id
            in self._config.destination_to_platform_instance,
        }

    @probe_method(kind=FIVETRAN_DESTINATION_KIND, row_limit_param="limit")
    def destinations(self, limit: int = 200) -> List[Dict[str, object]]:
        """Destinations (Fivetran groups) this recipe reads connectors from,
        by the destination id destination_patterns is matched against --
        including ids the pattern would exclude. In log_database mode these
        are the destinations of the connectors present in the log, so a
        destination with no connector there does not appear; it would filter
        nothing anyway. In rest_api mode these are the API key's groups, each
        with its display name."""
        if self._uses_rest:
            groups = self._rest(
                "listing Fivetran groups",
                # take stops list_groups paging once the limit is met.
                lambda: take(self._api().list_groups(), limit),
            )
            return [
                {**self._destination_record(group.id), "group_name": group.name}
                for group in groups
            ]
        # A connector with no destination_id names no destination; it is
        # reported by _warn_null_destinations instead.
        ids = sorted(
            {
                c.destination_id
                for c in self._list_connectors(destination=None)
                if c.destination_id is not None
            }
        )
        return [self._destination_record(d) for d in ids[:limit]]

    @probe_method(
        kind=FIVETRAN_CONNECTOR_KIND,
        row_limit_param="limit",
        parent_params=("destination",),
    )
    def connectors(
        self, destination: Optional[str] = None, limit: int = 200
    ) -> List[Dict[str, object]]:
        """Connectors this recipe's ingestion would consider, including ones
        connector_patterns or destination_patterns would drop -- a denied
        connector is reported, not hidden, so `probe filter --kind Connector`
        can explain it. `name` is the string connector_patterns is matched
        against; pass `destination_id` as --parent so the destination's own
        pattern is judged too, or pass --destination here to list one
        destination and have it reported as the parent. Read from the same
        backend ingestion uses (log_source). `destination_id_is_null` marks a
        connector the log places on no destination, which ingestion stops on.
        Metadata only: the connecting user is never returned."""
        found = self._list_connectors(destination, stop_after=limit)
        if destination is not None and not found:
            self._warn(
                f"no connector on destination {echoed(destination)} was found; check "
                f"the id against `probe run destinations`"
            )
        return [self._connector_record(c) for c in found]

    @probe_method(row_limit_param="limit")
    def connector_tables(
        self, connector: str, include_columns: bool = False, limit: int = 200
    ) -> List[Dict[str, object]]:
        """Source-to-destination table mappings for one connector, by
        connector_id or name -- the table lineage ingestion would emit for it,
        read the way this recipe's log_source reads it and capped where
        ingestion caps it (max_table_lineage_per_connector). `lineage_source`
        says which backend answered: in rest_api mode with a log configured,
        ingestion tries the log first and falls back to the REST schemas
        endpoint. include_columns adds the column mappings (names only).
        Reported whether or not the recipe's patterns keep this connector;
        ask `probe filter` for that. In rest_api mode each table costs one
        /columns request, as it does in ingestion."""
        target = self._resolve(connector)
        if self._uses_rest:
            fetched = self._rest_reader().fetch_lineage_with_source(target.connector_id)
            lineage, lineage_source = fetched.lineage, fetched.source
        else:
            # The same call get_allowed_connectors_list makes, so the Google
            # Sheets column-lineage opt-out applies as it does in ingestion.
            self._db()._fill_connectors_lineage([target])
            lineage, lineage_source = target.lineage, LINEAGE_FROM_DB_LOG
        cap = self._config.max_table_lineage_per_connector
        if lineage_source == LINEAGE_FROM_DB_LOG and len(lineage) >= cap:
            # The REST path records its own truncation warning; the log query
            # caps silently in SQL (QUALIFY ... <= max_table_lineage), so say it.
            self._warn(
                f"this connector reached max_table_lineage_per_connector ({cap}); "
                f"ingestion stops at the same point, so tables past it get no "
                f"lineage"
            )
        emits_columns = self._config.include_column_lineage
        if not emits_columns:
            self._warn(
                "include_column_lineage is false, so ingestion emits no column "
                "lineage; column counts and mappings are shown as empty"
            )
        records: List[Dict[str, object]] = []
        for table in lineage[:limit]:
            columns = table.column_lineage if emits_columns else []
            record: Dict[str, object] = {
                "source_table": table.source_table,
                "destination_table": table.destination_table,
                "column_count": len(columns),
                "lineage_source": lineage_source,
            }
            if include_columns:
                record["columns"] = [
                    {"source": c.source_column, "destination": c.destination_column}
                    for c in columns
                ]
            records.append(record)
        return records

    @probe_method(row_limit_param="limit")
    def sync_history(self, connector: str, limit: int = 50) -> List[Dict[str, object]]:
        """Recent sync runs of one connector, by connector_id or name: the runs
        ingestion turns into DataProcessInstance events, within
        history_sync_lookback_period days and max_jobs_per_connector. Times
        are epoch seconds, newest first. Only the status is returned, never
        the run's message text. Needs the Fivetran log warehouse: in rest_api
        mode without fivetran_log_config this is empty, because Fivetran's
        REST API has no sync-history endpoint and ingestion emits no runs
        either."""
        target = self._resolve(connector)
        if self._config.fivetran_log_config is None:
            self._warn(
                "no fivetran_log_config, so there is no sync history to read: "
                "Fivetran's REST API has no sync-history endpoint and ingestion "
                "emits no run events in this mode. Add fivetran_log_config "
                "alongside log_source: rest_api to get them."
            )
            return []
        jobs = (
            self._db()
            .fetch_jobs_for_connectors(
                [target.connector_id], self._config.history_sync_lookback_period
            )
            .get(target.connector_id, [])
        )
        return [
            {
                "sync_id": job.job_id,
                "start_time": job.start_time,
                "end_time": job.end_time,
                "status": job.status,
            }
            for job in jobs[:limit]
        ]
