import itertools
import logging
from typing import Callable, Dict, List, Optional, Tuple, TypeVar

import pydantic
import requests
import sqlalchemy.exc

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase, soft_listing
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeReadFailed,
    ProbeSoftError,
)
from datahub.ingestion.source.fivetran.config import (
    FIVETRAN_CONNECTOR_KIND,
    FIVETRAN_DESTINATION_KIND,
    REST_CONNECTOR_MATCH_NOTE,
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import Connector, TableLineage
from datahub.ingestion.source.fivetran.fivetran_log_db_reader import FivetranLogDbReader
from datahub.ingestion.source.fivetran.fivetran_log_rest_reader import (
    FivetranLogRestReader,
)
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranConnectionSchemas,
    FivetranListedConnection,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# The failures FivetranLogRestReader._fetch_lineage treats as "the log's
# lineage tables are unreadable, use the REST schemas instead". Mirrored so the
# probe falls back exactly where ingestion does.
_DB_LINEAGE_FALLBACK_ERRORS = (
    sqlalchemy.exc.SQLAlchemyError,
    ValueError,
    KeyError,
    AttributeError,
)


def _failure_label(exc: Exception) -> str:
    """Class name, plus the HTTP status when there is one; never the text,
    which comes from the remote service and is not scrubbed in a warning."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return f"{type(exc).__name__}, HTTP {status}"
    return type(exc).__name__


def _connector_from_listed(listed: FivetranListedConnection) -> Connector:
    # Field mapping from FivetranLogRestReader._build_connector, without its
    # lineage fetch and without connected_by, which the probe never reports.
    return Connector(
        connector_id=listed.id,
        connector_name=listed.schema_,
        connector_type=listed.service,
        paused=listed.paused,
        sync_frequency=listed.sync_frequency,
        destination_id=listed.group_id,
        user_id="",
        lineage=[],
        jobs=[],
    )


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

    def __init__(
        self, config: FivetranSourceConfig, api_client: Optional[FivetranAPIClient]
    ) -> None:
        self._config = config
        self._report = FivetranSourceReport()
        self._api_client = api_client
        # Built on first use. FivetranLogDbReader.__init__ connects (it runs
        # SELECT @@project_id on BigQuery and builds the engine everywhere),
        # and `probe methods` or a REST-only command must not pay for that.
        self._db_reader: Optional[FivetranLogDbReader] = None
        self._rest_lineage_reader: Optional[FivetranLogRestReader] = None
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

    def __exit__(self, *exc: object) -> None:
        if self._api_client is not None:
            self._api_client._session.close()
        if self._db_reader is not None:
            self._db_reader.engine.dispose()
        super().__exit__(*exc)

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
        if self._db_reader is None:
            log_config = self._config.fivetran_log_config
            # validate_log_source_credentials guarantees this in log_database
            # mode; REST-mode callers check fivetran_log_config before asking.
            assert log_config is not None
            try:
                self._db_reader = FivetranLogDbReader(
                    log_config,
                    self._report,
                    max_jobs_per_connector=self._config.max_jobs_per_connector,
                    max_table_lineage_per_connector=self._config.max_table_lineage_per_connector,
                    max_column_lineage_per_connector=self._config.max_column_lineage_per_connector,
                )
            except Exception as exc:
                # The recipe validated before we got here, so a failure opening
                # the warehouse is the source's, not the caller's. Mapped to
                # exit 3 for the same reason run_probe_method wraps a failing
                # for_config: Snowflake raises ConfigurationError for DNS and
                # auth failures alike, which would otherwise read as exit 2.
                raise ProbeConnectionError(
                    f"could not open the Fivetran log warehouse "
                    f"({log_config.destination_platform}): {type(exc).__name__}"
                ) from exc
        return self._db_reader

    def _db_connectors(self, destination: Optional[str]) -> List[Connector]:
        db = self._db()
        rows = db._query(db.fivetran_log_query.get_connectors_query())
        # Field mapping mirrors FivetranLogDbReader.get_allowed_connectors_list,
        # minus its two pattern checks and minus connecting_user_id, which the
        # probe never reports.
        return [
            Connector(
                connector_id=row[Constant.CONNECTOR_ID],
                connector_name=row[Constant.CONNECTOR_NAME],
                connector_type=row[Constant.CONNECTOR_TYPE_ID],
                paused=row[Constant.PAUSED],
                sync_frequency=row[Constant.SYNC_FREQUENCY],
                destination_id=row[Constant.DESTINATION_ID],
                user_id="",
                lineage=[],
                jobs=[],
            )
            for row in rows
            if destination is None or row[Constant.DESTINATION_ID] == destination
        ]

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
            context=f"connections listing for destination '{group_id}'",
        ):
            return self._rest(
                f"listing connections of destination '{group_id}'",
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
                        f"no Fivetran destination with id '{destination}'; list "
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
                    f"'{group_id}' ({_failure_label(exc)}); skipped, as "
                    f"ingestion skips it"
                )
                continue
            found.extend(_connector_from_listed(item) for item in listed)
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
        listed = self._list_connectors(destination=None)
        matches = [c for c in listed if c.connector_id == connector] or [
            c for c in listed if c.connector_name == connector
        ]
        if not matches and self._unlisted_groups:
            # Not ProbeArgumentError (exit 2): the name may be right and on a
            # destination this key could not read.
            raise ProbeReadFailed(
                f"no connector with id or name '{connector}' on the destinations "
                f"that could be read; it may be on one that could not be listed "
                f"({', '.join(self._unlisted_groups)})"
            )
        if not matches:
            raise ProbeArgumentError(
                f"no connector with id or name '{connector}'; list them with "
                f"`probe run connectors`"
            )
        if len(matches) > 1:
            ids = ", ".join(sorted(c.connector_id for c in matches))
            raise ProbeArgumentError(
                f"'{connector}' names {len(matches)} connectors ({ids}); pass "
                f"the connector_id instead"
            )
        return matches[0]

    def _rest_reader(self) -> FivetranLogRestReader:
        # Its constructor sends nothing. Built for _extract_lineage_from_schemas,
        # which carries ingestion's caps and truncation warnings.
        if self._rest_lineage_reader is None:
            self._rest_lineage_reader = FivetranLogRestReader(
                self._api(),
                self._report,
                max_table_lineage_per_connector=self._config.max_table_lineage_per_connector,
                max_column_lineage_per_connector=self._config.max_column_lineage_per_connector,
            )
        return self._rest_lineage_reader

    def _lineage_for(self, target: Connector) -> Tuple[List[TableLineage], str]:
        if not self._uses_rest:
            # The same call get_allowed_connectors_list makes, so the Google
            # Sheets column-lineage opt-out applies as it does in ingestion.
            self._db()._fill_connectors_lineage([target])
            return target.lineage, "log_database"
        if self._config.fivetran_log_config is not None:
            try:
                from_log = (
                    self._db()
                    .fetch_lineage_for_connectors([target.connector_id])
                    .get(target.connector_id, [])
                )
            except _DB_LINEAGE_FALLBACK_ERRORS as exc:
                self._warn(
                    f"the log warehouse's lineage tables could not be read "
                    f"({type(exc).__name__}); ingestion falls back to "
                    f"the REST schemas endpoint here, and so did this"
                )
                from_log = []
            if from_log:
                return from_log, "log_database"
        try:
            schemas = self._connection_schemas(target.connector_id)
        except (ProbeReadFailed, requests.RequestException) as exc:
            # The failures FivetranLogRestReader._fetch_lineage recovers from
            # (_RECOVERABLE_REST_ERRORS; ValueError arrives as ProbeReadFailed):
            # ingestion emits the connector without lineage, so this does too.
            self._warn(
                f"could not read the schemas of connector "
                f"'{target.connector_id}' ({_failure_label(exc)}); "
                f"ingestion emits it without table or column lineage"
            )
            return [], "rest_schemas"
        if schemas is None:
            return [], "rest_schemas"
        return (
            self._rest_reader()._extract_lineage_from_schemas(
                schemas, target.connector_id
            ),
            "rest_schemas",
        )

    def _connection_schemas(
        self, connector_id: str
    ) -> Optional[FivetranConnectionSchemas]:
        """A connector's schema config, or None with a warning when the
        endpoint answers 404."""
        with soft_listing(
            self._warn, 404, context=f"schemas for connector '{connector_id}'"
        ):
            return self._rest(
                f"reading schemas of connector '{connector_id}'",
                lambda: self._api().get_connection_schemas(connector_id),
            )
        return None

    def _connector_record(self, connector: Connector) -> Dict[str, object]:
        return {
            "name": connector.connector_name,
            "connector_id": connector.connector_id,
            "connector_type": connector.connector_type,
            "destination_id": connector.destination_id,
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
                # islice stops list_groups paging once the limit is met.
                lambda: list(itertools.islice(self._api().list_groups(), limit)),
            )
            return [
                {**self._destination_record(group.id), "group_name": group.name}
                for group in groups
            ]
        ids = sorted(
            {c.destination_id for c in self._list_connectors(destination=None)}
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
        backend ingestion uses (log_source). Metadata only: the connecting
        user is never returned."""
        found = self._list_connectors(destination, stop_after=limit)
        if self._uses_rest:
            self._warn(REST_CONNECTOR_MATCH_NOTE)
        if destination is not None and not found:
            self._warn(
                f"no connector on destination '{destination}' was found; check "
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
        lineage, lineage_source = self._lineage_for(target)
        cap = self._config.max_table_lineage_per_connector
        if lineage_source == "log_database" and len(lineage) >= cap:
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
