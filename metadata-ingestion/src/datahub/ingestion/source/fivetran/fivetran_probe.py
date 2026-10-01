import itertools
import logging
from typing import Callable, Dict, List, Optional, TypeVar

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import (
    ProbeConnectionError,
    ProbeReadFailed,
    ProbeSoftError,
    soft_on_status,
)
from datahub.ingestion.source.fivetran.config import (
    FIVETRAN_CONNECTOR_KIND,
    FIVETRAN_DESTINATION_KIND,
    REST_CONNECTOR_MATCH_NOTE,
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import Connector
from datahub.ingestion.source.fivetran.fivetran_log_db_reader import FivetranLogDbReader
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import FivetranListedConnection

logger = logging.getLogger(__name__)

T = TypeVar("T")


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


class FivetranMetadataProbe:
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

    warnings: List[str]

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
        self.warnings = []

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

    def __enter__(self) -> "FivetranMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        if self._api_client is not None:
            self._api_client._session.close()
        if self._db_reader is not None:
            self._db_reader.engine.dispose()

    @property
    def probe_report(self) -> FivetranSourceReport:
        """The report the reused readers write into, so the warnings they
        record (a truncated lineage, a failed per-table column fetch) reach
        the caller instead of staying in an ingestion-only report."""
        return self._report

    @property
    def _uses_rest(self) -> bool:
        return self._config.log_source == "rest_api"

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

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
                    f"({log_config.destination_platform}): {exc}"
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
        except ValueError as exc:
            raise ProbeReadFailed(f"{context}: {exc}") from exc

    def _group_connections(self, group_id: str) -> List[FivetranListedConnection]:
        with soft_on_status(
            404, context=f"connections listing for destination '{group_id}'"
        ):
            return self._rest(
                f"listing connections of destination '{group_id}'",
                lambda: list(self._api().list_connections(group_id)),
            )

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
                    raise ValueError(
                        f"no Fivetran destination with id '{destination}'; list "
                        f"them with `probe run destinations`"
                    ) from exc
                # A destination deleted between the groups listing and this
                # call. Ingestion skips the group with a warning too.
                self._warn(str(exc))
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
