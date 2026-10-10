from typing import (
    Callable,
    ClassVar,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Type,
    Union,
)

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    soft_listing,
    take,
)
from datahub.ingestion.agent.verdicts import ProbeConnectionError, ProbeReadFailed
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    DatasetContainerSubTypes,
)
from datahub.ingestion.source.superset import (
    PAGE_SIZE,
    SupersetConfig,
    SupersetSource,
    SupersetSourceReport,
)
from datahub.ingestion.source.superset_selection import SUPERSET_DATASET_KIND

# One listing record: the name a pattern is matched against, plus scalar facts
# `probe filter --from-run` hands back as attributes.
ProbeRecord = Dict[str, Union[str, int]]


class SupersetMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over Superset's REST API, for `superset` and, through
    PresetMetadataProbe, `preset`.

    Logs in with the source's own login(), on an uninitialised source: the
    source's __init__ also fetches every owner's email (parse_owner_info),
    which a probe has no use for and must not read. Listings page the same
    endpoints ingestion pages, but fail on an HTTP error rather than stopping
    with a warning, as paginate_entity_api_results does, so a refused listing
    can never read as an empty one.

    Records carry names and ids only. Owners and changed_by, which every list
    payload holds with names and ids of people, are never returned.
    """

    source_class: ClassVar[Type[SupersetSource]] = SupersetSource

    def __init__(self, config: SupersetConfig) -> None:
        self._config = config
        self._shell: Optional[SupersetSource] = None
        self._unnamed: Dict[str, int] = {}

    @classmethod
    def for_config(cls, config: SupersetConfig) -> "SupersetMetadataProbe":
        return cls(config)

    @property
    def probe_report(self) -> object:
        """The report get_dataset_info writes into: a dataset whose database is
        looked up and could not be read surfaces as a warning, not as a
        dataset with no database."""
        return self._shell.report if self._shell is not None else None

    def _source(self) -> SupersetSource:
        return self._open_once(
            "source", self._log_in, close=lambda source: source.session.close()
        )

    def _log_in(self) -> SupersetSource:
        source = self.source_class.__new__(self.source_class)
        source.config = self._config
        source.report = SupersetSourceReport()
        source._dataset_info_cache = {}
        self._shell = source
        try:
            source.session = source.login()
        except (KeyError, TypeError) as exc:
            # login() reads the token straight off the response body, so a
            # refused login surfaces as a missing key rather than an HTTP error.
            raise ProbeConnectionError(
                f"logging in to {self.source_class.platform} returned no access "
                f"token; check the recipe's credentials"
            ) from exc
        return source

    def _paged(self, entity: str) -> Iterator[Mapping[str, object]]:
        """Every record of one list endpoint, with ingestion's endpoint, query
        and page size."""
        source = self._source()
        page = 0
        while True:
            response = source.session.get(
                f"{self._config.connect_uri}/api/v1/{entity}/",
                params={"q": f"(page:{page},page_size:{PAGE_SIZE})"},
                timeout=self._config.timeout,
            )
            response.raise_for_status()
            payload = response.json()
            results = payload.get("result") if isinstance(payload, dict) else None
            if not isinstance(results, list):
                # Not an empty listing: the answer is not one Superset gives.
                raise ProbeReadFailed(
                    f"the {entity} listing answered without a result list"
                )
            for item in results:
                if isinstance(item, dict):
                    yield item
            page += 1
            count = payload.get("count")
            if not results or not isinstance(count, int) or page * PAGE_SIZE >= count:
                return

    def _listing(
        self,
        entity: str,
        build: Callable[[Mapping[str, object]], Optional[ProbeRecord]],
        limit: int,
    ) -> List[ProbeRecord]:
        """One listing, built record by record. A 403 or 404 on the endpoint
        (a role without read access to it) is a warning and an empty result;
        a refused login, another HTTP error or an unreachable host fails."""
        with soft_listing(self._warn, 403, 404, context=f"the {entity} listing"):
            records = take(
                (r for r in map(build, self._paged(entity)) if r is not None), limit
            )
            self._warn_unnamed(entity)
            return records
        return []

    def _named(
        self, entity: str, item: Mapping[str, object], key: str
    ) -> Optional[str]:
        """The record's name, or None (counted) when it has none: ingestion
        drops such a record when its pattern check fails on the missing name."""
        name = item.get(key)
        if isinstance(name, str):
            return name
        self._unnamed[entity] = self._unnamed.get(entity, 0) + 1
        return None

    def _warn_unnamed(self, entity: str) -> None:
        count = self._unnamed.get(entity)
        if count:
            self._warn(
                f"{count} {entity} record(s) had no name and were left out; "
                f"ingestion drops them too"
            )

    @probe_method(kind=DatasetContainerSubTypes.DATABASE, row_limit_param="limit")
    def databases(self, limit: int = 200) -> List[ProbeRecord]:
        """Database connections this workspace defines, by database_name, with
        their backend (Superset's engine name, e.g. "postgresql"). Includes ones
        database_pattern would exclude. Superset ingests no database entity:
        database_pattern drops the datasets reading from a database, so judge a
        database here to learn whether its datasets are kept. Metadata only."""

        def build(item: Mapping[str, object]) -> Optional[ProbeRecord]:
            name = self._named("database", item, "database_name")
            if name is None:
                return None
            record: ProbeRecord = {"name": name}
            _copy_scalar(item, "id", record)
            _copy_scalar(item, "backend", record)
            return record

        return self._listing("database", build, limit)

    @probe_method(kind=SUPERSET_DATASET_KIND, row_limit_param="limit")
    def datasets(self, limit: int = 200) -> List[ProbeRecord]:
        """Datasets by table_name, the string dataset_pattern is matched
        against, each with its database and schema. Includes ones the recipe
        would exclude. `probe filter --from-run` reads each record's database,
        since database_pattern drops a dataset by it. Datasets are ingested only
        when ingest_datasets is set. Metadata only."""

        def build(item: Mapping[str, object]) -> Optional[ProbeRecord]:
            name = self._named("dataset", item, "table_name")
            if name is None:
                return None
            record: ProbeRecord = {"name": name}
            database = self._database_of(item)
            if database:
                record["database"] = database
            _copy_scalar(item, "schema", record)
            _copy_scalar(item, "id", record)
            return record

        return self._listing("dataset", build, limit)

    @probe_method(kind=BIAssetSubTypes.CHART, row_limit_param="limit")
    def charts(self, limit: int = 200) -> List[ProbeRecord]:
        """Charts by slice_name, the string chart_pattern is matched against,
        with their id (a chart's URN is built from it) and viz_type. Includes
        ones chart_pattern would exclude. database_pattern and dataset_pattern
        never drop a chart: ingestion only warns about one reading a dropped
        dataset. Metadata only."""

        def build(item: Mapping[str, object]) -> Optional[ProbeRecord]:
            name = self._named("chart", item, "slice_name")
            if name is None:
                return None
            record: ProbeRecord = {"name": name}
            _copy_scalar(item, "id", record)
            _copy_scalar(item, "viz_type", record)
            return record

        return self._listing("chart", build, limit)

    @probe_method(kind=BIAssetSubTypes.DASHBOARD, row_limit_param="limit")
    def dashboards(self, limit: int = 200) -> List[ProbeRecord]:
        """Dashboards by dashboard_title, the string dashboard_pattern is
        matched against, with their id (a dashboard's URN is built from it).
        Includes ones dashboard_pattern would exclude. Metadata only."""

        def build(item: Mapping[str, object]) -> Optional[ProbeRecord]:
            name = self._named("dashboard", item, "dashboard_title")
            if name is None:
                return None
            record: ProbeRecord = {"name": name}
            _copy_scalar(item, "id", record)
            return record

        return self._listing("dashboard", build, limit)

    def _database_of(self, item: Mapping[str, object]) -> Optional[str]:
        """The dataset's database_name: from the list record, which carries it,
        else from the dataset detail ingestion reads it from. A failed detail
        read reaches the caller as a warning through probe_report."""
        database = item.get("database")
        if isinstance(database, dict):
            name = database.get("database_name")
            if isinstance(name, str):
                return name
        dataset_id = item.get("id")
        if not isinstance(dataset_id, int):
            return None
        detail = self._source().get_dataset_info(dataset_id)
        name = detail.get("result", {}).get("database", {}).get("database_name")
        return name if isinstance(name, str) else None


def _copy_scalar(item: Mapping[str, object], key: str, record: ProbeRecord) -> None:
    value = item.get(key)
    # bool is an int: a flag is not an id.
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        record[key] = value
