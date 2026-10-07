"""Qualytics ingestion source.

Walks datastores -> containers and, per container, emits a profile, one assertion per
quality check, and assertion run events from check state and anomalies. URN resolution
lives in `urn_resolver.py`; each mapper owns one aspect family. See
`skill_docs/_PLANNING.md` for the entity mapping.
"""

import contextlib
from collections.abc import Callable, Generator, Iterable
from typing import Any, TypeVar

from pydantic import ValidationError

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qualytics.anomaly import AnomalyMapper
from datahub.ingestion.source.qualytics.assertion import AssertionMapper
from datahub.ingestion.source.qualytics.client import (
    QualyticsApiError,
    QualyticsAuthError,
    QualyticsClient,
)
from datahub.ingestion.source.qualytics.config import QualyticsSourceConfig
from datahub.ingestion.source.qualytics.constants import (
    CONTAINERS_PATH,
    DATASTORES_PATH,
    FIELD_PROFILES_PATH,
    PLATFORM,
    QUALITY_CHECKS_PATH,
)
from datahub.ingestion.source.qualytics.links import container_url, derive_ui_base_url
from datahub.ingestion.source.qualytics.models import (
    Anomaly,
    Container,
    ContainerProfile,
    Datastore,
    FieldProfile,
    QualityCheck,
    parse_container,
    parse_datastore,
)
from datahub.ingestion.source.qualytics.profile import ProfileMapper
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport
from datahub.ingestion.source.qualytics.urn_resolver import UrnResolver
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.workunit_processors import AutoLowercaseUrnsProcessor

_T = TypeVar("_T")


@platform_name("Qualytics", id=PLATFORM)
@config_class(QualyticsSourceConfig)
@support_status(SupportStatus.BETA)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Enabled via datastore_to_platform_map",
)
@capability(
    SourceCapability.TEST_CONNECTION,
    "Enabled by default",
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Qualytics container and field profiles, when emit_profiles is enabled",
)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Quality check descriptions become assertion descriptions",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
    supported=True,
)
class QualyticsSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests Qualytics data-quality metadata into DataHub.

    Quality checks become ``Assertion`` entities, anomalies become
    ``AssertionRunEvent`` results, and Qualytics profiles become dataset and field
    profiles. These attach to the datasets the customer's warehouse or lake source
    already emitted, rather than to a parallel Qualytics catalog.
    """

    report: QualyticsSourceReport

    def __init__(self, config: QualyticsSourceConfig, ctx: PipelineContext) -> None:
        # StatefulIngestionSourceBase declares its config as
        # StatefulIngestionConfigBase[StatefulIngestionConfig]. That generic is
        # invariant, so parameterizing ours with the (narrower)
        # StatefulStaleMetadataRemovalConfig -- which is what stale entity removal
        # actually needs -- is not assignable, despite being a subclass. Upstream
        # sources dodge this by leaving the base unparameterized; we keep the precise
        # type and ignore here instead.
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = QualyticsSourceReport()
        self.client = QualyticsClient(config)
        self.ui_base_url = config.ui_base_url or derive_ui_base_url(config.base_url)
        self.resolver = UrnResolver(config, self.report)
        self.assertions = AssertionMapper(config.platform_instance, self.report)
        self.profiles = ProfileMapper(self.report)
        self.anomalies = AnomalyMapper(self.report)
        self._existence_check_broken = False
        self._seen_datastore_keys: set[str] = set()

    @classmethod
    def create(
        cls, config_dict: dict[str, Any], ctx: PipelineContext
    ) -> "QualyticsSource":
        # model_validate, not parse_obj: DataHub is on pydantic v2 and treats the v1
        # methods as deprecated. Some in-tree sources still call parse_obj; do not
        # copy them.
        config = QualyticsSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Walk datastores -> containers, emitting what each container supports.

        Ordered by datastore so a single unresolvable one is reported once and its
        containers skipped as a group, rather than warning per container.

        The walk always continues past a bad item: one malformed container must not
        cost a customer every other container's metadata. Auth failures are the
        exception -- they propagate, because every subsequent request would fail the
        same way and the warning storm would bury the cause.

        What decides warning versus failure is stale-entity removal. Anything that
        leaves this run's set of assertions incomplete -- a listing that failed, an
        object that would not parse -- is a *failure*, because DataHub's stale-entity
        handler skips soft-deletion only when the source reports one; as a warning, a
        single 500 on one container's quality-check listing would soft-delete every
        assertion on it. Profiles, anomalies and existence checks do not feed that
        state, so their problems stay warnings.
        """
        self.report.qualytics_version = self.client.get_version()
        if self.report.qualytics_version is None:
            self.report.info(
                title="Qualytics version unknown",
                message="The deployment's openapi.json could not be read, so this run's "
                "report does not record which Qualytics build it talked to.",
            )

        for payload in self.client.list_datastores():
            parsed = self._parse(parse_datastore, payload, "datastore", incomplete=True)
            if parsed is None:
                continue
            datastore, recognised = parsed
            self._seen_datastore_keys.update((datastore.name, str(datastore.id)))
            if not self.config.datastore_pattern.allowed(datastore.name):
                self.report.datastores_dropped += 1
                continue
            if not recognised:
                self.report.datastores_unrecognised += 1
                self.report.warning(
                    title="Unrecognised Qualytics datastore type",
                    message=(
                        "This datastore's containers will be skipped. The store type "
                        "is newer than this build of the connector."
                    ),
                    context=f"datastore={datastore.name}, store_type={datastore.store_type}",
                )
                continue

            self.report.datastores_scanned += 1

            # Resolve once per datastore. The resolver caches, so an unresolvable one
            # warns a single time no matter how many containers hang off it.
            if self.resolver.resolve_platform(datastore) is None:
                continue

            yield from self._process_datastore(datastore)

        self._report_unmatched_map_keys()

    def _report_unmatched_map_keys(self) -> None:
        """Warn once per datastore_to_platform_map key that matched no datastore.

        A typo in a key is otherwise silent: the datastore falls back to inference,
        with no platform instance, and its assertions land on URNs nothing emitted.
        """
        for key in self.config.datastore_to_platform_map:
            if key not in self._seen_datastore_keys:
                self.report.warning(
                    title="datastore_to_platform_map entry matched no datastore",
                    message="No datastore in this deployment has this name or id. "
                    "Check the key for typos; a renamed datastore needs its new name "
                    "or its numeric id.",
                    context=f"key={key}",
                )

    def _process_datastore(self, datastore: Datastore) -> Iterable[MetadataWorkUnit]:
        # The listing generator is inside the try on purpose: it raises lazily, at the
        # `for`, so a failure paging container 7 of 40 would otherwise escape a handler
        # placed around the loop body and abort every remaining datastore.
        try:
            yield from self._walk_containers(datastore)
        except QualyticsAuthError:
            raise
        except QualyticsApiError as e:
            self.report.failure(
                title="Failed to list a datastore's containers",
                message="The rest of the datastore is skipped, and stale-assertion removal "
                "is skipped for this run so its assertions are not deleted.",
                context=f"datastore={datastore.name}",
                exc=e,
            )

    def _walk_containers(self, datastore: Datastore) -> Iterable[MetadataWorkUnit]:
        for payload in self.client.list_containers(datastore_id=datastore.id):
            parsed = self._parse(parse_container, payload, "container", incomplete=True)
            if parsed is None:
                continue
            container, recognised = parsed
            if not self.config.container_pattern.allowed(container.name):
                self.report.containers_dropped += 1
                continue
            if not recognised:
                self.report.containers_unrecognised += 1
                self.report.warning(
                    title="Unrecognised Qualytics container type",
                    message=(
                        "The container will be skipped. The container type is newer "
                        "than this build of the connector."
                    ),
                    context=(
                        f"datastore={datastore.name}, container={container.name}, "
                        f"container_type={container.container_type}"
                    ),
                )
                continue

            self.report.containers_scanned += 1

            try:
                yield from self._process_container(datastore, container)
            except QualyticsAuthError:
                raise
            except Exception as e:
                # Profiles and anomalies have their own handlers, so what reaches here
                # left the container's assertions incomplete: its quality-check listing
                # failed, or something unexpected did. Broad on purpose -- one bad value
                # must not abort every remaining datastore.
                self.report.containers_failed += 1
                self.report.failure(
                    title="Failed to process a container",
                    message="Its assertions may be incomplete, so stale-assertion removal "
                    "is skipped for this run. Other containers are unaffected.",
                    context=f"datastore={datastore.name}, container={container.name}",
                    exc=e,
                )

    def _parse(
        self,
        parser: Callable[[dict[str, Any]], _T],
        payload: dict[str, Any],
        kind: str,
        incomplete: bool = False,
    ) -> _T | None:
        """Parse one API payload, or report and skip it.

        Unknown *enum* values are handled inside the models, but a payload missing a
        required field still raises. Every listed object goes through here so a bad one
        costs only itself.

        ``incomplete`` marks objects whose loss leaves assertions unaccounted for --
        datastores, containers, quality checks. Those are failures, so stale-entity
        removal does not delete what could not be read.
        """
        try:
            return parser(payload)
        except ValidationError as e:
            self.report.items_unparseable += 1
            context = f"kind={kind}, id={payload.get('id')}"
            if incomplete:
                self.report.failure(
                    title="Skipped an unparseable Qualytics object",
                    message="Its payload did not match the expected shape, so it was "
                    "skipped, and stale-assertion removal is skipped for this run.",
                    context=context,
                    exc=e,
                )
            else:
                self.report.warning(
                    title="Skipped an unparseable Qualytics object",
                    message="Its payload did not match the expected shape, so it was "
                    "skipped. Other objects are unaffected.",
                    context=context,
                    exc=e,
                )
            return None

    def _process_container(
        self, datastore: Datastore, container: Container
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self.resolver.dataset_urn(datastore, container)
        if dataset_urn is None:
            return

        external_url = container_url(self.ui_base_url, datastore.id, container.id)

        if self.config.emit_profiles:
            try:
                yield from self._emit_profile(dataset_urn, datastore, container)
            except QualyticsAuthError:
                raise
            except (QualyticsApiError, ValidationError) as e:
                # Scoped tightly: a profile that will not load says nothing about the
                # container's assertions, and failing the whole container here would
                # lose them for an unrelated reason.
                self.report.profiles_failed += 1
                self.report.warning(
                    title="Failed to fetch a container profile",
                    message="Profiles are skipped for this container; assertions still run.",
                    context=f"container={container.name}",
                    exc=e,
                )

        if not self.config.emit_assertions:
            return

        assertion_urns = yield from self._emit_assertions(
            dataset_urn, datastore, container, external_url
        )

        if self.config.emit_assertion_results and assertion_urns:
            try:
                yield from self._emit_anomalies(dataset_urn, container, assertion_urns)
            except QualyticsAuthError:
                raise
            except (QualyticsApiError, ValidationError) as e:
                # The assertions and their current verdicts are already out; only the
                # failure history is missing, and it does not feed stale-entity state.
                self.report.anomaly_listings_failed += 1
                self.report.warning(
                    title="Failed to list a container's anomalies",
                    message="Its assertion results history is incomplete for this run; "
                    "assertions and current verdicts were emitted.",
                    context=f"container={container.name}",
                    exc=e,
                )

    def _emit_profile(
        self, dataset_urn: str, datastore: Datastore, container: Container
    ) -> Iterable[MetadataWorkUnit]:
        if not self._dataset_in_datahub(dataset_urn):
            return

        payload = self.client.get_container_profile(container.id)
        if payload is None:
            # Never profiled. Normal for a newly catalogued container, not an error.
            self.report.containers_unprofiled += 1
            return

        container_profile = ContainerProfile.model_validate(payload)
        # Renamed here, not in the mapper: the name is DataHub's fieldPath, and whether
        # it is lowercased is a URN-resolution decision the resolver owns.
        field_profiles = [
            fp.model_copy(update={"name": self.resolver.field_path(datastore, fp.name)})
            for raw in self.client.list_container_field_profiles(container.id)
            if (fp := self._parse(FieldProfile.model_validate, raw, "field profile"))
        ]

        yield from self.profiles.workunits(
            dataset_urn, container_profile, field_profiles
        )

    def _dataset_in_datahub(self, dataset_urn: str) -> bool:
        """Whether a profile for this dataset may be written.

        Assertions and run events only *reference* the dataset, so they are safe either
        way. A datasetProfile is an aspect *of* the dataset, and DataHub creates the
        entity to hold it: a stub dataset with nothing but our profile in it.

        Fails closed, unlike dbt's equivalent check: a skipped profile comes back on the
        next run, a stub dataset stays. Without a DataHub connection there is nothing
        to ask, so the profile is emitted and counted as unchecked.
        """
        graph = self.ctx.graph
        if graph is None:
            self.report.profiles_existence_unchecked += 1
            return True

        if self._existence_check_broken:
            self.report.profiles_existence_check_failed += 1
            return False

        try:
            exists = graph.exists(dataset_urn)
        except Exception as e:
            # Reported once, then assumed broken for the run: an outage would otherwise
            # cost one timeout and one identical warning per dataset. Counted apart
            # from missing datasets, which need a different fix.
            self._existence_check_broken = True
            self.report.profiles_existence_check_failed += 1
            self.report.warning(
                title="Could not check whether datasets exist in DataHub",
                message="Profiles are skipped for the rest of this run rather than risk "
                "creating datasets. Assertions are unaffected.",
                context=f"first failure at dataset={dataset_urn}",
                exc=e,
            )
            return False

        if not exists:
            self.report.profiles_skipped_dataset_missing += 1
            self.report.datasets_missing.append(dataset_urn)
            return False
        return True

    def _emit_assertions(
        self,
        dataset_urn: str,
        datastore: Datastore,
        container: Container,
        external_url: str,
    ) -> Generator[MetadataWorkUnit, None, dict[int, str]]:
        """Emit an assertion per quality check; return check id -> assertion URN.

        The mapping is what lets anomalies attach their results to the right
        assertion without refetching or guessing.
        """
        assertion_urns: dict[int, str] = {}

        for payload in self.client.list_quality_checks(container_id=container.id):
            check = self._parse(
                QualityCheck.model_validate, payload, "quality check", incomplete=True
            )
            if check is None:
                continue
            self.report.quality_checks_scanned += 1

            assertion_urn = self.assertions.assertion_urn(check.id)
            assertion_urns[check.id] = assertion_urn

            yield from self.assertions.workunits(
                check,
                dataset_urn,
                field_urns=self._field_urns(dataset_urn, datastore, check),
                external_url=external_url,
            )

            if self.config.emit_assertion_results:
                yield from self.anomalies.check_state_workunits(
                    check, assertion_urn, dataset_urn
                )

        return assertion_urns

    def _emit_anomalies(
        self,
        dataset_urn: str,
        container: Container,
        assertion_urns: dict[int, str],
    ) -> Iterable[MetadataWorkUnit]:
        window = self.config.assertion_results
        for payload in self.client.list_anomalies(
            container_id=container.id,
            start_date=window.start_time.date().isoformat(),
            end_date=window.end_time.date().isoformat(),
        ):
            anomaly = self._parse(Anomaly.model_validate, payload, "anomaly")
            if anomaly is None:
                continue
            self.report.anomalies_scanned += 1
            yield from self.anomalies.anomaly_workunits(
                anomaly, assertion_urns, dataset_urn
            )

    def _field_urns(
        self, dataset_urn: str, datastore: Datastore, check: QualityCheck
    ) -> list[str]:
        """schemaField URNs for the columns a check covers.

        Built as strings rather than looked up: we do not emit schemaMetadata (the
        warehouse source owns it), so there is nothing local to resolve against. A
        field path that does not exist on their schema simply renders unlinked -- which
        is why its casing has to follow the warehouse source's, like the dataset name.
        """
        return [
            make_schema_field_urn(
                dataset_urn, self.resolver.field_path(datastore, field.name)
            )
            for field in check.fields
        ]

    def get_excluded_workunit_processors(self) -> list[Any]:
        # The resolver owns URN casing, per datastore. The framework's processor would
        # lowercase every dataset URN in the stream whenever the recipe sets
        # convert_urns_to_lowercase, overriding a map entry that turned it off -- and
        # leave column paths cased while lowercasing their dataset.
        #
        # Stale-entity removal is NOT wired here: the framework adds
        # AutoStaleEntityRemovalProcessor for any stateful source, and adding a second
        # handler made removal run twice.
        return [AutoLowercaseUrnsProcessor]

    def get_report(self) -> QualyticsSourceReport:
        return self.report

    def close(self) -> None:
        self.client.close()
        super().close()

    # --- test_connection ----------------------------------------------------------

    @staticmethod
    def test_connection(config_dict: dict[str, Any]) -> TestConnectionReport:
        """Validate a recipe before a full run.

        Surfaced by `datahub ingest --test-source-connection` and by the Test
        Connection button in the DataHub UI. Basic connectivity is reported first, then
        each capability separately, so a user sees exactly which part is broken rather
        than one opaque failure.
        """
        test_report = TestConnectionReport()
        client: QualyticsClient | None = None

        try:
            config = QualyticsSourceConfig.model_validate(config_dict)
            client = QualyticsClient(config)

            test_report.basic_connectivity = QualyticsSource._test_connectivity(client)
            if not test_report.basic_connectivity.capable:
                return test_report

            capability_report: dict[SourceCapability | str, CapabilityReport] = {
                SourceCapability.DESCRIPTIONS: QualyticsSource._test_read(
                    client, QUALITY_CHECKS_PATH, "quality checks"
                ),
            }
            if config.emit_profiles:
                capability_report[SourceCapability.DATA_PROFILING] = (
                    QualyticsSource._test_read(
                        client, FIELD_PROFILES_PATH, "field profiles"
                    )
                )
            test_report.capability_report = capability_report

        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        finally:
            if client is not None:
                client.close()

        return test_report

    @staticmethod
    def _test_connectivity(client: QualyticsClient) -> CapabilityReport:
        """Reach the deployment and authenticate, cheaply.

        One page of one datastore, not a full listing: test_connection has to stay
        fast. The root-path check runs here because pointing `base_url` at the
        deployment origin without its `/api` suffix is the most likely recipe error,
        and it otherwise shows up as a successful run that ingests nothing.
        """
        try:
            # Datastores and containers are both core reads -- nothing works without
            # them -- so they belong in connectivity rather than a feature capability.
            client.get(DATASTORES_PATH, params={"page": 1, "size": 1})
            client.get(CONTAINERS_PATH, params={"page": 1, "size": 1})
        except QualyticsAuthError as e:
            return CapabilityReport(capable=False, failure_reason=str(e))
        except QualyticsApiError as e:
            root_path_problem = None
            # Best-effort diagnosis: if the spec is unreachable too, we still want to
            # report the original failure rather than mask it with a second one.
            with contextlib.suppress(QualyticsApiError):
                root_path_problem = client.check_base_url_root_path()
            reason = f"{root_path_problem} ({e})" if root_path_problem else str(e)
            return CapabilityReport(capable=False, failure_reason=reason)

        mismatch = client.check_base_url_root_path()
        if mismatch:
            return CapabilityReport(capable=False, failure_reason=mismatch)

        return CapabilityReport(capable=True)

    @staticmethod
    def _test_read(client: QualyticsClient, path: str, what: str) -> CapabilityReport:
        """Confirm the token can read one endpoint, reported per capability."""
        try:
            client.get(path, params={"page": 1, "size": 1})
        except QualyticsApiError as e:
            return CapabilityReport(
                capable=False,
                failure_reason=f"Cannot read {what} ({path}): {e}",
            )
        return CapabilityReport(capable=True)
