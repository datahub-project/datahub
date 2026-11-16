from typing import Callable, Dict, Iterable, List, Optional, TypeVar

from azure.core.credentials import TokenCredential
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.mgmt.datafactory.models import Activity, Factory, PipelineResource

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    echoed,
    resolve_name,
    soft_listing,
    take,
)
from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.azure.constants import ADF_LINKED_SERVICE_PLATFORM_MAP
from datahub.ingestion.source.azure_data_factory.adf_client import (
    AzureDataFactoryClient,
)
from datahub.ingestion.source.azure_data_factory.adf_config import (
    ADF_ACTIVITY_KIND,
    ADF_PIPELINE_KIND,
    AzureDataFactoryConfig,
)
from datahub.ingestion.source.azure_data_factory.adf_report import (
    AzureDataFactorySourceReport,
)
from datahub.ingestion.source.azure_data_factory.adf_source import (
    ACTIVITY_SUBTYPE_MAP,
    AzureDataFactorySource,
)
from datahub.ingestion.source.common.subtypes import FlowContainerSubTypes

_T = TypeVar("_T")


def _activity_record(activity: Activity) -> Dict[str, object]:
    # An allowlist, not a dump: type properties hold Web activity headers and
    # auth, Script/Copy SQL text and stored-procedure parameters.
    activity_type = activity.type or "Unknown"
    return {
        # "Unknown" as _create_datajob names it, so this is the emitted DataJob name.
        "name": activity.name or "Unknown",
        "type": activity_type,
        "subtype": str(ACTIVITY_SUBTYPE_MAP.get(activity_type, activity_type)),
        "depends_on": [d.activity for d in activity.depends_on or []],
        "inputs": [r.reference_name for r in getattr(activity, "inputs", None) or []],
        "outputs": [r.reference_name for r in getattr(activity, "outputs", None) or []],
    }


def _unresolved_reason(
    ls_name: Optional[str], ls_type: Optional[str], found: bool, listed: bool
) -> str:
    # _extract_table_name falls back to the ADF dataset name, so once a mapped
    # platform is known _resolve_dataset_urn always returns a URN: these are
    # the only ways it returns None.
    if not ls_name:
        return "no linked service reference"
    if not listed:
        return "this factory's linked services could not be listed"
    if not found:
        return f"linked service {echoed(ls_name)} not found in this factory"
    if not ls_type:
        return f"linked service {echoed(ls_name)} has no type"
    return f"linked service type {echoed(ls_type)} has no DataHub platform mapping"


def source_for_probe(
    config: AzureDataFactoryConfig, client: AzureDataFactoryClient
) -> AzureDataFactorySource:
    """An uninitialized source for the probe, which needs a few of the
    source's helpers but must not run __init__ (stateful ingestion, telemetry).

    Primes only what _extract_resource_group, _extract_table_name and
    _resolve_dataset_urn read. If one of those starts reading a new attribute,
    prime it here too, or the probe fails with AttributeError (reported as a
    defect, exit 1); the parity test against get_workunits_internal is what
    notices.
    """
    source = AzureDataFactorySource.__new__(AzureDataFactorySource)
    source.config = config
    source.report = AzureDataFactorySourceReport()
    source.client = client
    source._datasets_cache = {}
    source._linked_services_cache = {}
    return source


class AzureDataFactoryMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over Azure Data Factory's management API.

    Every record is an allowlist projection of the SDK model. ADF keeps
    credentials inside the objects this lists -- a linked service's
    connection string, an HTTP dataset's headers, a Web activity's auth, a
    factory's global parameters -- and register_secrets only masks secrets
    that came from the recipe, so nothing else would catch them.

    Reuses the connector's fetch, never its policy: no getter applies
    factory_pattern or pipeline_pattern, so a denied name is reported for
    `probe filter` to explain rather than hidden.
    """

    def __init__(
        self, source: AzureDataFactorySource, credential: TokenCredential
    ) -> None:
        self._source = source
        self._config: AzureDataFactoryConfig = source.config
        self._client: AzureDataFactoryClient = source.client
        # Run last-in first-out: the client, then the credential. Ingestion
        # never closes the credential; a probe is a short-lived process, and
        # closing it releases the token client's transport.
        close_credential = getattr(credential, "close", None)
        if callable(close_credential):
            self._on_exit(close_credential)
        self._on_exit(self._client.close)

    @classmethod
    def for_config(
        cls, config: AzureDataFactoryConfig
    ) -> "AzureDataFactoryMetadataProbe":
        # The two calls AzureDataFactorySource.__init__ makes, so auth resolves
        # exactly as ingestion resolves it. Neither contacts Azure: tokens are
        # acquired on the first request.
        credential = config.credential.get_credential()
        client = AzureDataFactoryClient(
            credential=credential, subscription_id=config.subscription_id
        )
        return cls(source_for_probe(config, client), credential)

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """Azure's error code (`AuthorizationFailed`, `ResourceGroupNotFound`),
        which names the refused part of the scope where the HTTP status alone
        does not. None when the reply carried no ARM error body."""
        if not isinstance(exc, HttpResponseError) or exc.error is None:
            return None
        code = exc.error.code
        return code if isinstance(code, str) else None

    @property
    def probe_report(self) -> AzureDataFactorySourceReport:
        """_resolve_dataset_urn records unmapped platforms here with
        report.warning(); exposing it folds those into the result."""
        return self._source.report

    @probe_method(kind=FlowContainerSubTypes.ADF_DATA_FACTORY, row_limit_param="limit")
    def factories(self, limit: int = 200) -> List[Dict[str, object]]:
        """Data Factories in this subscription, including ones factory_pattern
        would exclude -- a denied factory is reported, not hidden, so `probe
        filter --kind "Data Factory"` can explain it. When the recipe sets
        resource_group, only that resource group is listed, as ingestion lists
        it. Each record is name, resource group and region; factory global
        parameters are withheld because their values can be secrets."""
        # Not soft: a failure here means nothing could be read, which is exit 3.
        rg = self._config.resource_group
        nameless = 0

        def addressable(factory: Factory) -> bool:
            nonlocal nameless
            if factory.name and factory.id:
                return True
            nameless += 1
            return False

        listed = take(
            self._client.get_factories(resource_group=rg), limit, keep=addressable
        )
        out: List[Dict[str, object]] = [
            {
                "name": factory.name,
                "resource_group": self._source._extract_resource_group(
                    factory.id or ""
                ),
                "location": factory.location,
            }
            for factory in listed
        ]
        if nameless:
            self._warn(
                f"{nameless} factor{'y' if nameless == 1 else 'ies'} had no name "
                f"or id and were skipped, as ingestion skips them"
            )
        if rg:
            self._warn(
                f"resource_group is set to {echoed(rg)}, so only that resource group "
                f"was listed; factories elsewhere in the subscription are absent "
                f"rather than reported excluded, and ingestion will not see them"
            )
        return out

    def _resource_group_of(self, factory: str) -> str:
        """The resource group a factory lives in, from one factory listing.

        Narrowed by resource_group as ingestion narrows it, so a factory the
        recipe cannot see is out of scope here too rather than silently reached.
        """
        rg = self._config.resource_group
        where = (
            f"in resource group {echoed(rg)} (the recipe's resource_group)"
            if rg
            else f"in subscription {echoed(self._config.subscription_id)}"
        )
        found = resolve_name(
            factory,
            (
                f
                for f in self._client.get_factories(resource_group=rg)
                if f.name and f.id
            ),
            key=lambda f: f.name or "",
            kind="data factory",
            where=where,
            list_command="probe run factories",
            distinguish=lambda f: self._source._extract_resource_group(f.id or ""),
            on_ambiguous="set resource_group in the recipe",
        ).record
        return self._source._extract_resource_group(found.id or "")

    def _try_listing(
        self,
        context: str,
        items: Callable[[], Iterable[_T]],
        limit: Optional[int],
    ) -> Optional[List[_T]]:
        """Named items from one per-factory listing, or None when it could not
        be read: a 403/404 becomes None plus a warning -- "could not look",
        never "nothing here".

        The soft_listing block wraps the iteration, not just the call: an
        Azure pager raises HttpResponseError while being iterated.
        """
        with soft_listing(self._warn, 403, 404, context=context):
            return self._named(context, items(), limit)
        return None

    def _named(
        self, context: str, items: Iterable[_T], limit: Optional[int]
    ) -> List[_T]:
        """Items with a name, up to `limit`. Nameless items are skipped as
        ingestion skips them, and counted so the gap is visible."""
        nameless = 0

        def named(item: _T) -> bool:
            nonlocal nameless
            if getattr(item, "name", None):
                return True
            nameless += 1
            return False

        out = take(items, limit, keep=named)
        if nameless:
            self._warn(
                f"{context}: {nameless} item(s) had no name and were skipped, "
                f"as ingestion skips them"
            )
        return out

    def _listing(
        self,
        context: str,
        items: Callable[[], Iterable[_T]],
        limit: Optional[int],
    ) -> List[_T]:
        listed = self._try_listing(context, items, limit)
        return listed if listed is not None else []

    @probe_method(
        kind=ADF_PIPELINE_KIND, row_limit_param="limit", parent_params=("factory",)
    )
    def pipelines(self, factory: str, limit: int = 200) -> List[Dict[str, object]]:
        """Pipelines in one factory, by factory name, including ones
        pipeline_pattern would exclude. pipeline_pattern is matched against
        the bare pipeline name, not factory-qualified, so a same-named
        pipeline in another factory gets the same verdict. Each record is
        name, folder and activity count; parameter defaults are withheld
        because they can hold secrets. Listing pipelines does not prove the
        factory is ingested: ingestion skips the whole factory when it cannot
        list its triggers or, with include_lineage on, its datasets, linked
        services or data flows, and this command reads none of those."""
        rg = self._resource_group_of(factory)
        pipelines = self._listing(
            f"pipelines listing for factory {echoed(factory)}",
            lambda: self._client.get_pipelines(rg, factory),
            limit,
        )
        return [
            {
                "name": p.name,
                "folder": p.folder.name if p.folder else None,
                "activity_count": len(p.activities or []),
            }
            for p in pipelines
        ]

    @probe_method(kind=ADF_ACTIVITY_KIND, parent_params=("factory", "pipeline"))
    def activities(self, factory: str, pipeline: str) -> List[Dict[str, object]]:
        """All activities of one pipeline, by factory and pipeline name --
        exactly the ones ingestion emits as DataJobs, each with the DataHub
        subtype it gets. Nothing filters activities; they follow their
        pipeline's verdict. Includes activities nested inside
        ForEach/IfCondition/Until/Switch control activities, traversed the
        same way ingestion does (AzureDataFactorySource._walk_activities),
        since those get DataJobs too. inputs/outputs are ADF dataset names;
        resolve them with `datasets`. Activity settings (URLs, headers, SQL,
        parameters) are withheld."""
        resource = self._pipeline(factory, pipeline)
        if resource is None:
            return []
        return [
            _activity_record(activity)
            for activity in self._source._walk_activities(resource.activities or [])
        ]

    def _pipeline(self, factory: str, pipeline: str) -> Optional[PipelineResource]:
        """One pipeline by name, or None when reading it is forbidden (a 403
        degrades with a warning). A pipeline the factory does not have is
        the caller's error."""
        rg = self._resource_group_of(factory)
        try:
            with soft_listing(
                self._warn,
                403,
                context=f"pipeline {echoed(pipeline)} in factory {echoed(factory)}",
            ):
                return self._client.get_pipeline(rg, factory, pipeline)
        except ResourceNotFoundError as exc:
            raise ProbeArgumentError(
                f"no pipeline named {echoed(pipeline)} in data factory {echoed(factory)}"
            ) from exc
        return None

    def _note_lineage_off(self) -> None:
        if not self._config.include_lineage:
            self._warn(
                "include_lineage is false, so ingestion reads neither datasets "
                "nor linked services and emits no dataset lineage; these are "
                "listed for inspection only"
            )

    @probe_method(row_limit_param="limit")
    def linked_services(
        self, factory: str, limit: int = 200
    ) -> List[Dict[str, object]]:
        """Linked services (ADF's connections) in one factory, by factory name,
        each with the DataHub platform ingestion maps its type to -- null means
        no lineage resolves through it -- and the platform_instance that
        platform_instance_map assigns it (keyed by linked-service name). The
        connection definition itself (connection strings, hosts, users, keys,
        Key Vault references, encrypted credentials) is always withheld: ADF
        stores credentials there."""
        rg = self._resource_group_of(factory)
        services = self._try_listing(
            f"linked services listing for factory {echoed(factory)}",
            lambda: self._client.get_linked_services(rg, factory),
            limit,
        )
        if services is None:
            # The read failed and _try_listing said so; a note about what
            # ingestion would do with these is noise next to that.
            return []
        records: List[Dict[str, object]] = []
        for ls in services:
            # Only type and connect_via are read from the definition; every
            # type-specific property is where a credential can live.
            props = ls.properties
            ls_type = props.type if props else None
            connect_via = props.connect_via if props else None
            records.append(
                {
                    "name": ls.name,
                    "type": ls_type,
                    "platform": (
                        ADF_LINKED_SERVICE_PLATFORM_MAP.get(ls_type)
                        if ls_type
                        else None
                    ),
                    "platform_instance": self._config.platform_instance_map.get(
                        ls.name or ""
                    ),
                    "integration_runtime": (
                        connect_via.reference_name if connect_via else None
                    ),
                }
            )
        self._note_lineage_off()
        return records

    @probe_method(row_limit_param="limit")
    def datasets(self, factory: str, limit: int = 200) -> List[Dict[str, object]]:
        """ADF datasets in one factory, by factory name, each with the DataHub
        URN ingestion resolves it to for lineage -- through the connector's own
        resolver, so the URN (platform_instance_map included) is the one a run
        emits. A null urn carries unresolved_reason: no linked service, a
        linked service missing from this factory, or a linked-service type
        with no DataHub platform mapping -- the usual reason lineage is absent.
        Dataset settings (headers, request bodies, parameters) are withheld."""
        rg = self._resource_group_of(factory)
        key = f"{rg}/{factory}"
        datasets = self._try_listing(
            f"datasets listing for factory {echoed(factory)}",
            lambda: self._client.get_datasets(rg, factory),
            limit,
        )
        if datasets is None:
            return []
        # Every linked service, not `limit` of them: any dataset may reference any.
        listed_services = self._try_listing(
            f"linked services listing for factory {echoed(factory)}",
            lambda: self._client.get_linked_services(rg, factory),
            None,
        )
        services = {ls.name: ls for ls in listed_services or [] if ls.name}
        # Primed as _cache_factory_resources primes them, so
        # _resolve_dataset_urn reads what it reads during a run.
        self._source._datasets_cache[key] = {d.name: d for d in datasets if d.name}
        self._source._linked_services_cache[key] = services
        records: List[Dict[str, object]] = []
        for dataset in datasets:
            # Only type and the linked-service reference are read from the
            # definition; type properties carry headers and request bodies.
            props = dataset.properties
            ref = props.linked_service_name if props else None
            ls_name = ref.reference_name if ref else None
            ls = services.get(ls_name) if ls_name else None
            ls_type = ls.properties.type if ls and ls.properties else None
            platform = ADF_LINKED_SERVICE_PLATFORM_MAP.get(ls_type) if ls_type else None
            # The resolver reads dataset.properties unguarded; without them
            # there is no linked service to resolve through anyway.
            urn = (
                self._source._resolve_dataset_urn(dataset.name or "", key)
                if props
                else None
            )
            reason = (
                None
                if urn
                else _unresolved_reason(
                    ls_name=ls_name,
                    ls_type=ls_type,
                    found=ls is not None,
                    listed=listed_services is not None,
                )
            )
            records.append(
                {
                    "name": dataset.name,
                    "type": props.type if props else None,
                    "linked_service": ls_name,
                    "linked_service_type": ls_type,
                    "platform": platform,
                    "urn": str(urn) if urn else None,
                    "unresolved_reason": reason,
                }
            )
        if listed_services is not None:
            self._note_lineage_off()
        return records
