from typing import Callable, Dict, Iterable, List, Optional, TypeVar

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.datafactory.models import Activity

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeSoftError, soft_on_status
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

# The attributes an ADF control activity keeps its children in. Ingestion walks
# only pipeline.activities (AzureDataFactorySource._process_pipelines), so
# anything under these never becomes a DataJob.
_NESTED_ACTIVITY_ATTRS = (
    "activities",  # ForEach, Until
    "if_true_activities",  # IfCondition
    "if_false_activities",
    "default_activities",  # Switch
)


def _nested_activity_count(activity: object) -> int:
    children: List[object] = []
    for attr in _NESTED_ACTIVITY_ATTRS:
        children.extend(getattr(activity, attr, None) or [])
    for case in getattr(activity, "cases", None) or []:  # Switch
        children.extend(getattr(case, "activities", None) or [])
    return sum(1 + _nested_activity_count(child) for child in children)


def _activity_record(activity: Activity) -> Dict[str, object]:
    # An allowlist, not a dump: type properties hold Web activity headers and
    # auth, Script/Copy SQL text and stored-procedure parameters.
    activity_type = activity.type or "Unknown"
    record: Dict[str, object] = {
        # "Unknown" as _create_datajob names it, so this is the emitted DataJob name.
        "name": activity.name or "Unknown",
        "type": activity_type,
        "subtype": str(ACTIVITY_SUBTYPE_MAP.get(activity_type, activity_type)),
        "depends_on": [d.activity for d in activity.depends_on or []],
        "inputs": [r.reference_name for r in getattr(activity, "inputs", None) or []],
        "outputs": [
            r.reference_name for r in getattr(activity, "outputs", None) or []
        ],
    }
    nested = _nested_activity_count(activity)
    if nested:
        record["nested_activities"] = nested
    return record


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
        return f"linked service '{ls_name}' not found in this factory"
    if not ls_type:
        return f"linked service '{ls_name}' has no type"
    return f"linked service type '{ls_type}' has no DataHub platform mapping"


class AzureDataFactoryMetadataProbe:
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

    warnings: List[str]

    def __init__(self, source: AzureDataFactorySource, credential: object) -> None:
        self._source = source
        self._config: AzureDataFactoryConfig = source.config
        self._client: AzureDataFactoryClient = source.client
        self._credential = credential
        self.warnings = []

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
        return cls(AzureDataFactorySource.for_probe(config, client), credential)

    def __enter__(self) -> "AzureDataFactoryMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            self._client.close()
        finally:
            # Ingestion never closes the credential; a probe is a short-lived
            # process, and closing it releases the token client's transport.
            close = getattr(self._credential, "close", None)
            if callable(close):
                close()

    @property
    def probe_report(self) -> AzureDataFactorySourceReport:
        """_resolve_dataset_urn records unmapped platforms here with
        report.warning(); exposing it folds those into the result."""
        return self._source.report

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

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
        out: List[Dict[str, object]] = []
        nameless = 0
        for factory in self._client.get_factories(resource_group=rg):
            if not factory.name or not factory.id:
                nameless += 1
                continue
            out.append(
                {
                    "name": factory.name,
                    "resource_group": self._source._extract_resource_group(
                        factory.id
                    ),
                    "location": factory.location,
                }
            )
            if len(out) >= limit:
                break
        if nameless:
            self._warn(
                f"{nameless} factor{'y' if nameless == 1 else 'ies'} had no name "
                f"or id and were skipped, as ingestion skips them"
            )
        if rg:
            self._warn(
                f"resource_group is set to '{rg}', so only that resource group "
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
        matches = [
            f
            for f in self._client.get_factories(resource_group=rg)
            if f.name == factory and f.id
        ]
        if not matches:
            where = (
                f"resource group '{rg}' (the recipe's resource_group)"
                if rg
                else f"subscription '{self._config.subscription_id}'"
            )
            raise ValueError(f"no data factory named '{factory}' in {where}")
        groups = sorted(
            {self._source._extract_resource_group(f.id or "") for f in matches}
        )
        if len(groups) > 1:
            raise ValueError(
                f"data factory name '{factory}' is in several resource groups "
                f"({', '.join(groups)}); set resource_group in the recipe"
            )
        return groups[0]

    def _try_listing(
        self,
        context: str,
        items: Callable[[], Iterable[_T]],
        limit: Optional[int],
    ) -> Optional[List[_T]]:
        """Named items from one per-factory listing, or None when it could not
        be read.

        The soft_on_status block wraps the iteration, not just the call: an
        Azure pager raises HttpResponseError while being iterated. Nameless
        items are skipped as ingestion skips them, and counted so the gap is
        visible. A 403/404 becomes None plus a warning -- "could not look",
        never "nothing here".
        """
        out: List[_T] = []
        nameless = 0
        try:
            with soft_on_status(403, 404, context=context):
                for item in items():
                    if not getattr(item, "name", None):
                        nameless += 1
                        continue
                    out.append(item)
                    if limit is not None and len(out) >= limit:
                        break
        except ProbeSoftError as exc:
            self._warn(str(exc))
            return None
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
        because they can hold secrets."""
        rg = self._resource_group_of(factory)
        pipelines = self._listing(
            f"pipelines listing for factory '{factory}'",
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
        """Top-level activities of one pipeline, by factory and pipeline name --
        exactly the ones ingestion emits as DataJobs, each with the DataHub
        subtype it gets. Nothing filters activities; they follow their
        pipeline's verdict. Activities nested in ForEach/IfCondition/Until/
        Switch are counted on their container as nested_activities, because
        ingestion does not emit them. inputs/outputs are ADF dataset names;
        resolve them with `datasets`. Activity settings (URLs, headers, SQL,
        parameters) are withheld."""
        rg = self._resource_group_of(factory)
        try:
            with soft_on_status(
                403, context=f"pipeline '{pipeline}' in factory '{factory}'"
            ):
                resource = self._client.get_pipeline(rg, factory, pipeline)
        except ResourceNotFoundError as exc:
            raise ValueError(
                f"no pipeline named '{pipeline}' in data factory '{factory}'"
            ) from exc
        except ProbeSoftError as exc:
            self._warn(str(exc))
            return []
        records = [_activity_record(a) for a in resource.activities or []]
        nested = sum(_nested_activity_count(a) for a in resource.activities or [])
        if nested:
            self._warn(
                f"{nested} activit{'y is' if nested == 1 else 'ies are'} nested "
                f"inside control activities (ForEach/IfCondition/Until/Switch); "
                f"ingestion emits only top-level activities, so these will not "
                f"appear as DataJobs"
            )
        return records

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
        services = self._listing(
            f"linked services listing for factory '{factory}'",
            lambda: self._client.get_linked_services(rg, factory),
            limit,
        )
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
        datasets = self._listing(
            f"datasets listing for factory '{factory}'",
            lambda: self._client.get_datasets(rg, factory),
            limit,
        )
        # Every linked service, not `limit` of them: any dataset may reference any.
        listed_services = self._try_listing(
            f"linked services listing for factory '{factory}'",
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
            platform = (
                ADF_LINKED_SERVICE_PLATFORM_MAP.get(ls_type) if ls_type else None
            )
            urn = self._source._resolve_dataset_urn(dataset.name or "", key)
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
        self._note_lineage_off()
        return records
