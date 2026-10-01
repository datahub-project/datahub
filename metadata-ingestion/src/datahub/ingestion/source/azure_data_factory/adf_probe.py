from typing import Callable, Dict, Iterable, List, Optional, TypeVar

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.datafactory.models import Activity

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeSoftError, soft_on_status
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

    def _listing(
        self,
        context: str,
        items: Callable[[], Iterable[_T]],
        limit: Optional[int],
    ) -> List[_T]:
        """Named items from one per-factory listing.

        The soft_on_status block wraps the iteration, not just the call: an
        Azure pager raises HttpResponseError while being iterated. Nameless
        items are skipped as ingestion skips them, and counted so the gap is
        visible. A 403/404 becomes [] plus a warning -- "could not look",
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
            return []
        if nameless:
            self._warn(
                f"{context}: {nameless} item(s) had no name and were skipped, "
                f"as ingestion skips them"
            )
        return out

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
