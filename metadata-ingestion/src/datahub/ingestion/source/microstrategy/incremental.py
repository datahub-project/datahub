import hashlib
import json
import logging
from typing import (
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Type,
    Union,
)

from pydantic import BaseModel, ConfigDict, Field

from datahub._version import __version__
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.microstrategy.checkpoint_state import (
    ObjectCheckpoint,
    ReportDatasetCheckpoint,
)
from datahub.ingestion.source.microstrategy.client import MicroStrategyAuthError
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    ENTITY_CHECK_BATCH_SIZE,
    FINGERPRINT_EXCLUDED_CONFIG_FIELDS,
    MSTR_PROP_FINGERPRINT,
    SHARED_REFERENCE_ENTITY_TYPES,
)
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    MicroStrategyObject,
    PredefinedFolderResolution,
    extract_folder_parts,
    normalize_object_id,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    StatusClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)

# (project id, object id) -> the object's search/object-info record, or None.
ObjectLookup = Callable[[str, str], Optional[MicroStrategyObject]]
# (project id, object id) -> the URN that carries the object's fingerprint.
StampedUrnBuilder = Callable[[str, str], str]
StampedAspect = Union[
    Type[DashboardInfoClass], Type[ChartInfoClass], Type[DatasetPropertiesClass]
]


class ChangeCheckResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    unchanged: bool
    failure: Optional[str] = None


# A per_report dataset merges every embedding dashboard, so it is re-emitted
# only together with all of them; the sets are filled in place during a run.
class ReportDatasetPlan(BaseModel):
    previous_dashboards: Dict[str, ObjectCheckpoint]
    previous_reports: Dict[str, ReportDatasetCheckpoint]
    # Normalized ids of the project's allowed dashboards, in search order.
    candidates: List[str]
    unchanged: Set[str]
    dirty: Set[str] = Field(default_factory=set)
    reports_in_pass: Set[str] = Field(default_factory=set)

    def embedders(self, report_id: str) -> Set[str]:
        candidates = set(self.candidates)
        found = {
            dashboard_id
            for dashboard_id, previous in self.previous_dashboards.items()
            if dashboard_id in candidates
            and report_id
            in {normalize_object_id(dep) for dep in previous.dependency_ids}
        }
        previous_report = self.previous_reports.get(report_id)
        if previous_report is not None:
            found.update(candidates.intersection(previous_report.dashboard_ids))
        return found

    def mark_reports_of_changed_dashboards(self) -> None:
        for report_id, previous in self.previous_reports.items():
            if any(
                dashboard_id not in self.unchanged
                for dashboard_id in previous.dashboard_ids
            ):
                self.dirty.add(report_id)

    def read_in_report_pass(self, report_id: str) -> bool:
        # A report newly entering the report pass changes its dataset.
        self.reports_in_pass.add(report_id)
        previous = self.previous_reports.get(report_id)
        if (
            report_id not in self.dirty
            and previous is not None
            and previous.from_report_pass
        ):
            return False
        self.dirty.add(report_id)
        return True

    def mark_reports_left_report_pass(self) -> None:
        for report_id, previous in self.previous_reports.items():
            if previous.from_report_pass and report_id not in self.reports_in_pass:
                self.dirty.add(report_id)

    def emits(self, report_id: str) -> bool:
        return report_id in self.dirty or report_id not in self.previous_reports

    def kept_reports(self) -> Dict[str, ReportDatasetCheckpoint]:
        return {
            report_id: previous
            for report_id, previous in self.previous_reports.items()
            if report_id not in self.dirty
        }

    def dashboards_to_read(self) -> Set[str]:
        found: Set[str] = set()
        for report_id in self.dirty:
            found.update(self.embedders(report_id))
        return found & self.unchanged


def object_signature(
    mstr_object: Optional[MicroStrategyObject],
) -> Optional[Dict[str, object]]:
    if mstr_object is None:
        return None
    return {
        "id": normalize_object_id(mstr_object.id),
        "type": mstr_object.type,
        "subtype": mstr_object.subtype,
        "name": mstr_object.name,
        "owner": mstr_object.owner,
        "description": mstr_object.description,
        "version": mstr_object.version,
        "date_modified": mstr_object.date_modified,
        "view_media": mstr_object.view_media,
        "certified_info": mstr_object.certified_info,
        # Raw ancestors, so a moved dependency changes its dependents too.
        "folders": [
            [part.id, part.name]
            for part in extract_folder_parts(mstr_object.model_dump())
        ],
    }


def entities_present(graph: DataHubGraph, urns: Sequence[str]) -> bool:
    # Also catches entities the sink failed to write.
    by_type: Dict[str, List[str]] = {}
    for urn in urns:
        entity_type = guess_entity_type(urn)
        if entity_type not in SHARED_REFERENCE_ENTITY_TYPES:
            by_type.setdefault(entity_type, []).append(urn)
    for entity_type, typed_urns in by_type.items():
        for start in range(0, len(typed_urns), ENTITY_CHECK_BATCH_SIZE):
            batch = typed_urns[start : start + ENTITY_CHECK_BATCH_SIZE]
            entities = graph.get_entities(
                entity_type, batch, aspects=[StatusClass.ASPECT_NAME]
            )
            for urn in batch:
                status = entities.get(urn, {}).get(StatusClass.ASPECT_NAME)
                if (
                    status is None
                    or not isinstance(status[0], StatusClass)
                    or status[0].removed
                ):
                    return False
    return True


def _stamped_in_datahub(
    graph: DataHubGraph, urn: str, aspect_class: StampedAspect, fingerprint: str
) -> bool:
    aspect = graph.get_aspect(urn, aspect_class)
    return (
        aspect is not None
        and (aspect.customProperties or {}).get(MSTR_PROP_FINGERPRINT) == fingerprint
    )


# The evaluate methods only read, so they are safe on worker threads.
class MicroStrategyChangeDetector:
    def __init__(self, config: MicroStrategyConfig) -> None:
        self.config = config
        self._salts: Dict[str, str] = {}

    def init_project(
        self,
        project_id: str,
        source_warehouses: Sequence[Datasource],
        predefined_folders: PredefinedFolderResolution,
    ) -> None:
        # Anything shaping every object's output re-reads the whole project.
        self._salts[project_id] = json.dumps(
            {
                "version": __version__,
                "config": self.config.model_dump(
                    mode="json", exclude=set(FINGERPRINT_EXCLUDED_CONFIG_FIELDS)
                ),
                "warehouses": [
                    warehouse.model_dump(mode="json") for warehouse in source_warehouses
                ],
                "folder_labels": sorted(predefined_folders.labels.items()),
                "hidden_folders": sorted(predefined_folders.hidden_ids),
            },
            sort_keys=True,
            default=str,
        )

    def _fingerprint(self, project_id: str, payload: Mapping[str, object]) -> str:
        salt = self._salts[project_id]
        body = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(f"{salt}|{body}".encode("utf-8")).hexdigest()

    def object_fingerprint(
        self,
        project_id: str,
        mstr_object: MicroStrategyObject,
        folder_path: Optional[str],
        dependency_ids: Sequence[str],
        lookup: ObjectLookup,
    ) -> str:
        return self._fingerprint(
            project_id,
            {
                "object": object_signature(mstr_object),
                "folder_path": folder_path,
                "dependencies": [
                    [
                        normalize_object_id(dep),
                        object_signature(lookup(project_id, dep)),
                    ]
                    for dep in sorted(dependency_ids, key=normalize_object_id)
                ],
            },
        )

    def report_dataset_fingerprint(
        self,
        project_id: str,
        report_object: Optional[MicroStrategyObject],
        dashboard_fingerprints: Mapping[str, Optional[str]],
        from_report_pass: bool,
    ) -> str:
        return self._fingerprint(
            project_id,
            {
                "report": object_signature(report_object),
                "dashboards": sorted(dashboard_fingerprints.items()),
                "from_report_pass": from_report_pass,
            },
        )

    def evaluate_object(
        self,
        project_id: str,
        mstr_object: MicroStrategyObject,
        folder_path: Optional[str],
        previous: Optional[ObjectCheckpoint],
        lookup: ObjectLookup,
        graph: Optional[DataHubGraph],
        stamped_urn: StampedUrnBuilder,
        stamped_aspect: StampedAspect,
    ) -> ChangeCheckResult:
        if (
            previous is None
            or not previous.urns
            or previous.fingerprint is None
            or graph is None
        ):
            return ChangeCheckResult(unchanged=False)
        previous_fingerprint = previous.fingerprint

        def unchanged() -> bool:
            fingerprint = self.object_fingerprint(
                project_id, mstr_object, folder_path, previous.dependency_ids, lookup
            )
            # Checkpoints are committed even when the sink failed.
            return (
                fingerprint == previous_fingerprint
                and _stamped_in_datahub(
                    graph,
                    stamped_urn(project_id, mstr_object.id),
                    stamped_aspect,
                    fingerprint,
                )
                and entities_present(graph, previous.urns)
            )

        return _checked(project_id, mstr_object.id, unchanged)

    def evaluate_report_dataset(
        self,
        project_id: str,
        report_id: str,
        previous: ReportDatasetCheckpoint,
        dashboard_fingerprints: Mapping[str, Optional[str]],
        lookup_report_object: ObjectLookup,
        graph: Optional[DataHubGraph],
    ) -> ChangeCheckResult:
        if not previous.urns or previous.fingerprint is None or graph is None:
            return ChangeCheckResult(unchanged=False)
        previous_fingerprint = previous.fingerprint
        dataset_urn = next(
            (
                urn
                for urn in previous.urns
                if guess_entity_type(urn) == DatasetUrn.ENTITY_TYPE
            ),
            None,
        )
        if dataset_urn is None:
            return ChangeCheckResult(unchanged=False)
        stamped_urn = dataset_urn

        def unchanged() -> bool:
            fingerprint = self.report_dataset_fingerprint(
                project_id,
                lookup_report_object(project_id, report_id),
                dashboard_fingerprints,
                previous.from_report_pass,
            )
            return (
                fingerprint == previous_fingerprint
                and _stamped_in_datahub(
                    graph, stamped_urn, DatasetPropertiesClass, fingerprint
                )
                and entities_present(graph, previous.urns)
            )

        return _checked(project_id, report_id, unchanged)


def _checked(
    project_id: str, object_id: str, unchanged: Callable[[], bool]
) -> ChangeCheckResult:
    # A failed check reads the object again rather than failing the run.
    try:
        return ChangeCheckResult(unchanged=unchanged())
    except MicroStrategyAuthError:
        raise
    except Exception as error:
        logger.debug(
            "Change check failed for %s in project %s",
            object_id,
            project_id,
            exc_info=True,
        )
        return ChangeCheckResult(
            unchanged=False, failure=f"{project_id}:{object_id}: {error}"
        )
