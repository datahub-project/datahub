import hashlib
import json
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

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

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
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    StatusClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urns.urn import guess_entity_type

# (project id, object id) -> the object's search/object-info record, or None.
ObjectLookup = Callable[[str, str], Optional[MicroStrategyObject]]
# (project id, object id) -> the URN that carries the object's fingerprint.
StampedUrnBuilder = Callable[[str, str], str]
StampedAspect = Union[
    Type[DashboardInfoClass], Type[ChartInfoClass], Type[DatasetPropertiesClass]
]


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
    _embedders: Dict[str, Set[str]] = PrivateAttr(default_factory=dict)

    def model_post_init(self, __context: object) -> None:
        candidates = set(self.candidates)
        for dashboard_id, previous in self.previous_dashboards.items():
            if dashboard_id in candidates:
                for dependency_id in previous.dependency_ids:
                    self._embedders.setdefault(dependency_id, set()).add(dashboard_id)
        for report_id, previous_report in self.previous_reports.items():
            self._embedders.setdefault(report_id, set()).update(
                candidates.intersection(previous_report.dashboard_ids)
            )

    def embedders(self, report_id: str) -> Set[str]:
        return self._embedders.get(report_id, set())

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


class FolderRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: Optional[str]
    name: str


class ObjectSignature(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    type: Optional[str]
    subtype: Optional[str]
    name: str
    owner: Optional[str]
    description: Optional[str]
    version: Optional[str]
    date_modified: Optional[str]
    view_media: Optional[int]
    certified_info: Optional[Dict[str, object]]
    # Raw ancestors, so a moved dependency changes its dependents too.
    folders: List[FolderRef]

    @classmethod
    def of(cls, mstr_object: MicroStrategyObject) -> "ObjectSignature":
        return cls(
            id=normalize_object_id(mstr_object.id),
            type=mstr_object.type,
            subtype=mstr_object.subtype,
            name=mstr_object.name,
            owner=mstr_object.owner,
            description=mstr_object.description,
            version=mstr_object.version,
            date_modified=mstr_object.date_modified,
            view_media=mstr_object.view_media,
            certified_info=mstr_object.certified_info,
            folders=[
                FolderRef(id=part.id, name=part.name)
                for part in extract_folder_parts(mstr_object.model_dump())
            ],
        )


class DependencySignature(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    signature: Optional[ObjectSignature]


class ObjectFingerprintInput(BaseModel):
    model_config = ConfigDict(frozen=True)

    target: ObjectSignature
    folder_path: Optional[str]
    dependencies: List[DependencySignature]


class ReportDatasetFingerprintInput(BaseModel):
    model_config = ConfigDict(frozen=True)

    report: Optional[ObjectSignature]
    dashboards: Dict[str, Optional[str]]
    from_report_pass: bool


class ProjectSalt(BaseModel):
    """Anything shaping every object's output; a change re-reads the project."""

    model_config = ConfigDict(frozen=True)

    version: str
    config: Dict[str, object]
    warehouses: List[Dict[str, object]]
    folder_labels: Dict[str, str]
    hidden_folders: List[str]
    model_tables: Optional[str]


class ProjectFingerprinter:
    def __init__(self, project_id: str, salt: ProjectSalt) -> None:
        self.project_id = project_id
        self._salt = _canonical_json(salt)

    def _digest(self, payload: BaseModel) -> str:
        body = f"{self._salt}|{_canonical_json(payload)}"
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def object_fingerprint(
        self,
        mstr_object: MicroStrategyObject,
        folder_path: Optional[str],
        dependency_ids: Sequence[str],
        lookup: ObjectLookup,
    ) -> str:
        dependencies = []
        for dependency_id in sorted(dependency_ids, key=normalize_object_id):
            dependency = lookup(self.project_id, dependency_id)
            dependencies.append(
                DependencySignature(
                    id=normalize_object_id(dependency_id),
                    signature=ObjectSignature.of(dependency) if dependency else None,
                )
            )
        return self._digest(
            ObjectFingerprintInput(
                target=ObjectSignature.of(mstr_object),
                folder_path=folder_path,
                dependencies=dependencies,
            )
        )

    def report_dataset_fingerprint(
        self,
        report_object: Optional[MicroStrategyObject],
        dashboard_fingerprints: Mapping[str, Optional[str]],
        from_report_pass: bool,
    ) -> str:
        return self._digest(
            ReportDatasetFingerprintInput(
                report=ObjectSignature.of(report_object) if report_object else None,
                dashboards=dict(dashboard_fingerprints),
                from_report_pass=from_report_pass,
            )
        )


def _canonical_json(model: BaseModel) -> str:
    return json.dumps(model.model_dump(mode="json"), sort_keys=True)


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


def _rows_digest(rows: Optional[Sequence[Mapping[str, object]]]) -> Optional[str]:
    # Hashed once here; a large project's rows would otherwise go into every fingerprint.
    if rows is None:
        return None
    body = json.dumps(sorted(json.dumps(row, sort_keys=True) for row in rows))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


# The evaluate methods only read, so they are safe on worker threads.
class MicroStrategyChangeDetector:
    def __init__(self, config: MicroStrategyConfig, report: MicroStrategyReport):
        self.config = config
        self.report = report

    def init_project(
        self,
        project_id: str,
        source_warehouses: Sequence[Datasource],
        predefined_folders: PredefinedFolderResolution,
        model_tables: Optional[Sequence[Mapping[str, object]]],
    ) -> ProjectFingerprinter:
        return ProjectFingerprinter(
            project_id,
            ProjectSalt(
                version=__version__,
                config=self.config.model_dump(
                    mode="json", exclude=set(FINGERPRINT_EXCLUDED_CONFIG_FIELDS)
                ),
                # Declared fields only: extra="allow" keeps raw API fields too.
                warehouses=[
                    warehouse.model_dump(
                        mode="json", include=set(Datasource.model_fields)
                    )
                    for warehouse in sorted(
                        source_warehouses,
                        key=lambda warehouse: normalize_object_id(warehouse.id),
                    )
                ],
                folder_labels=predefined_folders.labels,
                hidden_folders=sorted(predefined_folders.hidden_ids),
                model_tables=_rows_digest(model_tables),
            ),
        )

    def evaluate_object(
        self,
        fingerprinter: ProjectFingerprinter,
        mstr_object: MicroStrategyObject,
        folder_path: Optional[str],
        previous: Optional[ObjectCheckpoint],
        lookup: ObjectLookup,
        graph: Optional[DataHubGraph],
        stamped_urn: StampedUrnBuilder,
        stamped_aspect: StampedAspect,
    ) -> bool:
        if (
            previous is None
            or not previous.urns
            or previous.fingerprint is None
            or graph is None
        ):
            return False
        previous_fingerprint = previous.fingerprint

        def unchanged() -> bool:
            fingerprint = fingerprinter.object_fingerprint(
                mstr_object, folder_path, previous.dependency_ids, lookup
            )
            # Checkpoints are committed even when the sink failed.
            return (
                fingerprint == previous_fingerprint
                and _stamped_in_datahub(
                    graph,
                    stamped_urn(fingerprinter.project_id, mstr_object.id),
                    stamped_aspect,
                    fingerprint,
                )
                and entities_present(graph, previous.urns)
            )

        return self._checked(fingerprinter.project_id, mstr_object.id, unchanged)

    def evaluate_report_dataset(
        self,
        fingerprinter: ProjectFingerprinter,
        report_id: str,
        previous: ReportDatasetCheckpoint,
        dashboard_fingerprints: Mapping[str, Optional[str]],
        lookup_report_object: ObjectLookup,
        graph: Optional[DataHubGraph],
    ) -> bool:
        if not previous.urns or previous.fingerprint is None or graph is None:
            return False
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
            return False
        stamped_urn = dataset_urn

        def unchanged() -> bool:
            fingerprint = fingerprinter.report_dataset_fingerprint(
                lookup_report_object(fingerprinter.project_id, report_id),
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

        return self._checked(fingerprinter.project_id, report_id, unchanged)

    def _checked(
        self, project_id: str, object_id: str, unchanged: Callable[[], bool]
    ) -> bool:
        # A failed check reads the object again rather than failing the run.
        try:
            return unchanged()
        except MicroStrategyAuthError:
            raise
        except Exception as error:
            self.report.change_check_failures += 1
            self.report.warning(
                title="Unchanged-object check failed",
                message=(
                    "The object could not be compared with DataHub, so it is "
                    "read again."
                ),
                context=f"project_id={project_id}, object_id={object_id}",
                exc=error,
            )
            return False
