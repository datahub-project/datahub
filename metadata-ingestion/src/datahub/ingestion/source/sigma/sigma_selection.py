"""Which Sigma workbooks and Data Models ingestion keeps, as pure functions of
the recipe and facts about one object.

`placement_verdict` is the rule SigmaAPI.get_sigma_workbooks and
SigmaAPI.get_data_models apply once an object's own name has passed its
pattern; `workbook_verdict` and `data_model_verdict` add the steps before it in
the order those methods take them, for `probe filter`
(SigmaSourceConfig.probe_verdict_override).
"""

from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional, Protocol, Union

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

WORKSPACE_PATTERN = "workspace_pattern"
WORKBOOK_PATTERN = "workbook_pattern"
DATA_MODEL_PATTERN = "data_model_pattern"
INGEST_SHARED_ENTITIES = "ingest_shared_entities"
# Not a config field: ingestion drops a workbook /files does not list, since
# its workspace is read from there (get_sigma_workbooks' "missing file
# metadata" branch).
MISSING_FILE_METADATA = "missing_file_metadata"


class SigmaSelectionConfig(Protocol):
    @property
    def workspace_pattern(self) -> AllowDenyPattern: ...

    @property
    def workbook_pattern(self) -> AllowDenyPattern: ...

    @property
    def data_model_pattern(self) -> AllowDenyPattern: ...

    @property
    def ingest_shared_entities(self) -> Optional[bool]: ...


class _Unknown(Enum):
    UNKNOWN = "unknown"


# A fact `probe filter` was not given (a bare --name): that rule is skipped.
# Not None, because None is a fact ingestion reads: an object in no readable
# workspace is a shared entity.
UNKNOWN: Literal[_Unknown.UNKNOWN] = _Unknown.UNKNOWN

# An object's workspace name, None for an object in no readable workspace, or
# UNKNOWN.
WorkspaceFact = Union[Optional[str], _Unknown]


def placement_verdict(
    config: SigmaSelectionConfig, workspace_name: Optional[str]
) -> Verdict:
    """In a workspace this credential can read, the workspace's name decides;
    in none (no workspace, or one that refused the lookup), the object is a
    shared entity and ingest_shared_entities decides."""
    if workspace_name is not None:
        if config.workspace_pattern.allowed(workspace_name):
            return Verdict.include()
        return Verdict.exclude(WORKSPACE_PATTERN)
    if config.ingest_shared_entities:
        return Verdict.include()
    return Verdict.exclude(INGEST_SHARED_ENTITIES)


@dataclass(frozen=True)
class WorkbookFacts:
    name: str
    # Whether /files lists the workbook, which is where its workspace is read.
    in_files: Union[bool, _Unknown] = UNKNOWN
    workspace_name: WorkspaceFact = UNKNOWN


def workbook_verdict(config: SigmaSelectionConfig, facts: WorkbookFacts) -> Verdict:
    """Name, then /files, then placement: get_sigma_workbooks' order."""
    if not config.workbook_pattern.allowed(facts.name):
        return Verdict.exclude(WORKBOOK_PATTERN)
    if facts.in_files is False:
        return Verdict.exclude(MISSING_FILE_METADATA)
    if facts.workspace_name is UNKNOWN:
        return Verdict.include()
    return placement_verdict(config, facts.workspace_name)


@dataclass(frozen=True)
class DataModelFacts:
    name: str
    workspace_name: WorkspaceFact = UNKNOWN


def data_model_verdict(config: SigmaSelectionConfig, facts: DataModelFacts) -> Verdict:
    """Name, then placement: get_data_models' order. A Data Model needs no
    /files row, since its own payload names its workspace."""
    if not config.data_model_pattern.allowed(facts.name):
        return Verdict.exclude(DATA_MODEL_PATTERN)
    if facts.workspace_name is UNKNOWN:
        return Verdict.include()
    return placement_verdict(config, facts.workspace_name)
