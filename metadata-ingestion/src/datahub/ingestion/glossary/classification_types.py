from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# These dataclasses define the data contract between the classification framework
# and any `Classifier` implementation. They were previously imported from
# `datahub_classify.helper_classes` (the `acryl-datahub-classify` package). That
# dependency pinned `numpy<2` and an outdated spaCy stack and has been dropped, so
# the types are vendored here verbatim to keep the `Classifier` extension point and
# the orchestration mixin usable by third-party classifiers without the dependency.


@dataclass
class DebugInfo:
    name: Optional[float] = None
    description: Optional[float] = None
    datatype: Optional[float] = None
    values: Optional[float] = None


@dataclass
class InfotypeProposal:
    infotype: str
    confidence_level: float
    debug_info: DebugInfo


@dataclass
class Metadata:
    meta_info: Dict[str, Any]
    # Derived from meta_info; any of these may be absent (e.g. a column without a
    # description), so they are Optional even though callers typically populate them.
    name: Optional[str] = field(init=False)
    description: Optional[str] = field(init=False)
    datatype: Optional[str] = field(init=False)
    dataset_name: Optional[str] = field(init=False)

    def __post_init__(self) -> None:
        self.name = self.meta_info.get("Name", None)
        self.description = self.meta_info.get("Description", None)
        self.datatype = self.meta_info.get("Datatype", None)
        self.dataset_name = self.meta_info.get("Dataset_Name", None)


@dataclass
class ColumnInfo:
    metadata: Metadata
    values: List[Any]
    infotype_proposals: Optional[List[InfotypeProposal]] = None
