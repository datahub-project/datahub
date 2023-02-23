
import json
from dataclasses import dataclass
from typing import Any, Dict, List

import yaml

@dataclass
class QuickstartVersionMappings:
    composefileGitRef: str
    dockerTag: str

@dataclass
class StableVersions:
    force: bool
    composefileGitRef: str
    dockerTag: str

@dataclass
class QuickstartChecks:
    validUntilGitRef: str
    requieredContainers: List[str]
    ensureExistSuccess: List[str]

@dataclass
class QuickstartVersionMappingCongif:
    quickstartVersionMappings: Dict[str, QuickstartVersionMappings]
    stableVersions: StableVersions
    quickstartChecks: List[QuickstartChecks]