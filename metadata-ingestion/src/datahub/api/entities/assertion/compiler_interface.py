from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List

from datahub.api.entities.assertion.assertion_config_spec import AssertionsConfigSpec
from datahub.ingestion.api.report import Report
from datahub.utilities.lossy_collections import LossyDict, LossyList


class StrEnum(str, Enum):
    pass


class CompileResultArtifactType(StrEnum):
    SQL_QUERIES = "SQL_QUERIES"
    COMPILE_REPORT = "COMPILE_REPORT"


@dataclass
class CompileResultArtifact(Report):
    name: str
    type: CompileResultArtifactType
    description: str
    path: Path


@dataclass
class AssertionCompilationReport(Report):
    num_processed: int = 0
    num_compile_succeeded: int = 0
    num_compile_failed: int = 0  # Likely due to assertion not supported in platform
    warnings: LossyDict[str, LossyList[str]] = field(default_factory=LossyDict)
    failures: LossyDict[str, LossyList[str]] = field(default_factory=LossyDict)

    generated_artifacts: List[CompileResultArtifact] = field(default_factory=list)

    def report_warning(self, key: str, reason: str) -> None:
        warnings = self.warnings.get(key, LossyList())
        warnings.append(reason)
        self.warnings[key] = warnings

    def report_failure(self, key: str, reason: str) -> None:
        failures = self.failures.get(key, LossyList())
        failures.append(reason)
        self.failures[key] = failures

    def report_artifact(self, artifact: CompileResultArtifact) -> None:
        self.generated_artifacts.append(artifact)


class AssertionCompiler:
    @classmethod
    @abstractmethod
    def create(cls, output_dir: str, extras: Dict[str, str]) -> "AssertionCompiler":
        pass

    @abstractmethod
    def process(
        self, assertion_config_spec: AssertionsConfigSpec
    ) -> AssertionCompilationReport:
        pass
