from abc import abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Literal

from datahub.api.entities.assertion.assertion_config_spec import AssertionsConfigSpec
from datahub.ingestion.api.report import Report
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.str_enum import StrEnum


class CompileResultArtifactType(StrEnum):
    SQL_QUERIES = "SQL_QUERIES"
    COMPILE_REPORT = "COMPILE_REPORT"


@dataclass
class CompileResultArtifact(Report):
    name: str
    type: CompileResultArtifactType
    path: Path
    description: str


@dataclass
class AssertionCompilationReport(Report):
    """Additional details to debug compilation"""

    num_processed: int = 0
    num_compile_succeeded: int = 0
    num_compile_failed: int = 0  # Likely due to assertion not supported in platform

    warnings: LossyDict[str, LossyList[str]] = field(default_factory=LossyDict)
    failures: LossyDict[str, LossyList[str]] = field(default_factory=LossyDict)

    artifacts: List[Path] = field(default_factory=list)

    def report_warning(self, key: str, reason: str) -> None:
        warnings = self.warnings.get(key, LossyList())
        warnings.append(reason)
        self.warnings[key] = warnings

    def report_failure(self, key: str, reason: str) -> None:
        failures = self.failures.get(key, LossyList())
        failures.append(reason)
        self.failures[key] = failures


@dataclass
class AssertionCompilationResult:
    """Results of compilation step , along with detailed report object"""

    platform: str
    status: Literal["success", "failure"]

    report: AssertionCompilationReport = field(
        default_factory=AssertionCompilationReport
    )

    artifacts: List[CompileResultArtifact] = field(default_factory=list)

    def add_artifact(self, artifact: CompileResultArtifact) -> None:
        self.artifacts.append(artifact)
        self.report.artifacts.append(artifact.path)


class AssertionCompiler:
    @classmethod
    @abstractmethod
    def create(cls, output_dir: str, extras: Dict[str, str]) -> "AssertionCompiler":
        pass

    @abstractmethod
    def compile(
        self, assertion_config_spec: AssertionsConfigSpec
    ) -> AssertionCompilationResult:
        pass
