from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from lib.query_spec import QuerySpec, parse_query_specs


@dataclass(frozen=True)
class Scenario:
    operation: str
    variables: Dict[str, Any]
    assertions: Optional[Dict[str, Any]] = None
    expected_status_code: int = 200
    fixture_expected_status_code: Optional[int] = None

    @property
    def fixture_expected(self) -> int:
        if self.fixture_expected_status_code is not None:
            return self.fixture_expected_status_code
        return self.expected_status_code


@dataclass(frozen=True)
class PersonaBenchmark:
    persona: str
    scenarios: List[Scenario]


@dataclass(frozen=True)
class PersonaOracle:
    user_urn: str
    stress_target: str
    membership_count: int


@dataclass(frozen=True)
class BenchmarkPack:
    query_specs: Dict[str, QuerySpec]
    benchmarks: Dict[str, PersonaBenchmark]


def load_personas_oracle(fixture_dir: Path) -> Dict[str, PersonaOracle]:
    path = fixture_dir / "personas.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    result: Dict[str, PersonaOracle] = {}
    for name, entry in raw.items():
        result[name] = PersonaOracle(
            user_urn=entry["userUrn"],
            stress_target=entry.get("stressTarget", ""),
            membership_count=int(entry.get("membershipCount", 0)),
        )
    return result


def load_benchmark_pack(fixture_dir: Path) -> BenchmarkPack:
    path = fixture_dir / "benchmarks.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    schema_version = int(raw.get("schema_version", 0))
    if schema_version < 3:
        raise ValueError(
            f"{path} schema_version must be >= 3 with query_specs (got {schema_version})"
        )
    if raw.get("query_templates"):
        raise ValueError(f"{path} must use query_specs, not legacy query_templates")

    query_specs = parse_query_specs(raw)

    benchmarks: Dict[str, PersonaBenchmark] = {}
    for entry in raw["personas"]:
        scenarios = [
            Scenario(
                operation=s["operation"],
                variables=dict(s.get("variables", {})),
                assertions=s.get("assertions"),
                expected_status_code=int(s.get("expected_status_code", 200)),
            )
            for s in entry["scenarios"]
        ]
        benchmarks[entry["persona"]] = PersonaBenchmark(
            persona=entry["persona"], scenarios=scenarios
        )

    for benchmark in benchmarks.values():
        for scenario in benchmark.scenarios:
            if scenario.operation not in query_specs:
                raise ValueError(
                    f"benchmarks.json missing query_specs entry for "
                    f"operation {scenario.operation!r}"
                )

    return BenchmarkPack(query_specs=query_specs, benchmarks=benchmarks)


def load_benchmarks(fixture_dir: Path) -> Dict[str, PersonaBenchmark]:
    return load_benchmark_pack(fixture_dir).benchmarks
