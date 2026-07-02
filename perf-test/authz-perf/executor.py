from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

from lib.expectations import benchmark_metric_labels
from lib.graphql import (
    GraphqlResult,
    RequestHealthTracker,
    check_assertions,
    execute_graphql,
    resolve_operation_document,
    log,
    meets_expectation,
)
from lib.graphql_adapt import GraphqlQueryRegistry
from lib.init import smoke_persona_login
from lib.personas import PersonaBenchmark, PersonaOracle, Scenario
from lib.results import JsonlWriter, build_result_row
from lib.session import PersonaSession


@dataclass
class RunConfig:
    run_id: str
    metadata: dict
    gms_url: str
    frontend_url: str
    warmup: int
    iterations: int
    cache_phases: List[str]
    parallel_personas: int
    full_correctness: bool
    es_jitter: bool
    graphql_registry: GraphqlQueryRegistry
    view_authorization_enabled: Optional[bool] = None


@dataclass
class PersonaRunResult:
    rows: List[dict] = field(default_factory=list)


def _maybe_jitter_search_variables(
    variables: dict, iteration: int, es_jitter: bool
) -> dict:
    if not es_jitter:
        return variables
    inp = variables.get("input")
    if not isinstance(inp, dict):
        return variables
    copy = dict(variables)
    inner = dict(inp)
    inner["start"] = (iteration * 5) % 100
    copy["input"] = inner
    return copy


def _run_scenario_timed(
    persona_session: PersonaSession,
    scenario: Scenario,
    registry: GraphqlQueryRegistry,
    warmup: int,
    iterations: int,
    es_jitter: bool,
) -> tuple[List[float], Optional[float], dict, dict]:
    document = resolve_operation_document(registry, scenario.operation)
    health = RequestHealthTracker()
    expected = scenario.expected_status_code
    timed_results: List[GraphqlResult] = []
    for _ in range(warmup):
        vars_ = _maybe_jitter_search_variables(scenario.variables, 0, es_jitter)
        health.record(
            execute_graphql(
                persona_session.session,
                persona_session.frontend_url,
                document,
                scenario.operation,
                vars_,
            ),
            expected_status_code=expected,
        )

    samples: List[float] = []
    cold_first: Optional[float] = None
    for i in range(iterations):
        vars_ = _maybe_jitter_search_variables(scenario.variables, i, es_jitter)
        result = execute_graphql(
            persona_session.session,
            persona_session.frontend_url,
            document,
            scenario.operation,
            vars_,
        )
        health.record(result, expected_status_code=expected)
        if meets_expectation(result, expected):
            timed_results.append(result)
            if cold_first is None and i == 0:
                cold_first = result.elapsed_ms
            samples.append(result.elapsed_ms)

    context = f"{persona_session.persona}/{scenario.operation}"
    health.ensure_all_success(context)
    response_size = RequestHealthTracker.response_size_summary(timed_results)
    summary = health.summary()
    summary["expected_status_code"] = expected
    return samples, cold_first, summary, response_size


def _run_full_correctness(
    persona_session: PersonaSession,
    benchmark: PersonaBenchmark,
    registry: GraphqlQueryRegistry,
) -> List[str]:
    failures: List[str] = []
    for scenario in benchmark.scenarios:
        document = resolve_operation_document(registry, scenario.operation)
        result = execute_graphql(
            persona_session.session,
            persona_session.frontend_url,
            document,
            scenario.operation,
            scenario.variables,
        )
        if not meets_expectation(result, scenario.expected_status_code):
            failures.append(
                f"{scenario.operation}: {result.failure_label or 'status mismatch'}"
            )
            continue
        if scenario.assertions and scenario.expected_status_code == 200:
            failures.extend(
                check_assertions(result.data.get("data", {}), scenario.assertions)
            )
    return failures


def run_persona_benchmark(
    persona: str,
    benchmark: PersonaBenchmark,
    oracle: PersonaOracle,
    config: RunConfig,
    writer: JsonlWriter,
    on_row: Optional[Callable[[dict], None]] = None,
) -> PersonaRunResult:
    result = PersonaRunResult()
    persona_session = smoke_persona_login(
        persona,
        oracle,
        config.gms_url,
        config.graphql_registry,
        frontend_url=config.frontend_url,
    )

    if config.full_correctness:
        failures = _run_full_correctness(
            persona_session,
            benchmark,
            config.graphql_registry,
        )
        if failures:
            raise RuntimeError(
                f"full-correctness failed for {persona}: " + "; ".join(failures)
            )

    execution_mode = "concurrent" if config.parallel_personas > 1 else "isolated"
    harness_base = {
        "warmup": config.warmup,
        "iterations": config.iterations,
        "parallel_personas": config.parallel_personas,
        "execution_mode": execution_mode,
        "es_jitter": config.es_jitter,
    }

    for cache_phase in config.cache_phases:
        for scenario in benchmark.scenarios:
            samples, cold_first, request_health, response_size = _run_scenario_timed(
                persona_session,
                scenario,
                config.graphql_registry,
                config.warmup,
                config.iterations,
                config.es_jitter,
            )
            harness = {**harness_base, "cache_phase": cache_phase}
            labels = benchmark_metric_labels(
                scenario,
                view_authorization_enabled=config.view_authorization_enabled,
            )
            row = build_result_row(
                run_id=config.run_id,
                metadata=config.metadata,
                persona=persona,
                operation=scenario.operation,
                metric_key=labels["metric_key"],
                performance_profile=labels["performance_profile"],
                query_source=scenario.operation,
                stress_target=oracle.stress_target,
                membership_count=oracle.membership_count,
                cache_phase=cache_phase,
                harness=harness,
                samples_ms=samples,
                cold_first_ms=cold_first if cache_phase == "cold" else None,
                request_health=request_health,
                response_size=response_size,
                correctness={
                    "init_passed": True,
                    "full_correctness": config.full_correctness,
                    "assertions_checked": list(scenario.assertions or {}),
                    "expected_status_code": scenario.expected_status_code,
                    "fixture_expected_status_code": scenario.fixture_expected,
                    "view_authorization_enabled": config.view_authorization_enabled,
                },
            )
            writer.append(row)
            result.rows.append(row)
            if on_row:
                on_row(row)
            stats = row["stats"]
            health = row["request_health"]
            size = row["response_size"]
            expected = row["correctness"].get("expected_status_code", 200)
            log(
                f"{persona}/{labels['metric_key']}/{cache_phase}: "
                f"p50={stats['p50']}ms p95={stats['p95']}ms max={stats['max']}ms "
                f"({health['success_count']}/{health['total_requests']} "
                f"profile={labels['performance_profile']}, expected={expected}, "
                f"avg={size['avg_kb']}KB)"
            )
    return result


def run_all_personas(
    personas: List[str],
    benchmarks: Dict[str, PersonaBenchmark],
    oracles: Dict[str, PersonaOracle],
    config: RunConfig,
    writer: JsonlWriter,
) -> List[dict]:
    all_rows: List[dict] = []

    def collect(row: dict) -> None:
        all_rows.append(row)

    if config.parallel_personas <= 1:
        for persona in personas:
            run_persona_benchmark(
                persona, benchmarks[persona], oracles[persona], config, writer, collect
            )
    else:
        with ThreadPoolExecutor(max_workers=config.parallel_personas) as pool:
            futures = {
                pool.submit(
                    run_persona_benchmark,
                    persona,
                    benchmarks[persona],
                    oracles[persona],
                    config,
                    writer,
                    collect,
                ): persona
                for persona in personas
            }
            for fut in as_completed(futures):
                persona = futures[fut]
                try:
                    fut.result()
                except Exception as exc:
                    raise RuntimeError(
                        f"Persona worker failed for {persona}: {exc}"
                    ) from exc
    return all_rows
