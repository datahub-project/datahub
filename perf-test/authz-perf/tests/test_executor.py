from unittest.mock import MagicMock, patch

from executor import RunConfig, run_all_personas
from lib.personas import PersonaBenchmark, PersonaOracle, Scenario
from lib.results import JsonlWriter


def test_executor_calls_login_once_per_persona(tmp_path) -> None:
    oracle = PersonaOracle(
        user_urn="urn:li:corpuser:persona-admin",
        stress_target="",
        membership_count=0,
    )
    benchmark = PersonaBenchmark(
        persona="persona-admin",
        scenarios=[
            Scenario("getMe", {}),
        ],
    )
    config = RunConfig(
        run_id="test",
        metadata={},
        gms_url="http://localhost:8080",
        frontend_url="http://localhost:9002",
        warmup=0,
        iterations=1,
        cache_phases=["warm"],
        parallel_personas=1,
        full_correctness=False,
        es_jitter=False,
        graphql_registry=MagicMock(),
    )
    writer = JsonlWriter(tmp_path / "out.jsonl")
    login_calls = []

    def fake_run(*args, **kwargs):
        login_calls.append(1)
        return MagicMock(rows=[])

    with patch("executor.run_persona_benchmark", side_effect=fake_run):
        run_all_personas(
            ["persona-admin"],
            {"persona-admin": benchmark},
            {"persona-admin": oracle},
            config,
            writer,
        )
    writer.close()
    assert len(login_calls) == 1
