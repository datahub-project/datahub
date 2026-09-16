"""What the probe's telemetry can answer.

Telemetry was already on -- enable_auto_decorators wraps every CLI callback --
but it recorded only the function, its duration and its exit code. Which probe
command ran, and against which connector, were the two questions it could not
answer, and they are the two worth asking about an agent-facing CLI nobody has
used yet.
"""

import pathlib
from typing import Any, Dict, Iterator, List, Tuple

import pytest
from click.testing import CliRunner

from datahub.cli.recipe_cli import recipe
from datahub.telemetry import telemetry


@pytest.fixture
def pings(monkeypatch: pytest.MonkeyPatch) -> List[Tuple[str, Dict[str, Any]]]:
    captured: List[Tuple[str, Dict[str, Any]]] = []
    monkeypatch.setattr(
        telemetry.telemetry_instance,
        "ping",
        lambda event, props=None, *a, **k: captured.append((event, props or {})),
    )
    monkeypatch.setattr(telemetry.telemetry_instance, "init_tracking", lambda: None)
    monkeypatch.setattr(
        telemetry.telemetry_instance, "init_capture_exception", lambda: None
    )
    monkeypatch.setattr(
        telemetry.telemetry_instance, "capture_exception", lambda e: None
    )
    return captured


def _recipe(tmp_path: pathlib.Path) -> str:
    path = tmp_path / "r.yml"
    path.write_text(
        "source:\n"
        "  type: postgres\n"
        "  config:\n"
        "    host_port: localhost:5432\n"
        "    username: u\n"
        "    password: p\n"
        "    database: my_db\n"
    )
    return str(path)


def _probe_props(pings: List[Tuple[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return [props for event, props in pings if event == "recipe-probe"]


def test_describe_reports_which_connector_was_inspected(pings, tmp_path):
    res = CliRunner().invoke(recipe, ["describe", "postgres"])
    assert res.exit_code == 0, res.output
    assert _probe_props(pings) == [{"command": "describe", "source_type": "postgres"}]


def test_probe_methods_reports_the_connector(pings, tmp_path):
    res = CliRunner().invoke(
        recipe, ["probe", "methods", "--recipe", _recipe(tmp_path)]
    )
    assert res.exit_code == 0, res.output
    assert _probe_props(pings) == [{"command": "methods", "source_type": "postgres"}]


def test_probe_filter_reports_which_kind_was_judged(pings, tmp_path):
    res = CliRunner().invoke(
        recipe,
        [
            "probe",
            "filter",
            "--recipe",
            _recipe(tmp_path),
            "--kind",
            "Table",
            "--parent",
            "public",
            "--name",
            "orders",
        ],
    )
    assert res.exit_code == 0, res.output
    assert _probe_props(pings) == [
        {"command": "filter", "kind": "Table", "source_type": "postgres"}
    ]


def test_probe_run_reports_which_method_an_agent_called(pings, tmp_path, monkeypatch):
    """The valuable one: ten commands ship per SQL connector, and which of them
    agents actually reach for decides which are worth keeping."""
    monkeypatch.setattr(
        "datahub.cli.recipe_cli.run_probe_method",
        lambda st, cfg, cmd, kwargs: _Result(st, cmd, kwargs),
    )
    res = CliRunner().invoke(
        recipe,
        ["probe", "run", "tables", "--recipe", _recipe(tmp_path), "--schema", "public"],
    )
    assert res.exit_code == 0, res.output
    assert _probe_props(pings) == [
        {"command": "run", "probe_command": "tables", "source_type": "postgres"}
    ]


class _Result:
    def __init__(self, source_type: str, command: str, kwargs: Dict[str, Any]) -> None:
        self._d = {"source_type": source_type, "command": command, "args": kwargs}
        self.failures: List[str] = []

    def to_dict(self) -> Dict[str, Any]:
        return self._d


def test_a_method_that_could_not_reach_the_source_is_still_counted(pings, tmp_path):
    """Recorded before the call, deliberately. "Which methods do agents reach
    for" has to include the ones that did not work -- otherwise a command that
    always fails looks like a command nobody wants."""
    res = CliRunner().invoke(
        recipe,
        ["probe", "run", "tables", "--recipe", _recipe(tmp_path), "--schema", "public"],
    )
    # No server on localhost:5432 in CI, so this is a connection failure.
    assert res.exit_code == 3, res.output
    assert _probe_props(pings) == [
        {"command": "run", "probe_command": "tables", "source_type": "postgres"}
    ]


# Everything the fixture recipe states, including the two halves of host_port
# separately -- a future dimension could carry either on its own.
_RECIPE_VALUES = {
    "host_port": "localhost:5432",
    "host": "localhost",
    "port": "5432",
    "username": "u",
    "password": "p",
    "database": "my_db",
}


def _scalars(value: Any) -> Iterator[Any]:
    """Every leaf that reached telemetry, at any depth.

    Recursive because a prop value is not required to be a scalar, and a
    top-level scan would miss a recipe value nested one dict down.
    """
    if isinstance(value, dict):
        for item in value.values():
            yield from _scalars(item)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _scalars(item)
    else:
        yield value


@pytest.mark.parametrize(
    ("label", "argv"),
    [
        ("methods", ["probe", "methods"]),
        (
            "filter",
            [
                "probe",
                "filter",
                "--kind",
                "Table",
                "--parent",
                "public",
                "--name",
                "orders",
            ],
        ),
        # The error path deliberately: a connection failure is where a host or a
        # DSN is likeliest to reach a prop, and nothing reaches the source here.
        ("failed run", ["probe", "run", "tables", "--schema", "public"]),
    ],
)
def test_the_probe_carries_no_customer_data_into_telemetry(
    pings, tmp_path, label, argv
):
    """A connector name, a command name, a filter kind. Not a host, a database,
    a schema, a table name, or anything from the recipe's credentials.

    Checked against every event, not just `recipe-probe`. The per-command
    assertions above pin `recipe-probe` exactly, which is stronger than
    anything here -- but two `function-call` events fire per command and
    nothing pins those. `with_telemetry(capture_kwargs=...)` is a real option
    (see the test below for why it was not taken); turning it on would put
    `--parent public --name orders` into telemetry with every other assertion
    in this file still passing.

    Values are compared as values. The previous version rendered the props with
    repr() and searched for "'localhost'" quoted, which cannot match inside
    "'localhost:5432'" -- so the host and the port, the two worth guarding,
    were exactly the two it could not catch.
    """
    res = CliRunner().invoke(recipe, [*argv, "--recipe", _recipe(tmp_path)])
    assert res.exit_code in (0, 3), res.output

    reached = [leaf for _event, props in pings for leaf in _scalars(props)]
    assert reached, "no telemetry captured, so this would pass vacuously"

    for field, secret in _RECIPE_VALUES.items():
        for leaf in reached:
            assert str(leaf) != secret, f"{field} reached telemetry as {leaf!r}"
            # Substring only against strings: `duration` is a float, and
            # str(0.15432) contains "5432" for reasons that are not a leak.
            if isinstance(leaf, str) and len(secret) >= 4:
                assert secret not in leaf, f"{field} reached telemetry in {leaf!r}"


# --- the double-count this replaced -----------------------------------------


def test_a_probe_command_fires_exactly_one_function_call_event(pings, tmp_path):
    """with_telemetry(capture_kwargs=...) was the obvious way to add the
    dimensions above, and it would have double-counted: the decorator is applied
    automatically by cli_utils.enable_auto_decorators, and a second explicit one
    used to stack on top rather than suppress it. A separate ping avoids the
    question entirely."""
    # Through the real entrypoint group: enable_auto_decorators runs on
    # datahub.entrypoints import, so invoking the bare `recipe` group would see
    # no telemetry wrapper at all and pass for the wrong reason.
    from datahub.entrypoints import datahub

    res = CliRunner().invoke(
        datahub, ["recipe", "probe", "methods", "--recipe", _recipe(tmp_path)]
    )
    assert res.exit_code == 0, res.output
    starts = [
        props
        for event, props in pings
        if event == "function-call" and props.get("status") == "start"
    ]
    assert len(starts) == 1, starts
