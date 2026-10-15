import subprocess
from typing import Any, List, Optional, Tuple
from unittest.mock import patch

from datahub.testing.docker_utils import _project_image_ids, _prune_images


class _FakeCompose:
    """Stands in for pytest_docker's DockerComposeExecutor.

    Only ``execute`` is ever called by the prune helpers, and it returns bytes
    (or raises) exactly as the real executor does.
    """

    def __init__(self, output: bytes = b"", error: Optional[Exception] = None) -> None:
        self.output = output
        self.error = error
        self.calls: List[str] = []

    def execute(self, subcommand: str, **kwargs: Any) -> bytes:
        self.calls.append(subcommand)
        if self.error:
            raise self.error
        return self.output


def test_project_image_ids_parses_ids() -> None:
    compose = _FakeCompose(output=b"aaa111\nbbb222\n")
    assert _project_image_ids(compose) == {"aaa111", "bbb222"}  # type: ignore[arg-type]
    assert compose.calls == ["images -q"]


def test_project_image_ids_ignores_blank_lines() -> None:
    # A project whose containers are all gone prints nothing but a newline.
    compose = _FakeCompose(output=b"\n  \naaa111\n")
    assert _project_image_ids(compose) == {"aaa111"}  # type: ignore[arg-type]


def test_project_image_ids_survives_compose_failure() -> None:
    # Bookkeeping must never fail a module whose tests passed; leaking the
    # images (the old behavior) is the acceptable degradation.
    compose = _FakeCompose(error=Exception("compose exploded"))
    assert _project_image_ids(compose) == set()  # type: ignore[arg-type]


def _run_prune(
    image_ids: set,
    *,
    ci: bool,
    returncode: int = 0,
    stderr: str = "boom",
    raises: Optional[Exception] = None,
) -> List[Tuple[Any, ...]]:
    calls: List[Tuple[Any, ...]] = []

    class _Result:
        def __init__(self) -> None:
            self.returncode = returncode
            self.stdout = ""
            self.stderr = stderr

    def fake_run(cmd: Any, **kwargs: Any) -> Any:
        calls.append(tuple(cmd))
        if raises is not None:
            raise raises
        return _Result()

    with (
        patch("datahub.testing.docker_utils.is_ci", return_value=ci),
        patch("datahub.testing.docker_utils.subprocess.run", side_effect=fake_run),
    ):
        _prune_images(image_ids)
    return calls


def test_prune_images_removes_in_ci() -> None:
    calls = _run_prune({"bbb222", "aaa111"}, ci=True)
    # Sorted so the command is deterministic and easy to read in job logs.
    assert calls == [("docker", "image", "rm", "-f", "aaa111", "bbb222")]


def test_prune_images_skipped_outside_ci() -> None:
    # Locally the images are a cache: re-pulling costs more than the disk.
    assert _run_prune({"aaa111"}, ci=False) == []


def test_prune_images_noop_without_images() -> None:
    assert _run_prune(set(), ci=True) == []


def test_prune_images_tolerates_removal_failure() -> None:
    # An image another container still holds cannot be removed; that must be a
    # warning, not an exception raised from a passing module's teardown.
    calls = _run_prune({"aaa111"}, ci=True, returncode=1)
    assert calls == [("docker", "image", "rm", "-f", "aaa111")]


def test_prune_images_survives_missing_docker_binary() -> None:
    # No docker on PATH raises FileNotFoundError from subprocess.run. Pruning
    # is an optimization, so teardown must swallow it rather than fail a module
    # whose tests all passed.
    calls = _run_prune(
        {"aaa111"}, ci=True, raises=FileNotFoundError(2, "No such file: 'docker'")
    )
    assert calls == [("docker", "image", "rm", "-f", "aaa111")]


def test_prune_images_survives_timeout() -> None:
    # A wedged daemon must not block teardown forever and stall every module
    # after this one.
    calls = _run_prune(
        {"aaa111"},
        ci=True,
        raises=subprocess.TimeoutExpired(cmd="docker image rm", timeout=300.0),
    )
    assert calls == [("docker", "image", "rm", "-f", "aaa111")]


def test_prune_images_passes_a_timeout() -> None:
    kwargs: List[Any] = []

    def fake_run(cmd: Any, **kw: Any) -> Any:
        kwargs.append(kw)

        class _R:
            returncode = 0
            stdout = ""
            stderr = ""

        return _R()

    with (
        patch("datahub.testing.docker_utils.is_ci", return_value=True),
        patch("datahub.testing.docker_utils.subprocess.run", side_effect=fake_run),
    ):
        _prune_images({"aaa111"})

    assert kwargs[0].get("timeout"), "removal must be bounded"


def test_prune_images_ignores_benign_missing_image_lines() -> None:
    # A genuine failure still reports the ids another module already pruned.
    # Those lines must not be logged alongside the real conflict.
    import logging

    with patch.object(
        logging.getLogger("datahub.testing.docker_utils"), "warning"
    ) as warn:
        _run_prune(
            {"aaa111", "bbb222"},
            ci=True,
            returncode=1,
            stderr=(
                "Error response from daemon: conflict: unable to delete aaa111 "
                "(cannot be forced) - image is being used by running container x\n"
                "Error response from daemon: No such image: sha256:bbb222\n"
            ),
        )

    assert warn.call_count == 1
    logged = warn.call_args[0][0]
    assert "conflict" in logged
    assert "No such image" not in logged


def _warnings_from_prune(**kwargs: Any) -> List[str]:
    import logging

    with patch.object(
        logging.getLogger("datahub.testing.docker_utils"), "warning"
    ) as warn:
        _run_prune({"aaa111"}, ci=True, **kwargs)
    return [call[0][0] for call in warn.call_args_list]


def test_prune_images_warns_when_failure_has_no_output() -> None:
    # A nonzero exit with empty stderr must not be silent: suppressing it would
    # make a wholly failed cleanup look identical to a clean one.
    logged = _warnings_from_prune(returncode=1, stderr="   \n")
    assert len(logged) == 1
    assert "exited 1" in logged[0]


def test_prune_images_silent_when_all_lines_are_benign() -> None:
    # Every id was already pruned by an earlier module. Nothing went wrong, so
    # this must stay quiet even if docker chose a nonzero exit status.
    logged = _warnings_from_prune(
        returncode=1, stderr="Error response from daemon: No such image: sha256:aaa111"
    )
    assert logged == []
