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
    image_ids: set, *, ci: bool, returncode: int = 0
) -> List[Tuple[Any, ...]]:
    calls: List[Tuple[Any, ...]] = []

    class _Result:
        def __init__(self) -> None:
            self.returncode = returncode
            self.stdout = ""
            self.stderr = "boom"

    def fake_run(cmd: Any, **kwargs: Any) -> Any:
        calls.append(tuple(cmd))
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
