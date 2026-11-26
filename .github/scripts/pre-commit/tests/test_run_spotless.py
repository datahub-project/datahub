import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "run_spotless.py"
SPEC = importlib.util.spec_from_file_location("run_spotless", SCRIPT)
assert SPEC and SPEC.loader
run_spotless = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(run_spotless)

GENERATOR_SCRIPT = Path(__file__).resolve().parents[1] / "generate_pre_commit.py"
GENERATOR_SPEC = importlib.util.spec_from_file_location(
    "generate_pre_commit", GENERATOR_SCRIPT
)
assert GENERATOR_SPEC and GENERATOR_SPEC.loader
generate_pre_commit = importlib.util.module_from_spec(GENERATOR_SPEC)
sys.modules[GENERATOR_SPEC.name] = generate_pre_commit
GENERATOR_SPEC.loader.exec_module(generate_pre_commit)


def test_matching_projects_keeps_parent_and_child_projects():
    assert run_spotless._matching_projects(
        ["parent", "parent/child", "other"],
        ["parent/child/src/Foo.java"],
    ) == ["parent", "parent/child"]


def test_hook_generator_excludes_standalone_gradle_builds(tmp_path, monkeypatch):
    standalone = tmp_path / "contrib/example"
    standalone.mkdir(parents=True)
    (standalone / "settings.gradle").touch()
    monkeypatch.chdir(tmp_path)
    projects = [
        generate_pre_commit.Project(
            path="contrib/example", type=generate_pre_commit.ProjectType.JAVA
        ),
        generate_pre_commit.Project(
            path="metadata-io", type=generate_pre_commit.ProjectType.JAVA
        ),
    ]

    hook = generate_pre_commit.HookGenerator(projects)._generate_spotless_hook(projects)

    assert "contrib/example" not in hook["entry"]
    assert "metadata-io" in hook["entry"]


def test_main_batches_matching_root_projects(tmp_path, monkeypatch):
    script = tmp_path / ".github/scripts/pre-commit/run_spotless.py"
    script.parent.mkdir(parents=True)
    monkeypatch.setattr(run_spotless, "__file__", str(script))
    calls = []
    monkeypatch.setattr(
        run_spotless.subprocess,
        "call",
        lambda command, cwd: calls.append((command, cwd)) or 0,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_spotless.py",
            "--projects",
            "metadata-io,metadata-service/war",
            "--",
            "metadata-io/src/Bar.java",
            "metadata-service/war/src/Baz.java",
        ],
    )

    assert run_spotless.main() == 0
    assert calls == [
        (
            [
                str(tmp_path / "gradlew"),
                ":metadata-io:spotlessApply",
                ":metadata-service:war:spotlessApply",
                "-x",
                "generateGitPropertiesGlobal",
            ],
            tmp_path,
        ),
    ]


@pytest.mark.parametrize("exit_code", [1, 7])
def test_main_propagates_gradle_failure(tmp_path, monkeypatch, exit_code):
    script = tmp_path / ".github/scripts/pre-commit/run_spotless.py"
    script.parent.mkdir(parents=True)
    monkeypatch.setattr(run_spotless, "__file__", str(script))
    monkeypatch.setattr(
        run_spotless.subprocess, "call", lambda *args, **kwargs: exit_code
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_spotless.py",
            "--projects",
            "metadata-io",
            "--",
            "metadata-io/src/Foo.java",
        ],
    )

    assert run_spotless.main() == exit_code
