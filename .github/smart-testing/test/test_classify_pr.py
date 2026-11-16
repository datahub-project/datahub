"""Tests for classify_pr.py (run by .github/workflows/test-github-scripts.yml)."""

import fnmatch
import json
import subprocess
from pathlib import Path
from typing import Dict, List

import pytest

import classify_pr
import selection

KNOWN = {
    "feature-user": {"lineage", "search"},
    "feature-dev": {"executor"},
    "technical-layer": {"graphql"},
}


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_taxonomy_loads_every_dimension() -> None:
    known = classify_pr.load_known_tags()
    assert set(known) == {
        "feature-user",
        "feature-dev",
        "compound",
        "technical-layer",
        "entity-scope",
        "change-type",
    }
    assert "lineage" in known["feature-user"]
    assert known["change-type"] == {"ci", "build", "test-infra", "docs", "tooling"}


def test_schema_never_offers_path_assigned_tags() -> None:
    schema = classify_pr.classification_schema(classify_pr.load_known_tags())
    properties = schema["properties"]
    assert isinstance(properties, dict)
    assert "pytest-plumbing" not in properties["change-type"]["items"]["enum"]


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (".github/workflows/docker-unified.yml", {"ci"}),
        (".github/smart-testing/taxonomy-rubric.md", {"ci", "docs"}),
        ("docs/how/updating-datahub.md", {"docs"}),
        ("datahub-web-react/IMAGE.PNG", {"docs"}),
        (".claude/skills/x/scripts/tags.py", {"tooling"}),
        ("smoke-test/conftest.py", {"pytest-plumbing", "test-infra"}),
        ("smoke-test/smart_testing/taxonomy.py", {"pytest-plumbing", "test-infra"}),
        ("smoke-test/utilities/run_last_plugin.py", {"pytest-plumbing", "test-infra"}),
        ("smoke-test/tests/zdu/test_x.py", {"test-infra"}),
        ("smoke-test/tests/unit/test_shard_pack.py", {"test-infra"}),
        ("smoke-test/tests/oauth/conftest.py", {"test-infra"}),
        (".gitignore", {"tooling"}),
        # A feature directory's conftest and the e2e tests themselves are left
        # to the model, which tags the features they cover.
        ("smoke-test/tests/e2e/ai_plugins/conftest.py", set()),
        ("smoke-test/tests/e2e/lineage/test_lineage.py", set()),
        ("docker/datahub-gms/Dockerfile", set()),
        ("metadata-io/build.gradle", set()),
    ],
)
def test_change_types_for_path(path: str, expected: set) -> None:
    rules = classify_pr.load_path_rules()
    assert classify_pr.change_types_for(path, rules) == expected


def test_change_type_paths_match_tracked_files() -> None:
    # A location glob that matches nothing has gone stale after a move or
    # rename, and its files would quietly go back to costing a model call.
    # Extension globs (*.gif) name a kind of file, not a location, so skip them.
    tracked = (
        subprocess.run(
            ["git", "ls-files"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        .stdout.lower()
        .splitlines()
    )
    stale = [
        pattern
        for pattern, _ in classify_pr.load_path_rules()
        if not pattern.startswith("*.")
        and not any(fnmatch.fnmatchcase(f, pattern.lower()) for f in tracked)
    ]
    assert stale == []


def test_overlay_tags_merge_into_the_taxonomy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "taxonomy.yaml"
    base.write_text("dimensions:\n  feature-user:\n    tags:\n      - tag: lineage\n")
    overlay = tmp_path / "taxonomy.overlay.yaml"
    overlay.write_text(
        "dimensions:\n  feature-user:\n    tags:\n      - tag: chat\n"
        "  feature-dev:\n    tags:\n      - tag: billing\n"
    )
    monkeypatch.setattr(classify_pr, "TAXONOMY_YAML", base)
    monkeypatch.setattr(classify_pr, "TAXONOMY_OVERLAY_YAML", overlay)
    assert classify_pr.load_known_tags() == {
        "feature-user": {"lineage", "chat"},
        "feature-dev": {"billing"},
    }

    overlay.unlink()
    assert classify_pr.load_known_tags() == {"feature-user": {"lineage"}}


def test_overlay_change_type_paths_extend_the_path_rules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "taxonomy.yaml"
    base.write_text(
        "dimensions:\n  change-type:\n    tags:\n"
        '      - tag: ci\n        paths: [".github/*"]\n'
    )
    overlay = tmp_path / "taxonomy.overlay.yaml"
    overlay.write_text('dimensions: {}\nchange-type-paths:\n  ci: ["tools/*"]\n')
    monkeypatch.setattr(classify_pr, "TAXONOMY_YAML", base)
    monkeypatch.setattr(classify_pr, "TAXONOMY_OVERLAY_YAML", overlay)
    assert classify_pr.load_path_rules() == ((".github/*", "ci"), ("tools/*", "ci"))

    overlay.unlink()
    assert classify_pr.load_path_rules() == ((".github/*", "ci"),)


def test_llm_answer_drops_unknown_tags_and_keys() -> None:
    pr = classify_pr.pr_tags_from_llm(
        '{"feature-user": ["lineage", "made-up"], "confidence": "high",'
        ' "changed_connectors": []}',
        KNOWN,
    )
    assert pr is not None
    assert json.loads(pr.to_json()) == {
        "feature-user": ["lineage"],
        "confidence": "HIGH",
    }


def test_llm_answer_canonicalizes_tag_case() -> None:
    pr = classify_pr.pr_tags_from_llm(
        '{"feature-user": ["Lineage"], "confidence": "HIGH"}',
        KNOWN,
    )
    assert pr is not None
    assert json.loads(pr.to_json()) == {
        "feature-user": ["lineage"],
        "confidence": "HIGH",
    }


def test_fenced_llm_answer_is_invalid() -> None:
    with pytest.raises(json.JSONDecodeError):
        classify_pr.pr_tags_from_llm(
            '```json\n{"feature-user": ["lineage"], "confidence": "HIGH"}\n```',
            KNOWN,
        )


def test_classification_schema_enums_match_taxonomy() -> None:
    known = classify_pr.load_known_tags()
    schema = classify_pr.classification_schema(known)
    assert schema["additionalProperties"] is False
    assert schema["required"] == [*known.keys(), "confidence", "reasoning"]
    properties = schema["properties"]
    assert isinstance(properties, dict)
    for dim, tags in known.items():
        assert properties[dim]["items"]["enum"] == sorted(tags)
    assert properties["confidence"]["enum"] == ["HIGH", "MEDIUM", "LOW"]
    assert properties["reasoning"]["type"] == "string"


class _TextBlock:
    def __init__(self, text: str) -> None:
        self.type = "text"
        self.text = text


class _Response:
    def __init__(self, stop_reason: str, text: str) -> None:
        self.stop_reason = stop_reason
        self.content = [_TextBlock(text)]


class _Messages:
    def __init__(self, response: _Response) -> None:
        self.response = response
        self.kwargs: dict = {}

    def create(self, **kwargs: object) -> _Response:
        self.kwargs = kwargs
        return self.response


class _Client:
    def __init__(self, response: _Response) -> None:
        self.messages = _Messages(response)


def _patch_llm(
    monkeypatch: pytest.MonkeyPatch, stop_reason: str, text: str
) -> _Messages:
    client = _Client(_Response(stop_reason, text))
    monkeypatch.setattr(classify_pr, "anthropic_client", lambda: client)
    return client.messages


def test_classifier_reuses_one_client(monkeypatch: pytest.MonkeyPatch) -> None:
    # A second client would exchange the CI identity token again (HTTP 401).
    made: List[object] = []

    def make_client() -> object:
        made.append(object())
        return made[-1]

    monkeypatch.setattr(classify_pr.anthropic, "Anthropic", make_client)
    classify_pr.anthropic_client.cache_clear()
    try:
        assert classify_pr.anthropic_client() is classify_pr.anthropic_client()
        assert len(made) == 1
    finally:
        classify_pr.anthropic_client.cache_clear()


def test_call_llm_sends_taxonomy_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    messages = _patch_llm(
        monkeypatch,
        "end_turn",
        '{"feature-user": ["lineage"], "confidence": "HIGH"}',
    )
    known = classify_pr.load_known_tags()
    text = classify_pr.call_llm(_changes("src/a.py"), known)
    assert json.loads(text)["feature-user"] == ["lineage"]
    assert messages.kwargs["output_config"] == {
        "effort": "low",
        "format": {
            "type": "json_schema",
            "schema": classify_pr.classification_schema(known),
        },
    }


def test_call_llm_rejects_incomplete_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_llm(monkeypatch, "refusal", "no")
    with pytest.raises(RuntimeError):
        classify_pr.call_llm(_changes("src/a.py"), KNOWN)


def test_low_confidence_fail_safe_keeps_confidence() -> None:
    with pytest.raises(classify_pr.FailSafe) as exc:
        classify_pr.pr_tags_from_llm(
            '{"feature-user": ["lineage"], "confidence": "LOW"}', KNOWN
        )
    assert (exc.value.reason, exc.value.confidence) == (
        classify_pr.FailSafeReason.LOW_CONFIDENCE,
        "LOW",
    )


def test_unexpected_confidence_is_replaced() -> None:
    pr = classify_pr.pr_tags_from_llm(
        '{"feature-user": ["lineage"], "confidence": "HIGH\\n$(x)"}', KNOWN
    )
    assert pr.confidence == "UNKNOWN"


def test_only_unknown_feature_tags_fail_safe() -> None:
    with pytest.raises(classify_pr.FailSafe) as exc:
        classify_pr.pr_tags_from_llm(
            '{"feature-user": ["made-up"], "confidence": "HIGH"}', KNOWN
        )
    assert exc.value.reason is classify_pr.FailSafeReason.UNKNOWN_FEATURE_TAGS


def test_record_llm_output_logs_and_writes_compact_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    output = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    classify_pr.record_llm_output(
        '{\n "feature-user": ["lineage"],\n "confidence": "LOW"}'
    )
    expected = '{"confidence": "LOW", "feature-user": ["lineage"]}'
    assert f"[smart-testing] LLM output: {expected}" in capsys.readouterr().err
    assert output.read_text() == f"proposed_tags={expected}\n"


def test_record_llm_output_unparseable_is_logged_not_written(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    output = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    classify_pr.record_llm_output("not json\nmore")
    assert "not json\\nmore" in capsys.readouterr().err
    assert not output.exists()


def test_llm_output_is_logged_before_fail_safe(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(classify_pr, "get_pr_changes", lambda: _changes("src/a.py"))
    monkeypatch.setattr(
        classify_pr,
        "call_llm",
        lambda changed, known: '{"feature-dev": ["executor"], "confidence": "LOW"}',
    )
    with pytest.raises(classify_pr.FailSafe):
        classify_pr.classify(dry_run=False)
    err = capsys.readouterr().err
    assert '"feature-dev": ["executor"]' in err
    assert err.index("LLM output:") < err.index("LOW confidence")


def test_product_files_reach_the_model_before_tooling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    sent: list = []

    def _record(changed: list, known: object) -> str:
        sent.extend(f.path for f in changed)
        return '{"feature-user": ["glossary"], "confidence": "HIGH"}'

    monkeypatch.setattr(classify_pr, "call_llm", _record)
    classify_pr.classify_changes(
        _changes(".github/workflows/x.yml", "docs/a.md", "src/glossary.py"),
        dry_run=False,
    )
    assert sent[0] == "src/glossary.py"


def _changes(*paths: str) -> list:
    return [classify_pr.ChangedFile(path=p, status="modified", patch="") for p in paths]


def _run_main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Dict[str, str]:
    output = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr("sys.argv", ["classify_pr.py"])
    classify_pr.main()
    return dict(line.split("=", 1) for line in output.read_text().splitlines())


def _no_model(changed: object, known: object) -> str:
    raise AssertionError("the model was called for a PR its paths classify")


def test_harness_only_pr_is_tagged_from_paths_without_the_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(classify_pr, "call_llm", _no_model)
    pr = classify_pr.classify_changes(
        _changes(
            ".github/workflows/docker-unified.yml",
            ".github/scripts/escape_check.py",
            "smoke-test/smart_testing/escape_report.py",
            "smoke-test/tests/unit/test_smart_testing.py",
        ),
        dry_run=False,
    )
    assert json.loads(pr.to_json()) == {
        "change-type": ["ci", "pytest-plumbing", "test-infra"],
        "confidence": "HIGH",
    }
    assert selection.selection_for(pr.to_json()) == "plumbing"


def test_model_answer_keeps_path_change_types(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        classify_pr,
        "call_llm",
        lambda changed, known: (
            '{"feature-user": ["lineage"], "change-type": ["ci"], "confidence": "HIGH"}'
        ),
    )
    pr = classify_pr.classify_changes(
        _changes("metadata-io/src/main/java/Lineage.java", "smoke-test/conftest.py"),
        dry_run=False,
    )
    assert json.loads(pr.to_json()) == {
        "feature-user": ["lineage"],
        "change-type": ["ci", "pytest-plumbing", "test-infra"],
        "confidence": "HIGH",
    }


def test_model_change_type_only_answer_is_a_canary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        classify_pr,
        "call_llm",
        lambda changed, known: '{"change-type": ["build"], "confidence": "HIGH"}',
    )
    pr = classify_pr.classify_changes(_changes("buildSrc/x.gradle"), dry_run=False)
    assert json.loads(pr.to_json()) == {
        "change-type": ["build"],
        "confidence": "HIGH",
    }
    assert selection.selection_for(pr.to_json()) == "canary"


def test_docs_only_change_is_tagged_from_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        classify_pr,
        "get_pr_changes",
        lambda: _changes("docs/a.md", ".github/workflows/x.yml"),
    )
    outputs = _run_main(monkeypatch, tmp_path)
    assert json.loads(outputs["smart_tags"]) == {
        "change-type": ["ci", "docs"],
        "confidence": "HIGH",
    }
    assert outputs["failsafe_reason"] == ""
    assert outputs["confidence"] == "HIGH"
    assert outputs["model"] == classify_pr.MODEL
    assert float(outputs["classifier_duration_s"]) >= 0
    assert "proposed_tags" not in outputs


def test_large_docs_only_change_falls_back_to_p0(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    paths = [f"docs/d{i}.md" for i in range(classify_pr.CANARY_MAX_CHANGED_FILES)]
    monkeypatch.setattr(classify_pr, "get_pr_changes", lambda: _changes(*paths))
    outputs = _run_main(monkeypatch, tmp_path)
    assert outputs["smart_tags"] == ""
    assert (
        outputs["failsafe_reason"]
        == classify_pr.FailSafeReason.LARGE_PR_NO_FEATURE_TAGS.value
    )
    assert outputs["confidence"] == "HIGH"


def test_no_feature_tags_canary_only_below_threshold() -> None:
    no_features = classify_pr.PRTags(
        tags={"technical-layer": {"graphql"}}, confidence="MEDIUM"
    )
    limit = classify_pr.CANARY_MAX_CHANGED_FILES
    assert classify_pr.canary_or_fail_safe(no_features, limit - 1) is no_features
    with pytest.raises(classify_pr.FailSafe) as exc:
        classify_pr.canary_or_fail_safe(no_features, limit)
    assert exc.value.reason is classify_pr.FailSafeReason.LARGE_PR_NO_FEATURE_TAGS
    tagged = classify_pr.PRTags(tags={"feature-user": {"lineage"}}, confidence="HIGH")
    assert classify_pr.canary_or_fail_safe(tagged, 500) is tagged


def test_pr_at_compare_file_cap_runs_full_suite(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    paths = [f"src/f{i}.py" for i in range(classify_pr.COMPARE_FILE_LIMIT)]
    monkeypatch.setattr(classify_pr, "get_pr_changes", lambda: _changes(*paths))
    outputs = _run_main(monkeypatch, tmp_path)
    assert outputs["smart_tags"] == ""
    assert (
        outputs["failsafe_reason"] == classify_pr.FailSafeReason.COMPARE_FILE_CAP.value
    )
    assert outputs["confidence"] == ""


def test_failure_emits_no_tags(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    outputs = _run_main(monkeypatch, tmp_path)
    assert outputs["smart_tags"] == ""
    assert outputs["failsafe_reason"] == classify_pr.FailSafeReason.ERROR.value
    assert outputs["confidence"] == ""
    assert outputs["model"] == classify_pr.MODEL
    assert float(outputs["classifier_duration_s"]) >= 0


@pytest.mark.parametrize(
    ("labels", "changed", "argv", "reason"),
    [
        ("run-all-tests", ["src/a.py"], [], classify_pr.FailSafeReason.RUN_ALL_TESTS_LABEL),
        ("", [], [], classify_pr.FailSafeReason.NO_CHANGED_FILES),
        ("", ["src/a.py"], ["--dry-run"], classify_pr.FailSafeReason.DRY_RUN),
    ],
)
def test_pre_llm_fail_safes_report_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    labels: str,
    changed: List[str],
    argv: List[str],
    reason: "classify_pr.FailSafeReason",
) -> None:
    monkeypatch.setenv("PR_LABELS", labels)
    monkeypatch.setattr(classify_pr, "get_pr_changes", lambda: _changes(*changed))
    output = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr("sys.argv", ["classify_pr.py", *argv])
    classify_pr.main()
    outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert outputs["smart_tags"] == ""
    assert outputs["failsafe_reason"] == reason.value
    assert "proposed_tags" not in outputs


def test_unparseable_llm_output_reports_error_without_proposed_tags(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setattr(classify_pr, "get_pr_changes", lambda: _changes("src/a.py"))
    monkeypatch.setattr(classify_pr, "call_llm", lambda changed, known: "oops\nnot json")
    outputs = _run_main(monkeypatch, tmp_path)
    assert outputs["smart_tags"] == ""
    assert outputs["failsafe_reason"] == classify_pr.FailSafeReason.ERROR.value
    assert "proposed_tags" not in outputs
    assert "LLM output (unparseable): oops\\nnot json" in capsys.readouterr().err


def test_low_confidence_outputs_proposed_tags(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(classify_pr, "get_pr_changes", lambda: _changes("src/a.py"))
    monkeypatch.setattr(
        classify_pr,
        "call_llm",
        lambda changed, known: '{"feature-dev": ["executor"], "confidence": "LOW"}',
    )
    outputs = _run_main(monkeypatch, tmp_path)
    assert outputs["smart_tags"] == ""
    assert (
        outputs["failsafe_reason"] == classify_pr.FailSafeReason.LOW_CONFIDENCE.value
    )
    assert outputs["confidence"] == "LOW"
    assert json.loads(outputs["proposed_tags"])["feature-dev"] == ["executor"]


def test_tagged_pr_outputs_effective_and_proposed_tags(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(classify_pr, "get_pr_changes", lambda: _changes("src/a.py"))
    monkeypatch.setattr(
        classify_pr,
        "call_llm",
        lambda changed, known: (
            '{"feature-user": ["lineage"], "confidence": "MEDIUM", "reasoning": "r"}'
        ),
    )
    outputs = _run_main(monkeypatch, tmp_path)
    assert json.loads(outputs["smart_tags"]) == {
        "feature-user": ["lineage"],
        "confidence": "MEDIUM",
    }
    assert outputs["failsafe_reason"] == ""
    assert outputs["confidence"] == "MEDIUM"
    assert json.loads(outputs["proposed_tags"])["reasoning"] == "r"


def test_pr_changes_diff_against_merge_base(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def fake_run(args: list, **_: object) -> subprocess.CompletedProcess:
        calls.append(args)
        body = {"files": [{"filename": "a.py", "status": "added", "patch": "+x"}]}
        return subprocess.CompletedProcess(args, 0, stdout=json.dumps(body))

    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("BASE_SHA", "base1")
    monkeypatch.setenv("HEAD_SHA", "head1")
    monkeypatch.setattr(classify_pr.subprocess, "run", fake_run)
    assert classify_pr.get_pr_changes() == [
        classify_pr.ChangedFile(path="a.py", status="added", patch="+x")
    ]
    assert calls[0][-1] == "repos/org/repo/compare/base1...head1"


def test_patches_are_trimmed_to_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(classify_pr, "MAX_PATCH_BYTES_PER_FILE", 10)
    monkeypatch.setattr(classify_pr, "MAX_PATCH_BYTES_TOTAL", 40)
    changed = [
        classify_pr.ChangedFile(path=f"f{i}.py", status="modified", patch="x" * 50)
        for i in range(5)
    ]
    text = classify_pr.format_patches(changed)
    assert "### f0.py" in text
    assert "### f4.py" not in text
    assert "[... patch trimmed]" in text
