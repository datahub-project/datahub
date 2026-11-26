import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from docs_impact_check import (  # noqa: E402
    COMMENT_MARKER,
    DocsContext,
    FileChange,
    PrInfo,
    classify_path,
    docs_now_cover,
    new_undocumented_env_vars,
    prefilter,
    render_comment,
    render_included_comment,
    valid_doc_paths,
)

NO_DOCS = DocsContext(paths=["docs/cli.md", "docs/deploy/environment-vars.md"], tokens={"EXISTING_VAR"})
APP_YAML = "metadata-service/configuration/src/main/resources/application.yaml"


def _pr(title: str = "feat(ingest): add thing", **kw: object) -> PrInfo:
    base = dict(
        number=1,
        title=title,
        body="",
        author="someone",
        author_is_bot=False,
        draft=False,
        labels=[],
        is_external=True,
        url="https://github.com/datahub-project/datahub/pull/1",
    )
    base.update(kw)
    return PrInfo(**base)  # type: ignore[arg-type]


def _f(path: str, patch: Optional[str] = "+x") -> FileChange:
    return FileChange(path=path, status="modified", patch=patch)


def _outcome(pr: PrInfo, files: list) -> Optional[str]:
    result, _ = prefilter(pr, files, NO_DOCS)
    return result.outcome if result else None


def test_classify_path() -> None:
    assert classify_path("docs/cli.md") == "doc"
    assert classify_path("metadata-ingestion/docs/sources/snowflake/snowflake_pre.md") == "doc"
    assert classify_path("docs/how/breaking-changes.md") == "upgrade_note"
    assert classify_path("docs/tests/metadata-tests.md") == "doc"  # published page in a "tests" folder
    assert classify_path("metadata-ingestion/tests/unit/README.md") == "ignored"
    for not_docs in (
        "docs/generated/ingestion/sources/snowflake.md",
        "docs-website/static/img/logos/platforms/sns.svg",
        "datahub-web-react/src/alchemy-components/.docs/Icons.mdx",
        "metadata-ingestion/AGENTS.md",
        "metadata-ingestion/src/datahub/cli/resources/RECIPE_AGENT_CONTEXT.md",
        "metadata-ingestion/tests/unit/test_x.py",
    ):
        assert classify_path(not_docs) == "ignored", not_docs
    assert classify_path("metadata-ingestion/src/datahub/ingestion/source/snowflake/config.py") == "user_facing"
    assert classify_path("metadata-io/src/main/java/Foo.java") == "other"


def test_prefilter_skips_cheap_cases() -> None:
    src = _f("metadata-ingestion/src/datahub/ingestion/source/x.py")
    assert _outcome(_pr(draft=True), [src]) == "skipped"
    assert _outcome(_pr(author_is_bot=True), [src]) == "skipped"
    assert _outcome(_pr(labels=["no-docs-needed"]), [src]) == "skipped"
    assert _outcome(_pr("chore(deps): bump x"), [src]) == "skipped"
    assert _outcome(_pr(), [src, _f("docs/cli.md")]) == "docs_included"
    assert _outcome(_pr(), [_f("metadata-io/src/main/java/Foo.java")]) == "skipped"


def test_upgrade_note_alone_still_goes_to_model() -> None:
    files = [_f("metadata-ingestion/src/datahub/cli/foo.py"), _f("docs/how/breaking-changes.md")]
    result, only_upgrade_note = prefilter(_pr(), files, NO_DOCS)
    assert result is None and only_upgrade_note


def test_env_rule_flags_new_undocumented_vars() -> None:
    patch = "+  enabled: ${NEW_FEATURE_ENABLED:false}\n+  other: ${EXISTING_VAR:1}\n"
    files = [_f(APP_YAML, patch)]
    assert new_undocumented_env_vars(files, NO_DOCS) == ["NEW_FEATURE_ENABLED"]
    assert _outcome(_pr(), files) == "docs_needed"


def test_env_rule_accepts_docs_in_same_pr_and_renames() -> None:
    added = _f(APP_YAML, "+  enabled: ${NEW_FEATURE_ENABLED:false}\n")
    doc = _f("docs/deploy/environment-vars.md", "+| `NEW_FEATURE_ENABLED` | false | ... |\n")
    assert new_undocumented_env_vars([added, doc], NO_DOCS) == []
    moved = _f(APP_YAML, "-  a: ${MOVED_VAR:1}\n+  b: ${MOVED_VAR:1}\n")
    assert new_undocumented_env_vars([moved], NO_DOCS) == []


def test_sync_needs_the_env_var_documented_not_just_any_page() -> None:
    env = _f(APP_YAML, "+  enabled: ${NEW_FEATURE_ENABLED:false}\n")
    unrelated_doc = _f("docs/cli.md", "+Some unrelated wording.\n")
    assert not docs_now_cover([env, unrelated_doc], NO_DOCS)
    env_doc = _f("docs/deploy/environment-vars.md", "+| `NEW_FEATURE_ENABLED` | false | ... |\n")
    assert docs_now_cover([env, unrelated_doc, env_doc], NO_DOCS)
    assert docs_now_cover([_f("metadata-ingestion/src/datahub/cli/x.py"), unrelated_doc], NO_DOCS)


def test_model_doc_paths_are_validated() -> None:
    assert valid_doc_paths(["./docs/cli.md", "docs/made-up.md"], NO_DOCS.paths) == ["docs/cli.md"]


def test_comments_are_general_optional_and_marked() -> None:
    community = render_comment(_pr(is_external=True))
    internal = render_comment(_pr(is_external=False))
    assert community != internal
    for body in (community, internal, render_included_comment(_pr())):
        assert body.startswith(COMMENT_MARKER)
        assert "Linear:" not in body  # pr-to-linear.yml keys off "Linear: <ID>" comments
    assert "optional" in community.lower() and "no-docs-needed" in community
    assert "no-docs-needed" in internal


class _FakeGitHub:
    def __init__(self, pr: PrInfo, files: list) -> None:
        self._pr, self._files, self.labels_added = pr, files, []

    def pr(self, number: int, members: object) -> PrInfo:
        return self._pr

    def files(self, number: int) -> list:
        return self._files

    def add_label(self, number: int, label: str) -> None:
        self.labels_added.append(label)


def test_merge_keeps_the_follow_up_promise_for_flagged_prs(monkeypatch, tmp_path) -> None:
    import docs_impact_check as dic

    created = []
    monkeypatch.setattr(dic, "create_linear_ticket", lambda pr, result: created.append(result) or "OSS-1")
    # Title type "chore" makes the re-check skip, as a disagreeing re-check would.
    flagged = _pr("chore: tidy", labels=["docs-needed"], is_external=False)
    gh = _FakeGitHub(flagged, [_f("metadata-ingestion/src/datahub/cli/x.py")])
    dic.cmd_merged(gh, 1, tmp_path)  # type: ignore[arg-type]
    assert len(created) == 1 and gh.labels_added == ["docs-ticket-created"]

    created.clear()
    waived = _pr("chore: tidy", labels=["docs-needed", "no-docs-needed"])
    dic.cmd_merged(_FakeGitHub(waived, []), 1, tmp_path)  # type: ignore[arg-type]
    assert created == []
