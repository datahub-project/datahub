#!/usr/bin/env python3
"""
Report @Aspect PDL breaking changes between two git refs.

Auto-detects the repo flavor and picks sensible defaults:

- acryl-fork (when `acryl-main` ref exists): compares `acryl-main` against
  the latest stable `v*-cloud` release tag.
- OSS DataHub (no `acryl-main` ref): compares `master` against the latest
  stable `v*` release tag (rc and `-cloud` tags excluded).

Each mode falls back to including rc tags if no stable release exists yet.
The defaults self-adjust as new releases ship — no per-release config changes.

Usage:
    python3 .github/scripts/report_aspect_changes.py
    python3 .github/scripts/report_aspect_changes.py --base v1.0.0rc1-cloud
    python3 .github/scripts/report_aspect_changes.py --base v1.5.0 --head master
    python3 .github/scripts/report_aspect_changes.py --output report.md
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Sibling import: `.github/scripts/` is on sys.path[0] via Python's
# script-directory rule when this file is invoked as a script.
import bump_schema_versions as bsv

REPO_ROOT = Path(__file__).resolve().parents[2]
PDL_PREFIX = "metadata-models/src/main/pegasus"


def _latest_tag(
    match: str,
    *,
    exclude_substr: Optional[str] = None,
    exclude_extra: Optional[str] = None,
) -> Optional[str]:
    """Return the latest tag (version-sorted descending) matching `match`.

    Uses `git tag --list --sort=-v:refname` for a global tag enumeration —
    no ancestry constraint. Release tags in this repo often live on parallel
    hotfix branches that aren't ancestors of trunk, so an ancestry-based
    lookup (`git describe --abbrev=0`) would miss them. Pass `exclude_substr`
    to skip tags whose names contain a substring (e.g. "rc"); pass
    `exclude_extra` for a second substring filter (e.g. exclude "-cloud"
    when picking OSS tags since git's glob can't negate). Returns None if
    no matching tag is found or git fails.
    """
    try:
        out = subprocess.check_output(
            ["git", "tag", "--list", match, "--sort=-v:refname"],
            text=True,
            cwd=REPO_ROOT,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return None
    for line in out.splitlines():
        tag = line.strip()
        if not tag:
            continue
        if exclude_substr and exclude_substr in tag:
            continue
        if exclude_extra and exclude_extra in tag:
            continue
        return tag
    return None


def _resolve_ref(*candidates: str) -> Optional[str]:
    """Return the first candidate ref that resolves, or None.

    Checks each candidate via `git rev-parse --verify --quiet`. CI
    checkouts typically only have remote-tracking refs (e.g.
    `origin/acryl-main`), not the bare local branch name, so callers
    pass both forms and take whichever resolves.
    """
    for ref in candidates:
        try:
            subprocess.check_output(
                ["git", "rev-parse", "--verify", "--quiet", ref],
                cwd=REPO_ROOT,
                stderr=subprocess.DEVNULL,
            )
            return ref
        except subprocess.CalledProcessError:
            continue
    return None


def _detect_mode() -> str:
    """Return 'acryl' if any `acryl-main` ref exists in this repo, else 'oss'.

    Presence of `acryl-main` is the marker: it's a fork-only branch that
    never exists in the OSS DataHub repo. Using a branch presence check
    (rather than remote URL inspection or tag-pattern probing) keeps the
    signal stable across local renames and detached HEADs. Both the
    local branch and the `origin/` remote-tracking ref are checked so
    CI checkouts (which often only have the remote-tracking form) are
    detected correctly.
    """
    if _resolve_ref("acryl-main", "origin/acryl-main"):
        return "acryl"
    return "oss"


def _default_head(mode: Optional[str] = None) -> str:
    """Return the head ref default for the given mode (auto-detected if None).

    Prefers the local branch but falls back to the `origin/` remote-tracking
    ref so the default resolves in CI checkouts.
    """
    if mode is None:
        mode = _detect_mode()
    if mode == "acryl":
        return _resolve_ref("acryl-main", "origin/acryl-main") or "acryl-main"
    return _resolve_ref("master", "origin/master") or "master"


def resolve_base(mode: Optional[str] = None) -> str:
    """Pick the latest stable release tag for the given repo flavor.

    Looks across all tags in the repo (no ancestry constraint) and picks the
    latest version-sorted tag matching the mode's release naming convention:

    - acryl: `v*-cloud` tags. Stable cloud releases are cut on parallel
      hotfix branches that aren't ancestors of `acryl-main`, so an
      ancestry-based lookup would miss them.
    - oss: `v*` tags excluding `-cloud`. OSS release tags don't carry the
      cloud suffix; we strip cloud tags so a checkout that has both still
      picks the right one.

    Falls back to including rc tags if no stable release exists yet. The
    defaults self-maintain across releases — no hardcoded prefix to bump.

    `mode` defaults to auto-detection via `_detect_mode()`.
    """
    if mode is None:
        mode = _detect_mode()
    if mode == "acryl":
        match, exclude_extra = "v*-cloud", None
    elif mode == "oss":
        match, exclude_extra = "v*", "-cloud"
    else:
        raise ValueError(f"unknown mode: {mode!r}")

    stable = _latest_tag(
        match=match, exclude_substr="rc", exclude_extra=exclude_extra
    )
    if stable:
        return stable
    rc_fallback = _latest_tag(match=match, exclude_extra=exclude_extra)
    if rc_fallback:
        return rc_fallback
    raise SystemExit(
        f"Could not find any release tag in this repo (mode={mode}). Tried "
        f"`git tag --list {match!r} --sort=-v:refname` with and without "
        f"the rc filter. Pass --base explicitly."
    )


# ---------------------------------------------------------------------------
# PDL parsing (heuristic)
# ---------------------------------------------------------------------------

_DOC_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"//[^\n]*")


def _strip_comments(src: str) -> str:
    return _LINE_COMMENT_RE.sub("", _DOC_RE.sub("", src))


_ASPECT_BLOCK_RE = re.compile(r"@Aspect\s*=\s*\{(?P<body>[^}]*)\}", re.DOTALL)


def aspect_meta(src: str) -> Optional[dict]:
    """Return {name, schemaVersion, type} for the @Aspect annotation, or None."""
    m = _ASPECT_BLOCK_RE.search(src)
    if not m:
        return None
    body = m.group("body")
    name = re.search(r'"name"\s*:\s*"([^"]+)"', body)
    ver = re.search(r'"schemaVersion"\s*:\s*(\d+)', body)
    typ = re.search(r'"type"\s*:\s*"([^"]+)"', body)
    return {
        "name": name.group(1) if name else None,
        "schemaVersion": int(ver.group(1)) if ver else None,
        "type": typ.group(1) if typ else None,
    }


_RECORD_RE = re.compile(
    r"record\s+(?P<name>\w+)(?:\s+includes\s+[^\{]+)?\s*\{(?P<body>.*)\}",
    re.DOTALL,
)


def record_name(src: str) -> Optional[str]:
    m = _RECORD_RE.search(_strip_comments(src))
    return m.group("name") if m else None


def renamed_from(src: str) -> Optional[str]:
    m = re.search(r'@renamedFrom\s*=\s*"([^"]+)"', src)
    return m.group(1) if m else None


_FIELD_LINE_RE = re.compile(
    r"^\s*(?P<name>\w+)\s*:\s*(?P<opt>optional\s+)?(?P<type>(?:[^=\n]|\[[^\]]*\])+?)\s*(?:=\s*[^,\n]+)?\s*$"
)


def fields(src: str) -> dict[str, dict]:
    """Return {field_name: {'optional': bool, 'type': str}} for the outermost record."""
    cleaned = _strip_comments(src)
    m = _RECORD_RE.search(cleaned)
    if not m:
        return {}
    body = m.group("body")
    # Drop nested record/enum/typeref bodies so we don't pick up their fields
    body = re.sub(r"(record|enum|typeref)\s+\w+[^{}]*\{[^{}]*\}", "", body)
    out: dict[str, dict] = {}
    for raw_line in body.splitlines():
        line = re.sub(r"@\w+(?:\s*=\s*(?:\{[^}]*\}|\"[^\"]*\"|\d+))?", "", raw_line)
        if not line.strip() or line.strip().startswith("@"):
            continue
        fm = _FIELD_LINE_RE.match(line)
        if not fm:
            continue
        name = fm.group("name")
        if name in {"record", "enum", "typeref", "namespace", "import"}:
            continue
        out[name] = {
            "optional": bool(fm.group("opt")),
            "type": fm.group("type").strip().rstrip(","),
        }
    return out


_ENUM_RE = re.compile(r"enum\s+(?P<name>\w+)\s*\{(?P<body>[^}]*)\}")


def enums(src: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    cleaned = _strip_comments(src)
    for m in _ENUM_RE.finditer(cleaned):
        vals = [
            v
            for v in re.split(r"\s+", m.group("body").strip())
            if v and not v.startswith("@")
        ]
        out[m.group("name")] = vals
    return out


# ---------------------------------------------------------------------------
# Git plumbing
# ---------------------------------------------------------------------------


def _git(*args: str) -> str:
    # stderr=PIPE (not DEVNULL) preserves git's error output on
    # CalledProcessError.stderr so callers can surface it. Mirrors the
    # pattern in bump_schema_versions.py for CI debuggability.
    return subprocess.check_output(
        ["git", *args], cwd=REPO_ROOT, text=True, stderr=subprocess.PIPE
    )


def changed_pdls(base: str, head: str) -> list[str]:
    """Repo-relative paths of .pdl files that differ between base..head.

    Aborts with a non-zero exit code and a git error message if the diff
    fails (e.g. bad ref, shallow clone). Without this, CI failures would
    surface as a bare Python traceback.
    """
    try:
        out = _git("diff", "--name-only", f"{base}..{head}", "--", PDL_PREFIX)
    except subprocess.CalledProcessError as e:
        print(f"git diff {base}..{head} failed: {e.stderr or e}", file=sys.stderr)
        raise SystemExit(2)
    return [line for line in out.splitlines() if line.endswith(".pdl")]


def file_at(ref: str, path: str) -> str:
    """File content at ref, or '' if missing (added/deleted)."""
    try:
        return _git("show", f"{ref}:{path}")
    except subprocess.CalledProcessError:
        return ""


def latest_commit(ref: str, path: str, base: str) -> str:
    """Most recent commit subject for `path` in base..ref. Empty string on failure."""
    try:
        return _git("log", "-1", "--format=%h %s", f"{base}..{ref}", "--", path).strip()
    except subprocess.CalledProcessError:
        return ""


def all_commit_subjects(ref: str, path: str, base: str) -> list[str]:
    """Every commit subject on `ref` (in base..ref) that touched `path`. Newest first."""
    try:
        out = _git("log", "--format=%h %s", f"{base}..{ref}", "--", path)
        return [line for line in out.strip().splitlines() if line]
    except subprocess.CalledProcessError:
        return []


def pr_numbers_for_file(ref: str, path: str, base: str) -> list[str]:
    """All distinct PR numbers from commits touching `path` in base..ref. Sorted ascending."""
    seen: set[str] = set()
    for subject in all_commit_subjects(ref, path, base):
        pr = _extract_pr_number(subject)
        if pr:
            seen.add(pr)
    return sorted(seen, key=int)


def _commit_date(sha: str) -> str:
    """ISO-8601 commit date (YYYY-MM-DD) for `sha`. Empty string on failure."""
    try:
        full = _git("log", "-1", "--format=%cI", sha).strip()
    except subprocess.CalledProcessError:
        return ""
    return full[:10] if full else ""


def latest_commit_date_for_file(ref: str, path: str, base: str) -> Optional[str]:
    """YYYY-MM-DD of the most recent commit touching `path` in base..ref."""
    try:
        out = _git("log", "-1", "--format=%cI", f"{base}..{ref}", "--", path).strip()
    except subprocess.CalledProcessError:
        return None
    return out[:10] if out else None


def last_author_for_file(ref: str, path: str, base: str) -> Optional[str]:
    """Author name of the most recent commit touching `path` in base..ref.

    Returns None when the file had no commits in the window (e.g. transitively-
    affected aspects whose file itself wasn't directly edited).
    """
    try:
        out = _git("log", "-1", "--format=%an", f"{base}..{ref}", "--", path).strip()
    except subprocess.CalledProcessError:
        return None
    return out or None


def upstream_attribution_for_transitive(
    non_aspect_changed: list[str],
    head: str,
    base: str,
) -> tuple[list[str], Optional[str], Optional[str]]:
    """Derive PR/owner/date for a purely-transitively-affected aspect from
    the upstream changed non-aspect record(s) instead of the aspect's own
    commit history.

    A purely-transitive aspect (one in `transitive - direct_set`) has zero
    direct commits in the window, so `pr_numbers_for_file` /
    `last_author_for_file` / `latest_commit_date_for_file` all return empty
    for it. The actionable PR is whatever changed the upstream non-aspect
    record(s) that pulled this aspect into the BFS set.

    Returns `(pr_numbers, latest_author, latest_date)` aggregated across all
    changed non-aspects:
      - `pr_numbers`: every PR that touched any of the upstream sources,
        sorted ascending and deduplicated.
      - `latest_author`: author of the most recent upstream commit.
      - `latest_date`: date (YYYY-MM-DD) of the most recent upstream commit.

    Pure function over the git helpers — no side effects, safe to unit-test
    via monkeypatching of the underlying `*_for_file` helpers.
    """
    upstream_prs: set[str] = set()
    latest_date: Optional[str] = None
    latest_author: Optional[str] = None
    for src in non_aspect_changed:
        for pr in pr_numbers_for_file(head, src, base):
            upstream_prs.add(pr)
        src_date = latest_commit_date_for_file(head, src, base)
        if src_date and (latest_date is None or src_date > latest_date):
            latest_date = src_date
            latest_author = last_author_for_file(head, src, base)
    return sorted(upstream_prs, key=int), latest_author, latest_date


# ---------------------------------------------------------------------------
# Per-file classifier
# ---------------------------------------------------------------------------


# Bump-status classification per aspect (mirrors bump_schema_versions semantics:
# schemaVersion defaults to 1 when absent; any structural change OR transitive
# dependency change requires a bump).
BUMP_DONE = "bump_done"  # head schemaVersion > base AND a real change exists
BUMP_NEEDED = "bump_needed"  # changed (direct or transitive) but version not bumped
BUMP_SPURIOUS = "bump_spurious"  # version bumped but NO schema change (auto-bumper side-effect, e.g. PR #9579)
BUMP_NOT_NEEDED = "bump_not_needed"  # new file, deleted, non-aspect, no change, or version regressed — no forward bump applies
# Backwards-compat aliases. Both now resolve to BUMP_NOT_NEEDED so the
# 4-bucket classification is exhaustive: every changed PDL lands in
# bump_done / bump_needed / bump_spurious / bump_not_needed.
NOT_SURE = BUMP_NOT_NEEDED
BUMP_NA = BUMP_NOT_NEEDED

# Short human-readable descriptions used in the report legend
BUMP_STATUS_DESCRIPTIONS = {
    BUMP_DONE: "head schemaVersion > base AND a real change exists (intentional bump)",
    BUMP_NEEDED: "change exists (direct or transitive) but schemaVersion was NOT bumped",
    BUMP_SPURIOUS: "schemaVersion bumped but no actual schema change — likely auto-bumper side-effect",
    BUMP_NOT_NEEDED: "bump is not required — new file, deleted, non-aspect, unchanged, or version regressed",
}

# Captures `(#1234)` PR reference appended by GitHub squash-merge commit subjects.
_PR_REF_RE = re.compile(r"\(#(\d+)\)")


def _extract_pr_number(commit_subject: str) -> Optional[str]:
    if not commit_subject:
        return None
    m = _PR_REF_RE.search(commit_subject)
    return m.group(1) if m else None


def _bump_breakdown(findings: list["FileFinding"], status: str) -> str:
    """Group findings of `status` by PR number, list filenames per PR.

    Output shape: `(#9579: ActionRequestInfo.pdl, Status.pdl), (no-PR: Other.pdl)`
    or `(none)` if no findings match. Files with no detectable PR reference go
    under `no-PR`.
    """
    matching = [f for f in findings if f.bump_status == status]
    if not matching:
        return "(none)"
    by_pr: dict[Optional[str], list[str]] = {}
    for f in matching:
        if f.pr_numbers:
            # A file may appear under multiple PR groups when more than one PR
            # in the base..head window touched it.
            for pr in f.pr_numbers:
                by_pr.setdefault(pr, []).append(Path(f.path).name)
        else:
            by_pr.setdefault(None, []).append(Path(f.path).name)
    # Sort: numeric PRs ascending, then no-PR bucket last.
    sorted_groups = sorted(
        by_pr.items(),
        key=lambda kv: (kv[0] is None, int(kv[0]) if kv[0] else 0),
    )
    parts = []
    for pr, files in sorted_groups:
        label = f"#{pr}" if pr else "no-PR"
        parts.append(f"({label}: {', '.join(sorted(files))})")
    return ", ".join(parts)


_BUMP_WHY = {
    BUMP_DONE: "version bumped AND a real schema change exists in the window",
    BUMP_NEEDED: "schema changed (direct or transitive) but version was NOT bumped",
    BUMP_SPURIOUS: "version bumped but NO schema change in the contributing PR(s)",
    BUMP_NOT_NEEDED: "bump is not required (new file, deleted, non-aspect, unchanged, or version regressed)",
}


def _bump_needed_why(f: "FileFinding") -> str:
    """Row-specific reason for a `BUMP_NEEDED` finding.

    Distinguishes direct vs transitive change so reviewers know whether the
    file's own schema or a referenced record drove the missing bump. When
    both are true (rare), surfaces both so callers can attribute correctly.
    """
    direct = bool(f.has_structural)
    transitive_via = f.affected_via
    if direct and transitive_via:
        return (
            f"direct schema change AND transitively affected via `{transitive_via}` "
            "— schemaVersion was NOT bumped"
        )
    if transitive_via:
        return (
            f"transitively affected via `{transitive_via}` "
            "(file's own schema unchanged) — schemaVersion was NOT bumped"
        )
    if direct:
        return "direct schema change in this file — schemaVersion was NOT bumped"
    return _BUMP_WHY[BUMP_NEEDED]


_FIELD_ADD_RE = re.compile(r"added (optional|REQUIRED) field: (\S+): (.+)")
_FIELD_REMOVE_RE = re.compile(r"removed field: (.+)")
_REQ_FLIP_RE = re.compile(r"required-ness flip on (\S+): (.+)")
_TYPE_CHANGE_RE = re.compile(r"type change on (\S+): (.+)")
_ENUM_CHANGE_RE = re.compile(r"enum (\S+): (added|removed) value (\S+)")
_RENAME_RE = re.compile(r"record renamed (\S+)→(\S+)")


def _shorten_change_line(line: str) -> str:
    """Compact a verbose change-description into per-PR-table prose.

    `added optional field: foo: Bar`  → `added foo (optional)`
    `removed field: foo`              → `removed field foo`
    `required-ness flip on foo: x→y`  → `foo: x→y`
    `type change on foo: A→B`         → `type change on foo: A→B`
    `enum X: added value Y`           → `enum X: added value Y`
    `record renamed A→B (...)`        → `record renamed A→B`
    Unrecognised lines pass through verbatim.
    """
    m = _FIELD_ADD_RE.match(line)
    if m:
        opt, name, _type = m.group(1).lower(), m.group(2), m.group(3)
        return f"added {name} ({opt})"
    m = _FIELD_REMOVE_RE.match(line)
    if m:
        return f"removed field {m.group(1)}"
    m = _REQ_FLIP_RE.match(line)
    if m:
        return f"{m.group(1)}: {m.group(2)}"
    m = _TYPE_CHANGE_RE.match(line)
    if m:
        return f"type change on {m.group(1)}: {m.group(2)}"
    m = _ENUM_CHANGE_RE.match(line)
    if m:
        return f"enum {m.group(1)}: {m.group(2)} value {m.group(3)}"
    m = _RENAME_RE.match(line)
    if m:
        return f"record renamed {m.group(1)}→{m.group(2)}"
    return line


def _apply_catchup_reclassification(entries: list[dict]) -> list[dict]:
    """Reconcile schemaVersion debt across PRs chronologically.

    Walks per-PR slices in commit-date order (oldest first) maintaining the
    set of PRs whose real schema change was committed without a schemaVersion
    bump. A subsequent slice that bumps without contributing a change is
    reclassified as `bump_done` (a catch-up bump) and clears the entire
    pending debt. Once debt is cleared, further bumps without a change stay
    `bump_spurious`.

    Mutates entries in place; the new keys are `bump_status` (possibly
    flipped from BUMP_SPURIOUS to BUMP_DONE) and `catch_up_for_prs` (list of
    PRs whose unbumped changes this slice's bump covered). Returns the same
    list for chaining.
    """
    if not entries:
        return entries
    sorted_entries = sorted(
        entries,
        key=lambda e: (e.get("date") or "", e.get("pr") or ""),
    )
    pending_prs: list[str] = []
    for e in sorted_entries:
        status = e.get("bump_status")
        if status == BUMP_NEEDED:
            pr = e.get("pr")
            if pr and pr not in pending_prs:
                pending_prs.append(pr)
        elif status == BUMP_SPURIOUS:
            if pending_prs:
                e["bump_status"] = BUMP_DONE
                e["catch_up_for_prs"] = list(pending_prs)
                pending_prs = []
            # else: stays BUMP_SPURIOUS — no debt to pay
        elif status == BUMP_DONE:
            # The slice's own change + own bump implicitly clears prior debt
            # (one bump covers all pending changes).
            if pending_prs:
                e["catch_up_for_prs"] = list(pending_prs)
            pending_prs = []
    return entries


def _describe_per_pr_slice(entry: dict) -> str:
    """Synthesise a 'What happened' one-liner for a single PR-slice classification.

    Combines per-slice structural changes (additive / breaking / noisy lists)
    with the schemaVersion delta and the bump verdict to produce a compact
    prose summary suitable for a per-PR audit table.

    Lifecycle BUMP_NOT_NEEDED slices (file added new, file deleted) return
    a dedicated short description before the general path so the per-PR
    breakdown row reads naturally for those creator-PRs.
    """
    status = entry.get("bump_status")
    reason = entry.get("bump_reason") or ""

    # Lifecycle events — file added / deleted / version-regressed. These
    # slices are BUMP_NOT_NEEDED but represent real aspect-level activity.
    if status == BUMP_NOT_NEEDED:
        if reason.startswith("new aspect file"):
            return "added new aspect file (default schemaVersion = 1)"
        if reason.startswith("file deleted"):
            return "deleted aspect file"
        if reason.startswith("schemaVersion regressed"):
            return reason

    additive = entry.get("additive") or []
    breaking = entry.get("breaking") or []
    noisy = entry.get("noisy") or []
    old_v = entry.get("old_v")
    new_v = entry.get("new_v")
    via = entry.get("affected_via")

    # Skip synthetic narrator lines — the bump verdict + version delta below
    # already convey the same signal more cleanly.
    real_breaking = [
        b for b in breaking if "structural change without schemaVersion bump" not in b
    ]
    real_noisy = [
        n
        for n in noisy
        if "bumped with NO structural change" not in n
        and "transitively affected by a changed non-aspect record" not in n
    ]

    structural: list[str] = []
    for line in real_breaking + additive + real_noisy:
        structural.append(_shorten_change_line(line))

    parts: list[str] = []
    if structural:
        parts.append("; ".join(structural))

    if isinstance(old_v, int) and isinstance(new_v, int):
        if new_v > old_v:
            parts.append(f"bumped schemaVersion {old_v}→{new_v}")
        elif new_v < old_v:
            parts.append(f"schemaVersion regressed {old_v}→{new_v}")
        elif status == BUMP_NEEDED:
            parts.append("did NOT bump schemaVersion")

    if via and not structural:
        parts.append(f"transitively affected via `{via}`")

    # Catch-up annotation — this slice's bump cleared prior unbumped-change
    # debt. Set by _apply_catchup_reclassification on slices that absorbed
    # one or more pending BUMP_NEEDED PRs.
    catch_up = entry.get("catch_up_for_prs") or []
    if catch_up:
        prs_str = ", ".join(f"#{p}" for p in catch_up)
        parts.append(f"catch-up for unbumped change(s) in {prs_str}")

    if status == BUMP_SPURIOUS and not structural:
        parts.append("no schema change in this slice")

    return "; ".join(parts) if parts else "(no detail)"


def _render_release_test_pdl_file(
    findings: list["FileFinding"],
    mutators: list[dict],
    *,
    base: str,
    head: str,
    head_sha: str,
) -> str:
    """Render the standalone `release_test_pdl_changes.md` file.

    Contains a single table — the union of `bump_done`, `bump_needed`, and
    `bump_spurious` PDL files (the aspects whose `schemaVersion` activity
    changed in the window). Each row has a `Mutator` column listing any
    `AspectMigrationMutator` subclass added in this window that targets
    the same aspect, so reviewers can see which aspects have migration
    coverage and which don't.

    Returns an empty string when there are no relevant findings — the
    caller can then skip writing the file altogether.
    """
    relevant = [
        f for f in findings if f.bump_status in (BUMP_DONE, BUMP_NEEDED, BUMP_SPURIOUS)
    ]
    if not relevant:
        return ""
    # aspect_name → list[mutator class names that target it]
    by_aspect: dict[str, list[str]] = {}
    for m in mutators:
        target = m.get("target_aspect")
        if target:
            by_aspect.setdefault(target, []).append(m["class_name"])
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out: list[str] = [
        "# PDL files for integration test cases",
        "",
        f"**Base:** `{base}`  ",
        f"**Head:** `{head}` (sha: `{head_sha}`)  ",
        f"**Generated:** {generated}  ",
        f"**Total aspects:** {len(relevant)}",
        "",
        (
            "Use this list of pdl file changes to test coverage for "
            "zdu upgrade testing. The Mutator column names any "
            "AspectMigrationMutator subclass added in this window that "
            "targets the aspect (per its getAspectName())."
        ),
        "",
        "| PDL file | PR(s) | Owner | Date | Mutator |",
        "| --- | --- | --- | --- | --- |",
    ]
    ordered = sorted(
        relevant,
        key=lambda f: (f.last_commit_date or "", Path(f.path).name.lower()),
        reverse=True,
    )
    for f in ordered:
        aspect = Path(f.path).name
        prs = ", ".join(f"#{p}" for p in f.pr_numbers) if f.pr_numbers else "(no-PR)"
        owner = f.last_author or "_(no owner — file wasn't directly touched)_"
        date = f.last_commit_date or "_(unknown)_"
        mutators_for_aspect = by_aspect.get(f.aspect_name or "", [])
        mutator_cell = (
            ", ".join(f"`{cls}`" for cls in mutators_for_aspect)
            if mutators_for_aspect
            else "(none)"
        )
        out.append(f"| {aspect} | {prs} | {owner} | {date} | {mutator_cell} |")
    out.append("")
    return "\n".join(out)


def _render_per_pr_breakdown_section(
    audit: dict[str, list[dict]],
) -> list[str]:
    """Render the 'Per-PR breakdown by PDL file' section.

    One H3 table per PDL file with bump-relevant per-PR activity. Columns:
    PR | Date | Slice verdict | Author | What happened. Files sorted by their
    most recent slice date DESC; rows within each table same sort.
    """
    if not audit:
        return []

    def _file_sort_key(item: tuple[str, list[dict]]) -> tuple[str, str]:
        path, entries = item
        latest = max((e.get("date") or "" for e in entries), default="")
        return (latest, Path(path).name.lower())

    sorted_files = sorted(audit.items(), key=_file_sort_key, reverse=True)

    out: list[str] = [
        "## Per-PR breakdown by PDL file",
        "",
        (
            "For each PDL file with bump-relevant per-PR activity, one row per "
            "PR's classification of the file in isolation (`parent..commit` "
            "slice). Sorted newest-first."
        ),
        "",
    ]
    for path, entries in sorted_files:
        out.append(f"### `{Path(path).name}`")
        out.append("")
        out.append("| PR | Date | Slice verdict | Author | What happened |")
        out.append("| --- | --- | --- | --- | --- |")
        sorted_entries = sorted(
            entries,
            key=lambda e: (e.get("date") or "", e.get("pr") or ""),
            reverse=True,
        )
        for e in sorted_entries:
            pr_label = f"#{e['pr']}" if e.get("pr") else "(no-PR)"
            date = e.get("date") or "_(unknown)_"
            verdict = f"`{e['bump_status']}`"
            author = e.get("author") or "_(unknown)_"
            what = _describe_per_pr_slice(e)
            out.append(f"| {pr_label} | {date} | {verdict} | {author} | {what} |")
        out.append("")
    return out


def _file_why(f: "FileFinding") -> str:
    """Compute a file's Why text consistently across the All-PDLs section
    and the per-bucket breakdown tables.

    Priority:
      1. `f.bump_reason` — specific reason set by the classifier (most
         informative; covers new file, deleted, non-aspect, unchanged,
         version-regressed cases).
      2. For `BUMP_NEEDED`: row-specific Why from `_bump_needed_why` which
         distinguishes direct vs transitive cause and names the upstream
         record.
      3. Otherwise: the bucket-level `_BUMP_WHY[bump_status]`.

    This guarantees that for any given PDL file, the Why text in the
    All-PDLs section matches the Why text in its bump bucket table.
    """
    if f.bump_reason:
        return f.bump_reason
    if f.bump_status == BUMP_NEEDED:
        return _bump_needed_why(f)
    return _BUMP_WHY.get(f.bump_status, "")


def _bump_breakdown_table(findings: list["FileFinding"], status: str) -> list[str]:
    """Render findings of `status` as a markdown table.

    Columns: Aspect | PR(s) | Owner | Date | Why. Rows sorted by Date DESC
    (newest first), then alphabetically by aspect name when dates tie.
    Returns `["_(none)_"]` when no findings match.
    """
    matching = [f for f in findings if f.bump_status == status]
    if not matching:
        return ["_(none)_"]
    rows = [
        "| PDL file | PR(s) | Owner | Date | Why |",
        "| --- | --- | --- | --- | --- |",
    ]
    ordered = sorted(
        matching,
        key=lambda f: (f.last_commit_date or "", Path(f.path).name.lower()),
        reverse=True,
    )
    for f in ordered:
        aspect = Path(f.path).name
        prs = ", ".join(f"#{p}" for p in f.pr_numbers) if f.pr_numbers else "(no-PR)"
        owner = f.last_author or "_(no owner — file wasn't directly touched)_"
        date = f.last_commit_date or "_(unknown)_"
        why = _file_why(f)
        rows.append(f"| {aspect} | {prs} | {owner} | {date} | {why} |")
    return rows


@dataclass
class FileFinding:
    path: str
    is_aspect: bool
    aspect_name: Optional[str]
    head_commit: str
    affected_via: Optional[str] = None  # set for transitively-affected aspects
    breaking: list[str] = dc_field(default_factory=list)
    additive: list[str] = dc_field(default_factory=list)
    noisy: list[str] = dc_field(default_factory=list)
    bump_status: str = BUMP_NOT_NEEDED
    bump_reason: Optional[str] = (
        None  # specific reason when bump_status == bump_not_needed
    )
    pr_numbers: list[str] = dc_field(
        default_factory=list
    )  # ALL PRs in base..head touching this file
    last_author: Optional[str] = (
        None  # last-commit author name (owner) when the file was directly touched
    )
    last_commit_date: Optional[str] = None  # YYYY-MM-DD of most recent commit
    has_structural: bool = (
        False  # tracked so main() can re-classify with transitive context
    )
    old_v: Optional[int] = None  # schemaVersion at base (None if missing/not-aspect)
    new_v: Optional[int] = None  # schemaVersion at head (None if missing/not-aspect)
    is_purely_transitive: bool = False
    # True only when the file itself was NOT directly edited in the window
    # but was reached via the include / field-type graph from a changed
    # non-aspect. Used to gate the "(transitive)" suffix in the All-PDLs
    # Kind column so reviewers can tell pure-transitive impact apart from
    # direct edits that also happen to be transitively reached.

    def has_breaking(self) -> bool:
        return bool(self.breaking)

    def has_any(self) -> bool:
        return bool(self.breaking or self.additive or self.noisy)


def _classify_bump(
    is_aspect: bool,
    old_content: str,
    new_content: str,
    has_structural: bool,
    transitively_affected: bool = False,
) -> str:
    """Return the bump status for an aspect (delegates to _classify_bump_with_reason).

    Every PDL change falls into one of four buckets:
      - bump_done: legitimate bump
      - bump_needed: silent migration hazard
      - bump_spurious: auto-bumper side-effect
      - bump_not_needed: new file, deleted file, non-aspect, no change,
                        or version regressed — no forward bump applies
    """
    status, _reason = _classify_bump_with_reason(
        is_aspect, old_content, new_content, has_structural, transitively_affected
    )
    return status


def _classify_bump_with_reason(
    is_aspect: bool,
    old_content: str,
    new_content: str,
    has_structural: bool,
    transitively_affected: bool = False,
) -> tuple[str, Optional[str]]:
    """Same classification as _classify_bump but also returns the specific
    reason for bump_not_needed outcomes. Returns (status, reason) where
    reason is None for non-bump_not_needed statuses."""
    if not is_aspect:
        return BUMP_NOT_NEEDED, "non-aspect record (no schemaVersion concept applies)"
    if not new_content:
        return BUMP_NOT_NEEDED, "file deleted at head"
    if not old_content:
        return (
            BUMP_NOT_NEEDED,
            "new aspect file (default schemaVersion = 1; no prior version to bump from)",
        )
    old_v = (aspect_meta(old_content) or {}).get("schemaVersion")
    new_v = (aspect_meta(new_content) or {}).get("schemaVersion")
    # Per bump_schema_versions.md: missing schemaVersion is treated as 1.
    old_eff = 1 if old_v is None else old_v
    new_eff = 1 if new_v is None else new_v
    has_change = has_structural or transitively_affected
    if new_eff < old_eff:
        return (
            BUMP_NOT_NEEDED,
            f"schemaVersion regressed {old_eff}→{new_eff} (anomaly; no forward bump applies)",
        )
    if new_eff > old_eff:
        return (BUMP_DONE, None) if has_change else (BUMP_SPURIOUS, None)
    if has_change:
        return BUMP_NEEDED, None
    return BUMP_NOT_NEEDED, "no schema change in this file"


def analyze_file(path: str, base: str, head: str) -> FileFinding:
    old = file_at(base, path)
    new = file_at(head, path)
    new_meta = aspect_meta(new) or {}
    old_meta = aspect_meta(old) or {}
    # Consider both base and head: a deleted aspect has no `new` content but
    # still needs to render as [ASPECT] in the report.
    is_aspect = aspect_meta(new) is not None or aspect_meta(old) is not None
    f = FileFinding(
        path=path,
        is_aspect=is_aspect,
        aspect_name=new_meta.get("name") or old_meta.get("name"),
        head_commit=latest_commit(head, path, base),
    )
    # Track whether any actual structural changes have been found
    has_structural = False

    if not old:
        f.additive.append("new file")
        has_structural = True
    if not new:
        f.breaking.append("file deleted")
        has_structural = True
        return f

    old_name, new_name = record_name(old), record_name(new)
    if old_name and new_name and old_name != new_name:
        rf = renamed_from(new)
        # A rename is a structural change either way — the noisy-vs-breaking
        # split is about whether it was properly annotated, not whether the
        # schema changed. has_structural must be True in both branches so that
        # a legitimate rename+bump is classified as bump_done (not bump_spurious).
        has_structural = True
        if rf == old_name:
            f.noisy.append(f"record renamed {old_name}→{new_name} (renamedFrom set)")
        else:
            f.breaking.append(
                f"record renamed {old_name}→{new_name} with NO renamedFrom annotation"
            )

    old_fields, new_fields = fields(old), fields(new)
    for r in sorted(set(old_fields) - set(new_fields)):
        f.breaking.append(f"removed field: {r}")
        has_structural = True
    for a in sorted(set(new_fields) - set(old_fields)):
        opt = "optional" if new_fields[a]["optional"] else "REQUIRED"
        bucket = f.additive if new_fields[a]["optional"] else f.breaking
        bucket.append(f"added {opt} field: {a}: {new_fields[a]['type']}")
        has_structural = True
    for name in sorted(set(old_fields) & set(new_fields)):
        o, n = old_fields[name], new_fields[name]
        if o["optional"] and not n["optional"]:
            f.breaking.append(f"required-ness flip on {name}: optional→required")
            has_structural = True
        if (not o["optional"]) and n["optional"]:
            f.noisy.append(f"required-ness flip on {name}: required→optional")
            has_structural = True
        if o["type"] != n["type"]:
            f.breaking.append(f"type change on {name}: {o['type']}→{n['type']}")
            has_structural = True

    old_enums, new_enums = enums(old), enums(new)
    for ename in sorted(set(old_enums) & set(new_enums)):
        for v in sorted(set(old_enums[ename]) - set(new_enums[ename])):
            f.breaking.append(f"enum {ename}: removed value {v}")
            has_structural = True
        for v in sorted(set(new_enums[ename]) - set(old_enums[ename])):
            f.additive.append(f"enum {ename}: added value {v}")
            has_structural = True

    if is_aspect:
        # Use bump_schema_versions semantics: missing schemaVersion is treated as 1.
        # Keeps this check consistent with _classify_bump below.
        old_v_raw, new_v_raw = (
            old_meta.get("schemaVersion"),
            new_meta.get("schemaVersion"),
        )
        old_eff = 1 if old_v_raw is None else old_v_raw
        new_eff = 1 if new_v_raw is None else new_v_raw
        f.old_v = old_eff
        f.new_v = new_eff
        bumped = new_eff > old_eff
        if bumped and not has_structural and not any("renamed" in n for n in f.noisy):
            f.noisy.append(
                f"schemaVersion {old_eff}→{new_eff} bumped with NO structural change"
            )
        if has_structural and new_eff == old_eff:
            f.breaking.append("structural change without schemaVersion bump")

    f.has_structural = has_structural
    f.bump_status, f.bump_reason = _classify_bump_with_reason(
        is_aspect, old, new, has_structural
    )
    f.pr_numbers = pr_numbers_for_file(head, path, base)
    f.last_author = last_author_for_file(head, path, base)
    f.last_commit_date = latest_commit_date_for_file(head, path, base)
    return f


def find_transitive_aspects(directly_changed: list[str]) -> set[str]:
    """Surface aspects transitively affected via PDL includes or field-type refs.

    Delegates to bump_schema_versions for the reverse-graph + BFS. Returns the set
    of *aspect* file paths that depend on any of the directly-changed files.

    IMPORTANT: bsv's helpers (find_all_pdl_files, build_reverse_include_graph,
    find_transitively_affected_aspects) read files from the on-disk working tree,
    not from arbitrary git refs. The workflow always checks out the head ref, so
    the default invocation is safe. If you ever invoke with `--head <some-other-ref>`
    while the working tree is checked out elsewhere, the transitive closure
    reflects the checkout, not `head`.
    """
    all_pdl_files = bsv.find_all_pdl_files()
    reverse_graph = bsv.build_reverse_include_graph(all_pdl_files)
    return bsv.find_transitively_affected_aspects(
        directly_changed, reverse_graph, all_pdl_files
    )


def find_transitive_aspects_per_source(
    sources: list[str],
) -> dict[str, set[str]]:
    """For each source non-aspect path, compute its downstream aspect set.

    Returns `{source_path: set(aspect_paths)}` — the set of aspect files that
    transitively depend on each individual source. The reverse-include graph
    is built once and reused across all source BFS lookups for efficiency.

    Used by `_classify_window` to attribute purely-transitive aspects to the
    SPECIFIC upstream non-aspect(s) that pulled them into the transitive set,
    not the full list of changed non-aspects.
    """
    if not sources:
        return {}
    all_pdl_files = bsv.find_all_pdl_files()
    reverse_graph = bsv.build_reverse_include_graph(all_pdl_files)
    result: dict[str, set[str]] = {}
    for src in sources:
        result[src] = bsv.find_transitively_affected_aspects(
            [src], reverse_graph, all_pdl_files
        )
    return result


def render_report(
    findings: list[FileFinding],
    base: str,
    head: str,
    head_sha: str,
    audit: Optional[dict[str, list[dict]]] = None,
) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    header = [
        "# @Aspect PDL change report",
        "",
        f"**Base:** `{base}`  ",
        f"**Head:** `{head}` (sha: `{head_sha}`)  ",
        f"**Generated:** {generated}",
        "",
    ]
    if not findings:
        header.append(f"No aspect changes between `{base}` and `{head}`.")
        return "\n".join(header) + "\n"

    bump_done = sum(1 for f in findings if f.bump_status == BUMP_DONE)
    bump_needed = sum(1 for f in findings if f.bump_status == BUMP_NEEDED)
    bump_spurious = sum(1 for f in findings if f.bump_status == BUMP_SPURIOUS)
    bump_not_needed = sum(1 for f in findings if f.bump_status == BUMP_NOT_NEEDED)

    summary = (
        f"**Summary:** {len(findings)} PDL files changed · "
        f"{bump_done} bump_done · {bump_needed} bump_needed · "
        f"{bump_spurious} bump_spurious · {bump_not_needed} bump_not_needed"
    )
    bump_summary = (
        f"**Bump status:** {bump_done} bump_done · "
        f"**{bump_needed} bump_needed** · "
        f"**{bump_spurious} bump_spurious** · "
        f"{bump_not_needed} bump_not_needed"
    )
    bump_legend = (
        [
            "<details><summary>Bump status legend</summary>",
            "",
        ]
        + [
            f"- `{status}` — {desc}"
            for status, desc in BUMP_STATUS_DESCRIPTIONS.items()
        ]
        + ["", "</details>"]
    )

    breakdown_lines = ["**Bump status breakdown:**", ""]
    for status in (BUMP_DONE, BUMP_NEEDED, BUMP_SPURIOUS, BUMP_NOT_NEEDED):
        count = sum(1 for f in findings if f.bump_status == status)
        breakdown_lines.append(f"### `{status}` ({count})")
        breakdown_lines.append("")
        breakdown_lines.extend(_bump_breakdown_table(findings, status))
        breakdown_lines.append("")

    # All-PDLs table: one row per changed file, attributed to its introducing PR(s).
    # Covers every finding (aspects + nested records + transitively-affected aspects),
    # not just those with a bump_status — so the reader sees the full diff surface.
    all_pdls_lines = [
        f"## All changed PDL files ({len(findings)})",
        "",
        "| File | Kind | PR(s) | Owner | Date | Why |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    ordered_findings = sorted(
        findings,
        key=lambda f: (f.last_commit_date or "", Path(f.path).name.lower()),
        reverse=True,
    )
    for f in ordered_findings:
        kind = "ASPECT" if f.is_aspect else "nested"
        # "(transitive)" only when the file itself wasn't directly edited
        # in the window — directly-edited aspects that are ALSO reached via
        # the dependency graph keep the plain "ASPECT" label.
        if f.is_purely_transitive:
            kind = "ASPECT (transitive)"
        prs = ", ".join(f"#{p}" for p in f.pr_numbers) if f.pr_numbers else "(no-PR)"
        owner = f.last_author or "_(no owner — file wasn't directly touched)_"
        date = f.last_commit_date or "_(unknown)_"
        # All-PDLs Why matches the bump bucket Why for this file exactly,
        # so the legend and the per-row classification stay consistent.
        why = _file_why(f)
        all_pdls_lines.append(
            f"| {Path(f.path).name} | {kind} | {prs} | {owner} | {date} | {why} |"
        )
    all_pdls_lines.append("")

    out = (
        header
        + [summary, ""]
        + all_pdls_lines
        + [bump_summary, ""]
        + bump_legend
        + [""]
        + breakdown_lines
    )

    # Per-PR breakdown by PDL file — only rendered when audit data is available
    # (i.e., when called from the cumulative-mode path which collects per_pr_audit).
    if audit:
        out.extend(_render_per_pr_breakdown_section(audit))

    def section(title: str, items: list[FileFinding], collapsed: bool = False) -> None:
        if not items:
            return
        out.append(f"## {title}")
        if collapsed:
            out.append("")
            for f in items:
                out.append(f"- {f.path}")
            out.append("")
            return
        for f in items:
            tag = "ASPECT" if f.is_aspect else "nested"
            commit = f.head_commit or "(no commit info)"
            out.append(f"- [{tag}] `{f.path}` ({commit})")
            if f.aspect_name:
                out.append(f"  - aspect name: `{f.aspect_name}`")
            if f.affected_via:
                out.append(f"  - affected via: {f.affected_via}")
            if f.pr_numbers:
                out.append(f"  - PR: {', '.join('#' + pr for pr in f.pr_numbers)}")
            if f.last_author:
                out.append(f"  - owner: {f.last_author}")
            if f.is_aspect and f.bump_status != BUMP_NOT_NEEDED:
                out.append(f"  - bump: {f.bump_status}")
            for line in f.breaking:
                out.append(f"  - ⚠ BREAKING: {line}")
            for line in f.additive:
                out.append(f"  - + additive: {line}")
            for line in f.noisy:
                out.append(f"  - · noisy: {line}")
        out.append("")

    # All per-finding sections commented out by user request — bump_status
    # breakdown tables + summary line carry the actionable signal. To re-enable
    # any, also reintroduce `unchanged = [f for f in findings if not f.has_any()]`
    # above (currently removed to satisfy ruff F841).
    # section("⚠️ BREAKING", breaking)
    # section("Transitively affected aspects", transitive)
    # section("Additive", additive_only)
    # section("Noisy", noisy_only)
    # section("No logical change", unchanged, collapsed=True)
    return "\n".join(out) + "\n"


def _head_sha(ref: str) -> str:
    try:
        return _git("rev-parse", "--short", ref).strip()
    except subprocess.CalledProcessError:
        return ref


# ---------------------------------------------------------------------------
# Mutator detection (Java subclasses of AspectMigrationMutator)
# ---------------------------------------------------------------------------

_MUTATOR_BASE_CLASS = "AspectMigrationMutator"
_CLASS_EXTENDS_RE = re.compile(
    r"class\s+(\w+)(?:<[^>]+>)?[^{]*?\bextends\s+(\w+)\b",
    re.DOTALL,
)
# Match `public String getAspectName() { return X; }` where X is either a
# constant reference (e.g. `DATAHUB_VIEW_INFO_ASPECT_NAME`) or a string
# literal. `\s` matches newlines so multi-line bodies work.
_GET_ASPECT_NAME_RE = re.compile(
    r"public\s+(?:final\s+)?String\s+getAspectName\s*\(\s*\)"
    r"\s*\{\s*return\s+([A-Z_][A-Z0-9_]*|\"[^\"]+\")",
    re.DOTALL,
)
# `public static final String <FOO>_ASPECT_NAME = "fooName";` (with optional
# linebreak between `=` and the string literal).
_ASPECT_NAME_CONSTANT_RE = re.compile(
    r'public\s+static\s+final\s+String\s+(\w*ASPECT_NAME)\s*=\s*"([^"]+)"'
)
_CONSTANTS_JAVA_PATH = "li-utils/src/main/java/com/linkedin/metadata/Constants.java"


def _load_aspect_name_constants() -> dict[str, str]:
    """Parse `Constants.java` for `*_ASPECT_NAME` → string-value mappings.

    Used to resolve a mutator's `getAspectName()` return value when it
    references a constant (the common pattern in this repo) rather than a
    string literal.
    """
    try:
        content = (REPO_ROOT / _CONSTANTS_JAVA_PATH).read_text(encoding="utf-8")
    except OSError:
        return {}
    return {m.group(1): m.group(2) for m in _ASPECT_NAME_CONSTANT_RE.finditer(content)}


def _extract_mutator_target_aspect(
    java_content: str, constants_map: dict[str, str]
) -> Optional[str]:
    """Return the aspect name that this mutator targets, or None if unknown.

    Looks at `public String getAspectName() { return X; }`. If X is a string
    literal, returns it verbatim. If X is a `*_ASPECT_NAME` constant,
    resolves it via `constants_map`. Bases / abstract mutators that don't
    override `getAspectName()` return None.
    """
    m = _GET_ASPECT_NAME_RE.search(java_content)
    if not m:
        return None
    token = m.group(1)
    if token.startswith('"'):
        return token.strip('"')
    return constants_map.get(token)


def _extract_class_and_parent(java_content: str) -> Optional[tuple[str, str]]:
    """Return (class_name, parent_name) if the file declares `class X extends Y`."""
    m = _CLASS_EXTENDS_RE.search(java_content)
    if not m:
        return None
    return m.group(1), m.group(2)


def discover_mutator_hierarchy() -> set[str]:
    """Class names that directly or transitively extend AspectMigrationMutator.

    Uses `git grep` at HEAD against the working tree to find descendants. Test
    files under `/test/` are skipped — mutator audits focus on production code.
    """
    hierarchy = {_MUTATOR_BASE_CLASS}
    pending = [_MUTATOR_BASE_CLASS]
    while pending:
        parent = pending.pop()
        try:
            out = _git("grep", "-l", f"extends {parent}", "--", "*.java")
        except subprocess.CalledProcessError:
            continue
        for path in out.strip().splitlines():
            if "/test/" in path:
                continue
            try:
                content = (REPO_ROOT / path).read_text(encoding="utf-8")
            except OSError:
                continue
            cp = _extract_class_and_parent(content)
            if cp and cp[1] == parent and cp[0] not in hierarchy:
                hierarchy.add(cp[0])
                pending.append(cp[0])
    return hierarchy


def find_mutators_added_in_window(
    base: str, head: str, hierarchy: set[str]
) -> list[dict]:
    """For each .java file ADDED in base..head, check if it's a mutator subclass.

    Returns list of {sha, pr, path, class_name, parent, author, subject,
    target_aspect} dicts. `target_aspect` is the aspect name returned by
    `getAspectName()` (resolved via Constants.java when needed), or None
    for abstract/base mutators that don't override it. Test files under
    `/test/` are skipped.
    """
    results: list[dict] = []
    constants_map = _load_aspect_name_constants()
    try:
        out = _git(
            "log",
            "--diff-filter=A",
            "--name-only",
            "--format=COMMIT %H %s",
            f"{base}..{head}",
            "--",
            "*.java",
        )
    except subprocess.CalledProcessError:
        return results

    current_sha: Optional[str] = None
    current_subject: str = ""
    for line in out.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("COMMIT "):
            parts = line.split(" ", 2)
            current_sha = parts[1] if len(parts) > 1 else None
            current_subject = parts[2] if len(parts) > 2 else ""
            continue
        if not line.endswith(".java") or "/test/" in line or current_sha is None:
            continue
        try:
            content = _git("show", f"{current_sha}:{line}")
        except subprocess.CalledProcessError:
            continue
        cp = _extract_class_and_parent(content)
        if not cp:
            continue
        class_name, parent = cp
        if parent not in hierarchy:
            continue
        try:
            author = _git("log", "-1", "--format=%an", current_sha).strip() or None
        except subprocess.CalledProcessError:
            author = None
        target_aspect = _extract_mutator_target_aspect(content, constants_map)
        results.append(
            {
                "sha": current_sha[:10],
                "pr": _extract_pr_number(current_subject),
                "path": line,
                "class_name": class_name,
                "parent": parent,
                "author": author,
                "subject": current_subject,
                "commit_date": _commit_date(current_sha),
                "target_aspect": target_aspect,
            }
        )
    return results


def _render_mutator_section(mutators: list[dict]) -> list[str]:
    """Markdown section + table for newly-added mutators. Empty list if none."""
    if not mutators:
        return []
    out = [
        "## Mutators introduced in this window",
        "",
        (
            "Java classes that directly or transitively extend `AspectMigrationMutator` "
            "and were added by a commit in `base..head`. These are migration handlers "
            "for aspect schema evolution — each new mutator implements a single "
            "version-hop transform for a specific aspect, so track which PRs added them "
            "to audit migration coverage."
        ),
        "",
        "| PR | sha | Mutator class | Extends | Author | Date | Why |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    ordered = sorted(
        mutators,
        key=lambda m: (m.get("commit_date") or "", m["class_name"]),
        reverse=True,
    )
    for m in ordered:
        pr_label = f"#{m['pr']}" if m["pr"] else "(no-PR)"
        author = m["author"] or "_(unknown)_"
        date = m.get("commit_date") or "_(unknown)_"
        # Why: introducing commit subject (trimmed). For mutators this IS the
        # justification — the commit message describes the migration handler.
        subject = m.get("subject") or ""
        why = (subject[:90] + "…") if len(subject) > 90 else subject
        out.append(
            f"| {pr_label} | `{m['sha']}` | `{m['class_name']}` "
            f"| `{m['parent']}` | {author} | {date} | {why} |"
        )
    out.append("")
    return out


def _classify_window(base: str, head: str) -> list[FileFinding]:
    """Run the full classification for a single base..head window.

    Returns the list of FileFinding objects, including transitively-reclassified
    aspects and purely-transitive aspects pulled in via the BFS. Pure function:
    no rendering, no I/O beyond git plumbing.
    """
    paths = changed_pdls(base, head)
    findings = [analyze_file(p_, base, head) for p_ in paths]

    direct_set = set(paths)
    non_aspect_changed = [f.path for f in findings if not f.is_aspect]
    if not non_aspect_changed:
        return findings

    # Per-source BFS so we can attribute each transitive aspect to the
    # SPECIFIC upstream non-aspect(s) that reach it (rather than the full
    # list of changed non-aspects, which would over-attribute).
    aspects_reached_by_source = find_transitive_aspects_per_source(non_aspect_changed)
    transitive: set[str] = set().union(*aspects_reached_by_source.values())
    # Inverted map: aspect_path → list of upstream non-aspect sources that reach it
    sources_for_aspect: dict[str, list[str]] = {}
    for src, aspects in aspects_reached_by_source.items():
        for aspect in aspects:
            sources_for_aspect.setdefault(aspect, []).append(src)

    # Reclassify directly-edited aspects that are ALSO reached transitively.
    for f in findings:
        if not f.is_aspect or f.path not in transitive:
            continue
        new_content = file_at(head, f.path)
        old_content = file_at(base, f.path)
        f.bump_status, f.bump_reason = _classify_bump_with_reason(
            is_aspect=True,
            old_content=old_content,
            new_content=new_content,
            has_structural=f.has_structural,
            transitively_affected=True,
        )
        if not f.affected_via:
            f.affected_via = next(
                (
                    Path(p).stem
                    for p in non_aspect_changed
                    if Path(p).stem in new_content
                ),
                "transitive dependency",
            )
        f.noisy = [n for n in f.noisy if "bumped with NO structural change" not in n]
        if not any("transitively affected" in n for n in f.noisy):
            f.noisy.append("transitively affected by a changed non-aspect record")

    # Append purely-transitive aspects (not in direct set).
    for tpath in sorted(transitive - direct_set):
        tcontent_new = file_at(head, tpath)
        if not tcontent_new:
            continue
        tcontent_old = file_at(base, tpath)
        meta = aspect_meta(tcontent_new) or {}
        # `affected_via` should name the proximate upstream non-aspect when
        # one is referenced in this file's content; otherwise fall back to
        # the first source that's known to reach this aspect via BFS.
        relevant_sources_for_via = sources_for_aspect.get(tpath, [])
        via = next(
            (
                Path(f_).stem
                for f_ in relevant_sources_for_via
                if Path(f_).stem in tcontent_new
            ),
            (
                Path(relevant_sources_for_via[0]).stem
                if relevant_sources_for_via
                else "transitive dependency"
            ),
        )
        bump_status, bump_reason = _classify_bump_with_reason(
            is_aspect=True,
            old_content=tcontent_old,
            new_content=tcontent_new,
            has_structural=False,
            transitively_affected=True,
        )
        head_commit_subject = latest_commit(head, tpath, base)
        # Purely-transitive aspect: the file itself has no commits in the
        # window. Attribute to the SPECIFIC upstream non-aspect(s) whose BFS
        # closure reaches this aspect — not every non-aspect in the window,
        # which would over-attribute.
        relevant_sources = sources_for_aspect.get(tpath, [])
        prs, author, date = upstream_attribution_for_transitive(
            sorted(relevant_sources), head, base
        )
        findings.append(
            FileFinding(
                path=tpath,
                is_aspect=True,
                aspect_name=meta.get("name"),
                head_commit=head_commit_subject,
                affected_via=via,
                noisy=["transitively affected by a changed non-aspect record"],
                bump_status=bump_status,
                bump_reason=bump_reason,
                pr_numbers=prs,
                last_author=author,
                last_commit_date=date,
                is_purely_transitive=True,
            )
        )
    return findings


def enumerate_pdl_pr_commits(base: str, head: str) -> list[tuple[str, str]]:
    """First-parent commits in base..head that touched PDL files.

    Returns (sha, subject) pairs, newest first. Suitable for per-PR analysis
    against squash-merged PRs (one commit per PR on the first-parent line).
    """
    try:
        out = _git(
            "log",
            "--first-parent",
            "--format=%H %s",
            f"{base}..{head}",
            "--",
            PDL_PREFIX,
        )
    except subprocess.CalledProcessError as e:
        print(f"git log failed: {e.stderr or e}", file=sys.stderr)
        raise SystemExit(2)
    rows: list[tuple[str, str]] = []
    for line in out.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        sha, _, subject = line.partition(" ")
        rows.append((sha, subject))
    return rows


_LIFECYCLE_NOT_NEEDED_PREFIXES = (
    "new aspect file",  # PR that created the aspect
    "file deleted",  # PR that removed the aspect
    "schemaVersion regressed",  # PR with anomalous version regression
)


def per_pr_audit(base: str, head: str) -> dict[str, list[dict]]:
    """Run the classifier on each PDL-touching commit in base..head.

    Returns: {aspect_path: [{sha, pr, subject, bump_status, author}, ...], ...}
    sorted newest-first per aspect. Filters out per-PR slices that don't
    carry an aspect-level signal:
      - Non-aspect records (no schemaVersion concept).
      - Aspect slices classified BUMP_NOT_NEEDED with reason "no schema
        change in this file" (the file was touched in this PR but ended up
        with no aspect-relevant change — e.g. whitespace, comments).

    BUMP_NOT_NEEDED slices for **lifecycle events** (file added new, file
    deleted, schemaVersion regression) are KEPT so the per-PR breakdown
    table surfaces the creator-PR for new aspects, etc.
    """
    audit: dict[str, list[dict]] = {}
    for sha, subject in enumerate_pdl_pr_commits(base, head):
        parent = f"{sha}^"
        try:
            findings = _classify_window(parent, sha)
        except subprocess.CalledProcessError:
            continue
        pr = _extract_pr_number(subject)
        author = last_author_for_file(sha, "", base) if False else None  # placeholder
        # Author for THIS commit (not per-file): single git call
        try:
            author = _git("log", "-1", "--format=%an", sha).strip() or None
        except subprocess.CalledProcessError:
            author = None
        slice_date = _commit_date(sha)
        for f in findings:
            if not f.is_aspect:
                continue
            if f.bump_status == BUMP_NOT_NEEDED:
                reason = f.bump_reason or ""
                if not any(
                    reason.startswith(p) for p in _LIFECYCLE_NOT_NEEDED_PREFIXES
                ):
                    continue
            audit.setdefault(f.path, []).append(
                {
                    "sha": sha[:10],
                    "pr": pr,
                    "subject": subject,
                    "bump_status": f.bump_status,
                    "bump_reason": f.bump_reason,
                    "author": author,
                    "aspect_name": f.aspect_name,
                    "date": slice_date or None,
                    # Per-slice change details so the renderer can synthesize a
                    # "What happened" prose column without re-reading file content.
                    "additive": list(f.additive),
                    "breaking": list(f.breaking),
                    "noisy": list(f.noisy),
                    "old_v": f.old_v,
                    "new_v": f.new_v,
                    "affected_via": f.affected_via,
                }
            )
    # Apply chronological catch-up reclassification per file so all downstream
    # consumers (per-PR aggregator, cumulative override, renderer) see the
    # debt-reconciled verdicts. A bump-without-change PR that catches up a
    # prior unbumped change is reclassified BUMP_SPURIOUS → BUMP_DONE.
    for entries in audit.values():
        _apply_catchup_reclassification(entries)
    return audit


def render_per_pr_report(
    audit: dict[str, list[dict]], base: str, head: str, head_sha: str
) -> str:
    """Render the per-PR audit as markdown."""
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    n_aspects = len(audit)
    all_entries = [e for entries in audit.values() for e in entries]
    n_pr_slices = len({(e["sha"], e["pr"]) for e in all_entries})

    # Per-aspect roll-ups
    unjustified: list[tuple[str, list[dict]]] = []
    needed: list[tuple[str, list[dict]]] = []
    fully_justified: list[tuple[str, list[dict]]] = []
    for path, entries in sorted(audit.items(), key=lambda kv: Path(kv[0]).name.lower()):
        spurious = [e for e in entries if e["bump_status"] == BUMP_SPURIOUS]
        needs = [e for e in entries if e["bump_status"] == BUMP_NEEDED]
        done = [e for e in entries if e["bump_status"] == BUMP_DONE]
        if spurious:
            unjustified.append((path, entries))
        elif needs:
            needed.append((path, entries))
        elif done:
            fully_justified.append((path, entries))

    out: list[str] = [
        "# @Aspect PDL change report — per-PR audit",
        "",
        f"**Base:** `{base}`  ",
        f"**Head:** `{head}` (sha: `{head_sha}`)  ",
        f"**Generated:** {generated}",
        "",
        (
            f"**Audit:** {n_pr_slices} PR-slice(s) examined · "
            f"{n_aspects} aspect(s) with at least one bump-relevant verdict · "
            f"**{len(unjustified)} with unjustified bumps** · "
            f"**{len(needed)} with missing bumps** · "
            f"{len(fully_justified)} fully justified"
        ),
        "",
        "Each row below is one PR's classification of the aspect _in isolation_ "
        "(`parent..commit` slice), so spurious bumps that are absorbed in a "
        "cumulative view show up here.",
        "",
    ]

    def _entries_to_pr_table(entries: list[dict]) -> list[str]:
        rows = [
            "| PR | sha | bump | author | commit |",
            "| --- | --- | --- | --- | --- |",
        ]
        for e in entries:
            pr_label = f"#{e['pr']}" if e["pr"] else "(no-PR)"
            author = e["author"] or "_(unknown)_"
            subject = e["subject"][:80] + ("…" if len(e["subject"]) > 80 else "")
            rows.append(
                f"| {pr_label} | `{e['sha']}` | `{e['bump_status']}` | {author} | {subject} |"
            )
        return rows

    if unjustified:
        out.append("## ⚠️ Aspects with at least one unjustified bump (`bump_spurious`)")
        out.append("")
        out.append(
            "These aspects had at least one PR-slice classify their bump as "
            "`bump_spurious` — a `schemaVersion` increment in a slice that "
            "made no schema change. Each of these violates the "
            "_v→v+1 per justifying PR_ invariant."
        )
        out.append("")
        for path, entries in unjustified:
            out.append(f"### `{Path(path).name}`")
            out.append("")
            out.extend(_entries_to_pr_table(entries))
            out.append("")

    if needed:
        out.append("## 🟡 Aspects with at least one missing bump (`bump_needed`)")
        out.append("")
        out.append(
            "These aspects had at least one PR-slice classify them as "
            "`bump_needed` — schema changed in that slice but the version "
            "was NOT bumped. Silent migration hazards."
        )
        out.append("")
        for path, entries in needed:
            out.append(f"### `{Path(path).name}`")
            out.append("")
            out.extend(_entries_to_pr_table(entries))
            out.append("")

    if fully_justified:
        out.append("## ✅ Aspects with all bumps justified")
        out.append("")
        out.append(
            "Every PR-slice that bumped these aspects also contained a real schema change."
        )
        out.append("")
        out.append("| Aspect | Bumps | Justifying PR(s) |")
        out.append("| --- | --- | --- |")
        for path, entries in fully_justified:
            done = [e for e in entries if e["bump_status"] == BUMP_DONE]
            prs = ", ".join(
                sorted({f"#{e['pr']}" if e["pr"] else "(no-PR)" for e in done})
            )
            out.append(f"| {Path(path).name} | {len(done)} | {prs} |")
        out.append("")

    if not (unjustified or needed or fully_justified):
        out.append("_No aspects with bump-relevant PR-slice verdicts in this window._")
        out.append("")

    return "\n".join(out) + "\n"


def _aggregate_per_pr_verdict(entries: list[dict]) -> str:
    """Reduce a list of per-PR-slice verdicts to a single cumulative status,
    honoring catch-up reconciliation.

    Priority order (highest wins): bump_spurious > bump_needed > bump_done >
    bump_not_needed. A BUMP_NEEDED entry is **ignored** when its PR appears
    in any later slice's `catch_up_for_prs` — that NEEDED slice has already
    been reconciled by a catch-up bump and is no longer an unpaid debt.

    The cumulative bucket therefore follows the per-PR truth: a file lands
    in `bump_spurious` whenever any slice has an unreconciled spurious bump,
    even if cumulative-diff math would call the window `bump_done`.
    """
    paid_prs: set[str] = set()
    for e in entries:
        for pr in e.get("catch_up_for_prs") or []:
            paid_prs.add(pr)
    has_spurious = any(e["bump_status"] == BUMP_SPURIOUS for e in entries)
    has_unreconciled_needed = any(
        e["bump_status"] == BUMP_NEEDED and e.get("pr") not in paid_prs for e in entries
    )
    has_done = any(e["bump_status"] == BUMP_DONE for e in entries)
    if has_spurious:
        return BUMP_SPURIOUS
    if has_unreconciled_needed:
        return BUMP_NEEDED
    if has_done:
        return BUMP_DONE
    return BUMP_NOT_NEEDED


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--base",
        default=None,
        help=(
            "baseline ref (default: auto-detected — latest stable v*-cloud tag "
            "in acryl-fork repos, latest stable v* tag in OSS DataHub; falls "
            "back to rc tags if no stable release exists yet)"
        ),
    )
    p.add_argument(
        "--head",
        default=None,
        help=(
            "head ref (default: auto-detected — `acryl-main` in acryl-fork "
            "repos, `master` in OSS DataHub)"
        ),
    )
    p.add_argument(
        "--output", default=None, help="write markdown to FILE (default: stdout)"
    )
    p.add_argument(
        "--per-pr",
        action="store_true",
        help=(
            "Run per-PR audit mode: classify each PDL-touching commit in base..head "
            "in isolation, then aggregate. Catches the v→v+1 invariant per PR "
            "(e.g. PR #9579-style spurious bumps that wide cumulative windows mask)."
        ),
    )
    args = p.parse_args(argv)

    # Resolve mode once so base and head defaults stay consistent (and we
    # don't pay for two `git rev-parse` probes).
    mode = _detect_mode()
    base = args.base or resolve_base(mode)
    head = args.head or _default_head(mode)
    head_sha = _head_sha(head)

    if args.per_pr:
        audit = per_pr_audit(base, head)
        report = render_per_pr_report(
            audit, base=base, head=head, head_sha=head_sha
        )
    else:
        findings = _classify_window(base, head)
        # Enrich cumulative bump_status with per-PR-aware verdicts: walk each
        # PR in the window, classify in isolation, then override the
        # cumulative status with the highest-priority per-PR verdict. This
        # ensures a PR that bumped spuriously (e.g. PR #9579) shows up as
        # bump_spurious in the cumulative report even when other PRs' real
        # changes would have absorbed it under pure cumulative-diff math.
        try:
            audit = per_pr_audit(base, head)
        except subprocess.CalledProcessError:
            audit = {}
        for f in findings:
            if f.path not in audit:
                continue
            new_status = _aggregate_per_pr_verdict(audit[f.path])
            if new_status != f.bump_status:
                # Per-PR aggregation drives the cumulative bucket. The
                # aggregator already honors catch-up reconciliation, so a
                # NEEDED slice paid down by a later DONE slice no longer
                # holds the file back from bump_done. The cumulative
                # bump_reason no longer matches the new status — clear it
                # so the renderer falls back to _BUMP_WHY[new_status] or
                # per-row Why helpers.
                f.bump_status = new_status
                f.bump_reason = None
        report = render_report(
            findings,
            base=base,
            head=head,
            head_sha=head_sha,
            audit=audit,
        )

    # Append mutator section (subclasses of AspectMigrationMutator added in window).
    # Same data in both modes — it's a window-level audit, not per-PR.
    mutators = find_mutators_added_in_window(
        base, head, discover_mutator_hierarchy()
    )
    mutator_section = _render_mutator_section(mutators)
    if mutator_section:
        report = report.rstrip("\n") + "\n\n" + "\n".join(mutator_section) + "\n"

    # Standalone integration-test sidecar file: `release_test_pdl_changes.md`
    # next to `--output`. Cumulative mode only (per-PR mode's findings aren't
    # available here). Contains the union of bump_done + bump_needed +
    # bump_spurious aspects with a `Mutator` column matched via the mutator's
    # `getAspectName()`.
    if not args.per_pr:
        sidecar = _render_release_test_pdl_file(
            findings,
            mutators,
            base=base,
            head=head,
            head_sha=head_sha,
        )
        if sidecar and args.output:
            sidecar_path = Path(args.output).parent / "release_test_pdl_changes.md"
            sidecar_path.write_text(sidecar)

    if args.output:
        Path(args.output).write_text(report)
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
