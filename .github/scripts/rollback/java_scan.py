"""New AspectMigrationMutators and upgrade steps between two refs."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Optional

import report_aspect_changes as rac

from rollback import model, repo


_METHOD_RETURN_INT_RE_CACHE: dict[str, re.Pattern[str]] = {}


def _extract_method_return_int(java_content: str, method_name: str) -> Optional[int]:
    if method_name not in _METHOD_RETURN_INT_RE_CACHE:
        _METHOD_RETURN_INT_RE_CACHE[method_name] = re.compile(
            rf"{method_name}\s*\(\s*\)\s*\{{\s*return\s+(\d+)[Ll]?\s*;",
            re.DOTALL,
        )
    m = _METHOD_RETURN_INT_RE_CACHE[method_name].search(java_content)
    return int(m.group(1)) if m else None


def find_mutators_added_in_window(
    base: str, head: str, hierarchy: set[str]
) -> list[dict]:
    """Like `rac.find_mutators_added_in_window`, but only for files absent at `base`.

    Rollback cares about mutators N has and N-1 lacks, so mutators backported
    to N-1 are not new. Kept separate so the PDL change report is unchanged.
    """
    results: list[dict] = []
    constants_map = repo.aspect_name_constants()
    try:
        out = repo.git(
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

    added = repo.java_files_added_between(base, head)
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
        if (
            not line.endswith(".java")
            or "/test/" in line
            or current_sha is None
            or line not in added
        ):
            continue
        try:
            # Classify the file as it is at `head`: a later commit in the
            # window can turn it into a mutator or step.
            content = repo.git("show", f"{head}:{line}")
        except subprocess.CalledProcessError:
            continue
        cp = repo.class_and_parent_extends(content)
        if not cp:
            continue
        class_name, parent = cp
        if parent not in hierarchy:
            continue
        try:
            author = repo.git("log", "-1", "--format=%an", current_sha).strip() or None
        except subprocess.CalledProcessError:
            author = None
        results.append(
            {
                "sha": current_sha[:10],
                "pr": repo.pr_number(current_subject),
                "path": line,
                "class_name": class_name,
                "parent": parent,
                "author": author,
                "subject": current_subject,
                "target_aspect": repo.mutator_target_aspect(content, constants_map),
            }
        )
    return results


def classify_mutators_for_rollback(
    current: str, target: str
) -> list[model.RollbackFinding]:
    """Classify new mutators for rollback risk.

    Rollback uses retention + Kafka replay (not reverse transforms). All new
    mutators are flagged as requires_attention — the operator must verify that
    the retention window and Kafka replay cover the mutated data.
    """
    hierarchy = rac.discover_mutator_hierarchy()
    mutators = find_mutators_added_in_window(target, current, hierarchy)

    # Deduplicate by (path, class_name) — same mutator touched by multiple PRs
    # should be a single finding with merged PR numbers.
    seen: dict[tuple[str, str], dict] = {}
    for m in mutators:
        key = (m["path"], m["class_name"])
        if key not in seen:
            seen[key] = {**m, "_prs": []}
        pr = m.get("pr")
        if pr and pr not in seen[key]["_prs"]:
            seen[key]["_prs"].append(pr)

    findings: list[model.RollbackFinding] = []
    for (path, _cls), m in seen.items():
        content = rac.file_at(current, path)
        if not content:
            continue

        src_v = _extract_method_return_int(content, "getSourceVersion")
        tgt_v = _extract_method_return_int(content, "getTargetVersion")
        hop = f"v{src_v}→v{tgt_v}" if src_v is not None and tgt_v is not None else ""

        hop_label = f" ({hop})" if hop else ""
        summary = f"New mutator `{m['class_name']}`{hop_label} — check what it changes"
        pr_str = ", ".join(m["_prs"]) if m["_prs"] else None

        findings.append(
            model.RollbackFinding(
                dimension=model.DIM_MUTATOR,
                risk=model.REQUIRES_ATTENTION,
                path=path,
                aspect_name=m.get("target_aspect"),
                summary=summary,
                detail=None,  # set by set_mutator_impact from the field changes
                pr_number=pr_str,
                author=m.get("author"),
                subject=m["class_name"],
                hop=hop or None,
            )
        )

    return findings


_BLOCKING_STEP = "BlockingSystemUpgrade"
_NON_BLOCKING_STEP = "NonBlockingSystemUpgrade"
IMPLEMENTS_STEP_RE = re.compile(
    rf"\bimplements\s+[^{{]*?\b({_BLOCKING_STEP}|{_NON_BLOCKING_STEP})\b"
)
_CLASS_NAME_RE = re.compile(r"\bclass\s+(\w+)")


def _class_and_parent(content: str) -> Optional[tuple[str, Optional[str]]]:
    """(class name, superclass or None). Most upgrade steps only `implement`
    their interface, so a missing `extends` must not drop the class."""
    cp = repo.class_and_parent_extends(content)
    if cp:
        return cp
    m = _CLASS_NAME_RE.search(content)
    return (m.group(1), None) if m else None


def discover_upgrade_step_hierarchy() -> dict[str, str]:
    """Class names that implement BlockingSystemUpgrade or NonBlockingSystemUpgrade,
    directly or through a superclass. Returns {class_name: step_type}.

    Searches one inheritance level at a time with a single `git grep` per
    level, since most steps only `implement` the interface and a grep per
    class is slow on this repo.
    """
    hierarchy: dict[str, str] = {}
    frontier = {_BLOCKING_STEP: _BLOCKING_STEP, _NON_BLOCKING_STEP: _NON_BLOCKING_STEP}
    while frontier:
        names = "|".join(sorted(frontier))
        # Plain ERE (no \\b): a broad pre-filter, each file is checked below.
        pattern = rf"(implements|extends)[^{{]*({names})"
        try:
            out = repo.git("grep", "-l", "-E", pattern, "--", "*.java")
        except subprocess.CalledProcessError:
            break
        next_frontier: dict[str, str] = {}
        for path in out.strip().splitlines():
            if "/test/" in path:
                continue
            try:
                content = Path(rac.REPO_ROOT / path).read_text(encoding="utf-8")
            except OSError:
                continue
            cp = _class_and_parent(content)
            if not cp or cp[0] in hierarchy:
                continue
            m = IMPLEMENTS_STEP_RE.search(content)
            root = m.group(1) if m else frontier.get(cp[1] or "")
            if root:
                hierarchy[cp[0]] = root
                next_frontier[cp[0]] = root
        frontier = next_frontier
    return hierarchy


def find_upgrade_steps_added_in_window(base: str, head: str) -> list[dict]:
    hierarchy = discover_upgrade_step_hierarchy()
    results: list[dict] = []
    try:
        out = repo.git(
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

    added = repo.java_files_added_between(base, head)
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
        if (
            not line.endswith(".java")
            or "/test/" in line
            or current_sha is None
            or line not in added
        ):
            continue
        try:
            # Classify the file as it is at `head`: a later commit in the
            # window can turn it into a mutator or step.
            content = repo.git("show", f"{head}:{line}")
        except subprocess.CalledProcessError:
            continue

        cp = _class_and_parent(content)
        if not cp:
            continue
        class_name, parent = cp

        step_type: Optional[str] = None
        m = IMPLEMENTS_STEP_RE.search(content)
        if m:
            step_type = m.group(1)
        elif parent in hierarchy:
            step_type = hierarchy[parent]

        if not step_type:
            continue

        try:
            author = repo.git("log", "-1", "--format=%an", current_sha).strip() or None
        except subprocess.CalledProcessError:
            author = None

        results.append(
            {
                "sha": current_sha[:10],
                "pr": repo.pr_number(current_subject),
                "path": line,
                "class_name": class_name,
                "step_type": step_type,
                "author": author,
                "subject": current_subject,
            }
        )
    return results


def classify_upgrade_steps_for_rollback(
    current: str, target: str
) -> list[model.RollbackFinding]:
    # The same file can be added by several commits in the window (e.g. a
    # revert and re-land); report each step once with all its PRs.
    seen: dict[tuple[str, str], dict] = {}
    for s in find_upgrade_steps_added_in_window(target, current):
        key = (s["path"], s["class_name"])
        entry = seen.setdefault(key, {**s, "_prs": []})
        if s.get("pr") and s["pr"] not in entry["_prs"]:
            entry["_prs"].append(s["pr"])
    findings: list[model.RollbackFinding] = []
    for s in seen.values():
        findings.append(
            model.RollbackFinding(
                dimension=model.DIM_UPGRADE_STEP,
                risk=model.REQUIRES_ATTENTION,
                path=s["path"],
                aspect_name=None,
                **model.impact(model.UNKNOWN, model.UNKNOWN, model.UNKNOWN),
                summary=f"New {s['step_type']}: `{s['class_name']}`",
                detail="Verify idempotency and rollback safety",
                pr_number=", ".join(s["_prs"]) or None,
                author=s.get("author"),
                subject=s["class_name"],
            )
        )
    return findings


_ASPECT_CONST_RE = re.compile(r"\b([A-Z][A-Z0-9_]*_ASPECT_NAME)\b")
# Steps often write through side effects; their docs name the aspect, e.g.
# "a denormalized {@code dataProducts} aspect".
_DOC_ASPECT_RE = re.compile(r"\{@code\s+(\w+)\}\s+aspect")


def step_aspects(step_path: str, ref: str, constants: dict[str, str]) -> set[str]:
    """Aspects an upgrade step's own files reference, by constant or in docs.
    Reads the step class plus its `<Class>*` and `Abstract*` siblings."""
    directory, filename = step_path.rsplit("/", 1)
    cls = filename[: -len(".java")]
    try:
        listing = repo.git("ls-tree", "--name-only", ref, f"{directory}/")
    except subprocess.CalledProcessError:
        return set()
    files = [
        p
        for p in listing.split()
        if p.endswith(".java")
        and (
            p.rsplit("/", 1)[-1].startswith(cls)
            or p.rsplit("/", 1)[-1].startswith("Abstract")
        )
    ]
    aspects: set[str] = set()
    for content in repo.read_files_at(ref, files).values():
        aspects |= {
            constants[c] for c in _ASPECT_CONST_RE.findall(content) if c in constants
        }
        aspects |= set(_DOC_ASPECT_RE.findall(content))
    return aspects
