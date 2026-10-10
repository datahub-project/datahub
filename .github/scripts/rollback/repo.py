"""Git access, and the only adapter to private helpers of the shared release scripts."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from typing import Callable, Optional

import bump_schema_versions as bsv
import report_aspect_changes as rac


Reader = Callable[[str, str], str]


def cached_reader() -> Reader:
    """`rac.file_at` with a per-run cache: include walks read the same shared
    records (e.g. `CustomProperties`) for many aspects."""
    cache: dict[tuple[str, str], str] = {}

    def read(ref: str, path: str) -> str:
        if (ref, path) not in cache:
            cache[(ref, path)] = rac.file_at(ref, path)
        return cache[(ref, path)]

    return read


def read_files_at(ref: str, paths: list[str]) -> dict[str, str]:
    """Contents of many files at `ref` with one `git cat-file --batch` call."""
    if not paths:
        return {}
    proc = subprocess.run(
        ["git", "cat-file", "--batch"],
        input="".join(f"{ref}:{p}\n" for p in paths).encode(),
        capture_output=True,
        cwd=rac.REPO_ROOT,
        check=True,
    )
    out, pos, result = proc.stdout, 0, {}
    for p in paths:
        nl = out.index(b"\n", pos)
        header = out[pos:nl].decode().split()
        pos = nl + 1
        if len(header) == 3 and header[1] == "blob":
            size = int(header[2])
            result[p] = out[pos : pos + size].decode("utf-8", "replace")
            pos += size + 1
    return result


def java_files_added_between(base: str, head: str) -> set[str]:
    """Paths of .java files present at `head` but not at `base`.

    `git log --diff-filter=A base..head` also lists files re-added by root
    commits (history rewrites re-add the whole tree), which can mean tens of
    thousands of files that already existed at `base`. Filtering log entries
    through this set keeps only real additions.
    """
    try:
        out = git("diff", "--diff-filter=A", "--name-only", base, head, "--", "*.java")
    except subprocess.CalledProcessError:
        return set()
    return {line.strip() for line in out.splitlines() if line.strip()}


def first_parent_changes(
    head: str, path: str, base: str
) -> list[tuple[str, Optional[str]]]:
    """(sha, PR number) of each commit on `head`'s first-parent history since
    `base` that changed `path`, newest first. On a fork, a change merged from
    upstream shows up as the fork's merge commit, so links stay in one repo."""
    try:
        out = git(
            "log", "--first-parent", "--format=%H%x09%s", f"{base}..{head}", "--", path
        )
    except subprocess.CalledProcessError:
        return []
    changes = []
    for line in out.splitlines():
        sha, _, subject = line.partition("\t")
        if sha:
            changes.append((sha, pr_number(subject)))
    return changes


# scp-style (git@host:owner/repo) or URL-style (https, ssh, git) remotes.
_REMOTE_RE = re.compile(
    r"^(?:git@([^:]+):|(?:https?|ssh|git)://(?:[^@/]+@)?([^/:]+)(?::\d+)?/)"
    r"(.+?)(?:\.git)?/?$"
)


def repo_url() -> Optional[str]:
    """Web URL of the repository: the GitHub Actions repo, else `origin`."""
    server, name = (
        os.environ.get("GITHUB_SERVER_URL"),
        os.environ.get("GITHUB_REPOSITORY"),
    )
    if server and name:
        return f"{server}/{name}"
    try:
        remote = git("remote", "get-url", "origin").strip()
    except subprocess.CalledProcessError:
        return None
    m = _REMOTE_RE.match(remote)
    return f"https://{m.group(1) or m.group(2)}/{m.group(3)}" if m else None


def first_pr(head: str, path: str, base: str) -> Optional[str]:
    prs = rac.pr_numbers_for_file(head, path, base)
    return prs[0] if prs else None


def file_author(head: str, path: str, base: str) -> Optional[str]:
    return rac.last_author_for_file(head, path, base)


def resolve_ref_name(ref: str) -> str:
    """`ref`, or `origin/<ref>` when only the remote-tracking branch exists
    (CI checkouts usually have no local branches)."""
    return resolve_git_ref(ref, f"origin/{ref}") or ref


def resolve_sha(ref: str) -> str:
    try:
        return git("rev-parse", ref).strip()
    except subprocess.CalledProcessError:
        print(f"Error: could not resolve ref '{ref}'", file=sys.stderr)
        raise SystemExit(2)


def aspect_names_at(ref: str) -> set[str]:
    paths = [
        p
        for p in git("ls-tree", "-r", "--name-only", ref, "--", rac.PDL_PREFIX).split()
        if p.endswith(".pdl")
    ]
    names = set()
    for content in read_files_at(ref, paths).values():
        meta = rac.aspect_meta(content)
        if meta and meta.get("name"):
            names.add(meta["name"])
    return names


# Adapter: the only place that touches private helpers of the shared release
# scripts, so a change there needs fixing here only.


def git(*args: str) -> str:
    return rac._git(*args)


def strip_comments(src: str) -> str:
    return rac._strip_comments(src)


def skip_balanced(content: str, i: int) -> Optional[int]:
    return bsv._skip_balanced(content, i)


def class_and_parent_extends(content: str) -> Optional[tuple[str, str]]:
    return rac._extract_class_and_parent(content)


def pr_number(subject: str) -> Optional[str]:
    return rac._extract_pr_number(subject)


def mutator_target_aspect(content: str, constants: dict[str, str]) -> Optional[str]:
    return rac._extract_mutator_target_aspect(content, constants)


def aspect_name_constants() -> dict[str, str]:
    return rac._load_aspect_name_constants()


def resolve_git_ref(*candidates: str) -> Optional[str]:
    return rac._resolve_ref(*candidates)
