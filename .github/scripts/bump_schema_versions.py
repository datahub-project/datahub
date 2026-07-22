#!/usr/bin/env python3
"""
CLI tool to bump schemaVersion annotations on changed PDL aspect files.

Compares PDL files against a base branch and increments the schemaVersion
annotation on any aspect that has changed — including aspects that transitively
depend on a changed record via PDL `includes` or field-type references.

Rules:
  - Only aspects (files with @Aspect annotation) are versioned
  - Default version if not specified is 1
  - Any change bumps to base_branch_version + 1
  - New files (not on base branch) already default to version 1; no write needed
  - If a non-aspect record A is included by aspect B and A changes, B is bumped
  - If a non-aspect record A is referenced as a field type in aspect B
    (e.g. `schedule: optional A`) and A changes, B is bumped
  - The check is skipped entirely (exit 0, no bump) when the base is a
    release (releases/*) or hotfix (hotfixes/*) branch. CI classifies the
    explicit --base-branch; the local pre-commit hook infers the nearest
    ancestor branch. OSS repos have no such branches, so this never fires there.

Environment variables:
  PDL_ROOTS    Colon-separated list of PDL source roots to scan.
               Default: metadata-models/src/main/pegasus
  MAIN_BRANCH  Base branch fallback when origin/HEAD is not configured locally.
               Default: master

Usage:
    python3 scripts/bump_schema_versions.py
    python3 scripts/bump_schema_versions.py --base-branch master
    python3 scripts/bump_schema_versions.py --dry-run --verbose
    python3 scripts/bump_schema_versions.py --check   # CI: fail if a bump is missing
    PDL_ROOTS="metadata-models/src/main/pegasus:other/src/main/pegasus" python3 scripts/bump_schema_versions.py
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict, deque
from pathlib import Path


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------


def detect_default_branch() -> str:
    """
    Detect the repo's default branch.

    Resolution order:
    1. git symbolic-ref refs/remotes/origin/HEAD  (fast, offline)
    2. MAIN_BRANCH environment variable
    3. Hard fallback to 'master'
    """
    result = subprocess.run(
        ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return result.stdout.strip().split("/")[-1]

    if env_branch := os.environ.get("MAIN_BRANCH"):
        return env_branch

    return "master"


# Branch-name prefixes for release and hotfix branches. A branch whose base is
# one of these represents a targeted release/hotfix line, not trunk — the
# schemaVersion bump check must not run against it (it would demand pulling
# unrelated trunk schema churn into the release). Matches the convention in
# .github/workflows/post-workflow-actions.yml.
RELEASE_BRANCH_PREFIXES = ("releases/", "hotfixes/")

_REMOTE_REF_PREFIXES = ("refs/remotes/origin/", "origin/")


def is_release_or_hotfix_branch(name: str) -> bool:
    """Return True if name refers to a releases/* or hotfixes/* branch.

    Accepts bare names ('releases/x'), remote-tracking short names
    ('origin/releases/x'), and full refs ('refs/remotes/origin/releases/x').
    The match is a prefix on the branch portion — 'feature/releases-x' is not
    a release branch.
    """
    for prefix in _REMOTE_REF_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    return name.startswith(RELEASE_BRANCH_PREFIXES)


def _list_release_hotfix_refs() -> list[str]:
    """Return short names of all local/remote releases/* and hotfixes/* refs.

    Empty on git failure or when none exist (e.g. OSS DataHub).
    """
    result = subprocess.run(
        [
            "git",
            "for-each-ref",
            "--format=%(refname:short)",
            "refs/remotes/*/releases/*",
            "refs/remotes/*/hotfixes/*",
            "refs/heads/releases/*",
            "refs/heads/hotfixes/*",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []
    return [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]


def _merge_base_distance(ref: str) -> int | None:
    """Return the number of commits from merge-base(HEAD, ref) to HEAD.

    Smaller means ref is a nearer ancestor of HEAD. Returns None if ref does
    not resolve or shares no history with HEAD.
    """
    mb = subprocess.run(
        ["git", "merge-base", "HEAD", ref], capture_output=True, text=True
    )
    if mb.returncode != 0 or not mb.stdout.strip():
        return None
    count = subprocess.run(
        ["git", "rev-list", "--count", f"{mb.stdout.strip()}..HEAD"],
        capture_output=True,
        text=True,
    )
    if count.returncode != 0 or not count.stdout.strip():
        return None
    try:
        return int(count.stdout.strip())
    except ValueError:
        return None


def find_nearest_ancestor_release_branch(default_branch: str) -> str | None:
    """Return the releases/*|hotfixes/* branch that is the nearest ancestor of
    HEAD, or None if the default branch is a nearer ancestor, none exist, or
    the default branch cannot be resolved.

    "Nearest" = fewest commits from the merge-base to HEAD. On a tie with the
    default branch, the default branch wins (a tie means HEAD shares the
    same fork point with both, i.e. a trunk branch). Fails safe toward None
    so the check runs when in doubt.
    """
    candidates = _list_release_hotfix_refs()
    if not candidates:
        return None

    # Distance to the default branch — try remote-tracking then bare name,
    # since CI checkouts often only have the remote-tracking ref.
    default_distance = None
    for ref in (f"refs/remotes/origin/{default_branch}", default_branch):
        default_distance = _merge_base_distance(ref)
        if default_distance is not None:
            break
    if default_distance is None:
        return None

    nearest_ref = None
    nearest_distance = None
    for ref in candidates:
        d = _merge_base_distance(ref)
        if d is None:
            continue
        if nearest_distance is None or d < nearest_distance:
            nearest_ref = ref
            nearest_distance = d

    if nearest_ref is None:
        return None
    # Only a strictly-nearer release/hotfix branch wins. A tie means HEAD shares
    # the same fork point with both (e.g. a feature branch off trunk and a
    # release cut from trunk afterward) — that is a trunk branch, so run the check.
    return nearest_ref if nearest_distance < default_distance else None


def get_merge_base(remote_ref: str) -> str:
    """Return the merge-base commit SHA between HEAD and remote_ref."""
    result = subprocess.run(
        ["git", "merge-base", "HEAD", remote_ref],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            f"Error finding merge-base with {remote_ref}: {result.stderr}",
            file=sys.stderr,
        )
        sys.exit(1)
    return result.stdout.strip()


def get_base_pdl_changes(merge_base: str, remote_ref: str) -> list[str]:
    """Return PDL files that changed on remote_ref since merge_base.

    Used to detect whether any of the PDL files touched on this branch have
    also moved on the base branch — if so, the branch must be rebased before
    schemaVersion can be bumped correctly.
    """
    try:
        result = subprocess.run(
            [
                "git",
                "diff",
                "--name-only",
                "--diff-filter=ACM",
                merge_base,
                remote_ref,
                "--",
                "*.pdl",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return [f.strip() for f in result.stdout.strip().splitlines() if f.strip()]
    except subprocess.CalledProcessError as e:
        print(f"Error getting base branch PDL changes: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def get_changed_pdl_files(base_ref: str) -> list[str]:
    """Return repo-relative paths of PDL files that differ between base_ref and the working tree.

    Compares base_ref against the working tree (not HEAD) so that uncommitted
    changes are included — this function runs inside a pre-commit hook.
    base_ref may be a branch name, remote tracking ref, or commit SHA.
    """
    try:
        result = subprocess.run(
            [
                "git",
                "diff",
                "--name-only",
                "--diff-filter=ACM",
                base_ref,
                "--",
                "*.pdl",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return [f.strip() for f in result.stdout.strip().splitlines() if f.strip()]
    except subprocess.CalledProcessError as e:
        print(f"Error getting changed files: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def get_file_at_branch(filepath: str, branch: str) -> str | None:
    """Return file content at branch, or None if the file doesn't exist there."""
    result = subprocess.run(
        ["git", "show", f"{branch}:{filepath}"],
        capture_output=True,
        text=True,
    )
    return result.stdout if result.returncode == 0 else None


def is_comment_only_change(filepath: str, base_ref: str) -> bool:
    """Return True if filepath's only diff vs base_ref is comments/whitespace.

    PDL doc comments (`/** */`) and line comments (`//`) carry no schema
    semantics, so a doc-only clarification must not trigger a schemaVersion
    bump — nor cascade a bump into every aspect that references the edited
    record. Compares the working-tree file against base_ref with comments
    removed and whitespace collapsed.

    New files (absent on base_ref) and unreadable files are treated as real
    changes (returns False) so they are never silently dropped.
    """
    base_content = get_file_at_branch(filepath, base_ref)
    if base_content is None:
        return False
    try:
        current_content = Path(filepath).read_text(encoding="utf-8")
    except OSError:
        return False
    return normalize_pdl_for_compare(base_content) == normalize_pdl_for_compare(
        current_content
    )


# ---------------------------------------------------------------------------
# PDL parsing
# ---------------------------------------------------------------------------

# Default PDL source root relative to the repo root
_DEFAULT_PDL_ROOT = Path("metadata-models/src/main/pegasus")


def get_pdl_roots() -> list[Path]:
    """
    Return the list of PDL source roots to scan.

    Reads the PDL_ROOTS environment variable (colon-separated paths).
    Falls back to the single default root if the variable is not set.
    """
    raw = os.environ.get("PDL_ROOTS")
    if raw:
        return [Path(p.strip()) for p in raw.split(":") if p.strip()]
    return [_DEFAULT_PDL_ROOT]


def _relative_to_any_root(path: Path, roots: list[Path]) -> Path | None:
    """Return path relative to the first matching root, or None if it matches none."""
    for root in roots:
        try:
            return path.relative_to(root)
        except ValueError:
            continue
    return None


def find_all_pdl_files() -> list[Path]:
    return [f for root in get_pdl_roots() for f in root.rglob("*.pdl")]


def fqn_to_path(fqn: str) -> Path | None:
    """Convert a fully-qualified PDL name (com.linkedin.X.Foo) to a file path."""
    return _DEFAULT_PDL_ROOT / Path(fqn.replace(".", "/")).with_suffix(".pdl")


def parse_pdl_header(content: str) -> tuple[str, dict[str, str]]:
    """
    Return (namespace, imports) where imports maps short name → FQN.
    e.g. ("com.linkedin.dataset", {"TimeseriesAspectBase": "com.linkedin.timeseries.TimeseriesAspectBase"})
    """
    ns_match = re.search(r"^\s*namespace\s+([\w.]+)", content, re.MULTILINE)
    namespace = ns_match.group(1) if ns_match else ""

    imports: dict[str, str] = {}
    for m in re.finditer(r"^\s*import\s+([\w.]+)", content, re.MULTILINE):
        fqn = m.group(1)
        short = fqn.rsplit(".", 1)[-1]
        imports[short] = fqn

    return namespace, imports


def parse_includes(content: str) -> list[str]:
    """
    Return the short names listed in all `record X includes A, B, C` clauses.
    """
    names: list[str] = []
    for match in re.finditer(r"\brecord\s+\w+\s+includes\s+([^{]+)", content):
        names.extend(name.strip() for name in match.group(1).split(",") if name.strip())
    return names


def resolve_includes(content: str) -> list[str]:
    """
    Return FQNs for all records this PDL file includes.
    Short names are resolved via import statements, with namespace as fallback.
    """
    namespace, imports = parse_pdl_header(content)
    short_names = parse_includes(content)
    fqns: list[str] = []
    for name in short_names:
        if name in imports:
            fqns.append(imports[name])
        elif namespace:
            fqns.append(f"{namespace}.{name}")
    return fqns


# ---------------------------------------------------------------------------
# Field-type reference parsing
#
# `includes` alone is not enough: a non-aspect record referenced as a *field
# type* (e.g. `schedule: optional DataHubIngestionSourceSchedule`) creates a
# real schema dependency. When the referenced record changes, every aspect
# that uses it must be bumped, even though no `includes` relationship exists.
# ---------------------------------------------------------------------------

_STRING_LITERAL_RE = re.compile(r'"(?:[^"\\]|\\.)*"')
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"//[^\n]*")
_IMPORT_LINE_RE = re.compile(r"^\s*import\s+[\w.]+\s*$", re.MULTILINE)
_ANNOTATION_PREFIX_RE = re.compile(r"@\w+\s*=\s*\{")
# `pkg.subpkg.PascalName` — lowercase package parts joined by dots, ending in a PascalCase type
_FQN_REF_RE = re.compile(r"\b(?:[a-z][\w]*\.)+[A-Z]\w*\b")
_PASCAL_CASE_RE = re.compile(r"\b[A-Z]\w*\b")

_PDL_KEYWORDS = frozenset(
    {
        "namespace",
        "import",
        "package",
        "record",
        "enum",
        "union",
        "array",
        "map",
        "typeref",
        "fixed",
        "optional",
        "includes",
    }
)


def _strip_strings_and_comments(content: str) -> str:
    # Strings first — guards against comment markers (`/*`, `*/`, `//`) embedded
    # inside string literals being mis-recognized as real comments.
    content = _STRING_LITERAL_RE.sub('""', content)
    content = _BLOCK_COMMENT_RE.sub(" ", content)
    content = _LINE_COMMENT_RE.sub(" ", content)
    return content


def strip_pdl_comments(content: str) -> str:
    """Remove PDL block (`/* */`) and line (`//`) comments.

    Unlike `_strip_strings_and_comments`, this preserves the *contents* of
    string literals — it only drops comments. That distinction matters for
    semantic comparison: masking strings to `""` would hide a real change to a
    string value (e.g. an annotation `"name"`) and mis-classify it as
    comment-only. A single linear scan tracks string state so comment markers
    appearing inside a string literal are not treated as comments.
    """
    out: list[str] = []
    i = 0
    n = len(content)
    while i < n:
        ch = content[i]
        if ch == '"':
            # Copy the string literal verbatim, honoring backslash escapes.
            j = i + 1
            while j < n:
                if content[j] == "\\":
                    j += 2
                    continue
                if content[j] == '"':
                    j += 1
                    break
                j += 1
            out.append(content[i:j])
            i = j
        elif ch == "/" and i + 1 < n and content[i + 1] == "*":
            end = content.find("*/", i + 2)
            i = end + 2 if end != -1 else n
        elif ch == "/" and i + 1 < n and content[i + 1] == "/":
            end = content.find("\n", i + 2)
            i = end if end != -1 else n
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def normalize_pdl_for_compare(content: str) -> str:
    """Canonical form for semantic comparison: comments removed and runs of
    whitespace collapsed to single spaces. Two PDL files with identical
    canonical forms differ only in comments and/or formatting.
    """
    return " ".join(strip_pdl_comments(content).split())


def _strip_annotation_blocks(content: str) -> str:
    """
    Remove `@Foo = { ... }` annotation values.

    Annotation values are JSON-like, not PDL — identifiers inside are never type
    references. Brace-balanced so nested objects are handled correctly.
    """
    out: list[str] = []
    i = 0
    while True:
        m = _ANNOTATION_PREFIX_RE.search(content, i)
        if m is None:
            out.append(content[i:])
            return "".join(out)
        out.append(content[i : m.start()])
        depth = 0
        j = m.end() - 1  # position of the opening '{'
        while j < len(content):
            ch = content[j]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    j += 1
                    break
            j += 1
        i = j


def parse_field_types(content: str) -> list[str]:
    """
    Return tokens used as field types in this PDL file. Each token is either:
      - a fully-qualified name (`com.linkedin.common.AuditStamp`), already
        resolved; or
      - a PascalCase short name (`AuditStamp`), to be resolved later via the
        file's imports or namespace.

    The scan masks strings, comments, import lines, and `@Annotation = {...}`
    blocks first, so only real field-type references remain. It catches:

      - direct references:   `field: Foo`
      - optional fields:     `field: optional Foo`
      - generic params:      `field: array[Foo]`, `field: map[string, Foo]`
      - union members:       `field: union[Foo, Bar]`
      - nested fields inside inline `record { ... }` definitions
      - inline FQN refs:     `field: com.linkedin.common.Foo`

    Unresolved short names (no matching import, no matching namespace record)
    are dropped by `resolve_dependencies`, so any false positives are harmless.
    """
    cleaned = _strip_strings_and_comments(content)
    cleaned = _IMPORT_LINE_RE.sub(" ", cleaned)
    cleaned = _strip_annotation_blocks(cleaned)

    out: list[str] = []
    seen: set[str] = set()

    for m in _FQN_REF_RE.finditer(cleaned):
        ref = m.group(0)
        if ref not in seen:
            seen.add(ref)
            out.append(ref)

    # Mask the FQN matches so their trailing PascalCase tokens aren't counted twice
    cleaned_no_fqn = _FQN_REF_RE.sub(" ", cleaned)

    for token in _PASCAL_CASE_RE.findall(cleaned_no_fqn):
        if token in _PDL_KEYWORDS or token in seen:
            continue
        seen.add(token)
        out.append(token)

    return out


def resolve_dependencies(content: str) -> list[str]:
    """
    Return FQNs for every record / enum / typeref this PDL file depends on,
    via either `includes` clauses or field-type references.

    Short names are resolved via import statements, with namespace as fallback.
    Tokens already containing dots are treated as fully-qualified.
    Unresolvable names (no namespace, not imported) are silently dropped.
    """
    namespace, imports = parse_pdl_header(content)

    tokens: list[str] = []
    seen: set[str] = set()
    for name in parse_includes(content) + parse_field_types(content):
        if name not in seen:
            seen.add(name)
            tokens.append(name)

    fqns: list[str] = []
    for name in tokens:
        if "." in name:
            fqns.append(name)
        elif name in imports:
            fqns.append(imports[name])
        elif namespace:
            fqns.append(f"{namespace}.{name}")
    return fqns


def find_aspect_annotation_bounds(content: str) -> tuple[int, int] | None:
    """Locate the @Aspect annotation object. Returns (start, end) or None."""
    match = re.search(r"@Aspect\s*=\s*(\{)", content)
    if not match:
        return None

    start = match.start(1)
    depth = 0
    for i in range(start, len(content)):
        if content[i] == "{":
            depth += 1
        elif content[i] == "}":
            depth -= 1
            if depth == 0:
                return (start, i + 1)
    return None


def parse_annotation(annotation_str: str) -> dict:
    """Parse a PDL annotation object (JSON with optional trailing commas)."""
    cleaned = re.sub(r",\s*([}\]])", r"\1", annotation_str)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Cannot parse @Aspect annotation: {e}\nContent: {annotation_str}"
        )


def get_schema_version(content: str) -> int:
    """Extract schemaVersion from PDL content. Returns 1 if not present."""
    bounds = find_aspect_annotation_bounds(content)
    if bounds is None:
        return 1
    try:
        data = parse_annotation(content[bounds[0] : bounds[1]])
        return int(data.get("schemaVersion", 1))
    except (ValueError, KeyError):
        return 1


def is_aspect(content: str) -> bool:
    """Return True if the PDL file defines an aspect."""
    return bool(re.search(r"@Aspect\s*=\s*\{", content))


def update_schema_version(content: str, new_version: int) -> str:
    """
    Update or insert schemaVersion in the @Aspect annotation.
    Preserves the original formatting style of the annotation block.
    """
    bounds = find_aspect_annotation_bounds(content)
    if bounds is None:
        return content

    start, end = bounds
    annotation = content[start:end]

    if re.search(r'"schemaVersion"\s*:', annotation):
        new_annotation = re.sub(
            r'"schemaVersion"\s*:\s*\d+',
            f'"schemaVersion": {new_version}',
            annotation,
        )
    else:
        indent_match = re.search(r"\n(\s+)", annotation)
        indent = indent_match.group(1) if indent_match else "  "

        close_pos = annotation.rfind("}")
        before_close = annotation[:close_pos]
        before_stripped = before_close.rstrip()
        if before_stripped and not before_stripped.endswith(","):
            before_stripped += ","

        new_annotation = (
            before_stripped
            + f'\n{indent}"schemaVersion": {new_version}\n'
            + annotation[close_pos:]
        )

    return content[:start] + new_annotation + content[end:]


# ---------------------------------------------------------------------------
# Backward-compatibility detection
#
# A schemaVersion bump signals that aspect data stored at the old version may
# need migration before it can be read at the new version. A change that only
# *adds optional fields* to a record (or *adds symbols* to an enum, or adds an
# entirely new type definition) leaves every previously-serialized aspect valid
# and complete — no migration is required — so it must NOT trigger a bump.
#
# The detector is deliberately conservative and fails closed: anything it
# cannot prove to be additive-only is treated as a real, bump-worthy change. A
# false "compatible" verdict would silently skip a needed version hop (and any
# migration keyed on it), whereas a false "incompatible" verdict merely bumps a
# version that did not strictly need bumping. Only the former is dangerous, so
# every parse ambiguity resolves toward bumping.
# ---------------------------------------------------------------------------

_IDENT_RE = re.compile(r"[A-Za-z_]\w*")
_SCALAR_VALUE_RE = re.compile(r"[-+\w.]+")


def _skip_ws(content: str, i: int) -> int:
    n = len(content)
    while i < n and content[i].isspace():
        i += 1
    return i


def _skip_string_literal(content: str, i: int) -> int:
    """Given content[i] == '"', return the index just past the closing quote."""
    n = len(content)
    j = i + 1
    while j < n:
        if content[j] == "\\":
            j += 2
            continue
        if content[j] == '"':
            return j + 1
        j += 1
    return n


def _skip_balanced(content: str, i: int) -> int | None:
    """Given content[i] is an opening '{' or '[', return the index just past
    the matching close, honoring nesting and string literals. None on imbalance.
    """
    pairs = {"}": "{", "]": "["}
    stack: list[str] = []
    n = len(content)
    while i < n:
        c = content[i]
        if c == '"':
            i = _skip_string_literal(content, i)
            continue
        if c in "{[":
            stack.append(c)
            i += 1
            continue
        if c in "}]":
            if not stack or stack[-1] != pairs[c]:
                return None
            stack.pop()
            i += 1
            if not stack:
                return i
            continue
        i += 1
    return None


def _skip_value(content: str, i: int) -> int | None:
    """Skip a PDL annotation value (JSON object/array/string/scalar)."""
    i = _skip_ws(content, i)
    if i >= len(content):
        return None
    ch = content[i]
    if ch == '"':
        return _skip_string_literal(content, i)
    if ch in "{[":
        return _skip_balanced(content, i)
    m = _SCALAR_VALUE_RE.match(content, i)
    return m.end() if m else None


def _read_field_type(body: str, i: int) -> int | None:
    """Scan a field's type/default expression from i, returning the index of the
    next field/annotation boundary (or end of body). Boundaries at brace depth 0
    are a leading '@' (next field's annotation) or an identifier immediately
    followed by ':' (next field's name). Nested braces/brackets and string
    literals are skipped wholesale so their contents never look like boundaries.
    """
    n = len(body)
    while i < n:
        c = body[i]
        if c.isspace():
            i += 1
            continue
        if c == '"':
            i = _skip_string_literal(body, i)
            continue
        if c == "@" or c == ",":
            return i
        if c in "{[":
            j = _skip_balanced(body, i)
            if j is None:
                return None
            i = j
            continue
        if c in "}]":
            return None
        m = _IDENT_RE.match(body, i)
        if m:
            k = _skip_ws(body, m.end())
            if k < n and body[k] == ":":
                return i
            i = m.end()
            continue
        i += 1
    return i


def _type_is_optional(type_text: str) -> bool:
    """True if a field's type expression is declared `optional` (PDL places the
    keyword at the start of the type, e.g. `field: optional string`)."""
    return bool(re.match(r"\s*optional\b", type_text))


def _strip_field_default(type_text: str) -> str:
    """Drop a trailing PDL default (`= ...`) so type comparison matches the
    report tool, which ignores defaults when deciding whether a field changed.
    """
    return re.sub(r"\s*=\s*.*$", "", type_text).strip()


# Field-level annotations whose value drives Elasticsearch/graph index mapping
# (search index fields, graph edges, timeseries field indexing). Changing or
# adding one of these on an existing field requires a reindex, so — unlike
# other field annotations (@deprecated, @compliance, @UrnValidation, ...),
# which carry no reindex-relevant semantics and stay ignored — these must be
# treated as a real, bump-worthy schema change.
BUMP_WORTHY_ANNOTATIONS = frozenset(
    {
        "Searchable",
        "Relationship",
        "SearchableRef",
        "TimeseriesField",
        "TimeseriesFieldCollection",
    }
)


def parse_record_fields(body: str) -> dict[str, tuple[str, bool, dict[str, str]]] | None:
    """Parse the top-level fields of a record body (comments already stripped).

    Returns {field name: (normalized type, is_optional, annotations)}, or None
    if the body cannot be parsed unambiguously. `annotations` maps annotation
    name -> normalized value text, but only for names in
    `BUMP_WORTHY_ANNOTATIONS` — every other annotation (and any default value)
    is parsed (so the scanner stays positioned correctly) but discarded, since
    only the whitelisted annotations and the field's type/optional-ness matter
    for bump decisions.

    Public because `report_aspect_changes.fields()` delegates to this same
    parser (rather than duplicating the brace-balanced scanning logic) so both
    tools classify annotation changes identically.
    """
    fields: dict[str, tuple[str, bool, dict[str, str]]] = {}
    pending_annotations: dict[str, str] = {}
    n = len(body)
    i = _skip_ws(body, 0)
    while i < n:
        if body[i] == ",":
            i = _skip_ws(body, i + 1)
            continue
        if body[i] == "@":
            i += 1
            m = _IDENT_RE.match(body, i)
            if not m:
                return None
            ann_name = m.group(0)
            i = m.end()
            while i < n and body[i] == ".":
                m = _IDENT_RE.match(body, i + 1)
                if not m:
                    return None
                i = m.end()
            j = _skip_ws(body, i)
            if j < n and body[j] == "=":
                nxt = _skip_value(body, j + 1)
                if nxt is None:
                    return None
                if ann_name in BUMP_WORTHY_ANNOTATIONS:
                    pending_annotations[ann_name] = normalize_pdl_for_compare(
                        body[j + 1 : nxt]
                    )
                i = nxt
            i = _skip_ws(body, i)
            continue
        m = _IDENT_RE.match(body, i)
        if not m:
            return None
        name = m.group(0)
        k = _skip_ws(body, m.end())
        if k >= n or body[k] != ":":
            return None
        type_start = _skip_ws(body, k + 1)
        end = _read_field_type(body, type_start)
        if end is None:
            return None
        type_text = body[type_start:end]
        type_norm = normalize_pdl_for_compare(_strip_field_default(type_text))
        if name in fields:
            return None
        fields[name] = (type_norm, _type_is_optional(type_text), pending_annotations)
        pending_annotations = {}
        i = _skip_ws(body, end)
    return fields


def _parse_enum_symbols(body: str) -> set[str] | None:
    """Return the set of symbol names in an enum body (comments stripped),
    ignoring per-symbol annotations. None if it cannot be parsed."""
    symbols: set[str] = set()
    n = len(body)
    i = 0
    while i < n:
        ch = body[i]
        if ch.isspace() or ch == ",":
            i += 1
            continue
        if ch == '"':
            i = _skip_string_literal(body, i)
            continue
        if ch == "@":
            i += 1
            m = _IDENT_RE.match(body, i)
            if not m:
                return None
            i = m.end()
            j = _skip_ws(body, i)
            if j < n and body[j] == "=":
                nxt = _skip_value(body, j + 1)
                if nxt is None:
                    return None
                i = nxt
            continue
        if ch in "{[":
            j = _skip_balanced(body, i)
            if j is None:
                return None
            i = j
            continue
        m = _IDENT_RE.match(body, i)
        if m:
            symbols.add(m.group(0))
            i = m.end()
            continue
        i += 1
    return symbols


def _parse_includes_from_header(header: str) -> set[str]:
    m = re.search(r"\bincludes\b(.*)", header, re.DOTALL)
    if not m:
        return set()
    return {x.strip() for x in m.group(1).split(",") if x.strip()}


def parse_top_level_defs(content: str) -> dict[str, dict] | None:
    """Parse the top-level record/enum definitions in a PDL file.

    Returns {name: {"kind": "record", "includes": set, "fields": {...}}} or
    {name: {"kind": "enum", "symbols": set}}. Returns None when the file uses a
    construct this conservative parser does not model (typeref/fixed) or cannot
    parse — callers then treat the change as bump-worthy.
    """
    c = strip_pdl_comments(content)
    n = len(c)
    defs: dict[str, dict] = {}
    i = 0
    while i < n:
        ch = c[i]
        if ch == '"':
            i = _skip_string_literal(c, i)
            continue
        if ch in "{[":
            j = _skip_balanced(c, i)
            if j is None:
                return None
            i = j
            continue
        m = _IDENT_RE.match(c, i)
        if not m:
            i += 1
            continue
        word = m.group(0)
        if word in ("typeref", "fixed"):
            return None
        if word not in ("record", "enum"):
            i = m.end()
            continue
        nm = _IDENT_RE.match(c, _skip_ws(c, m.end()))
        if not nm:
            return None
        name = nm.group(0)
        p = nm.end()
        while p < n and c[p] != "{":
            if c[p] == '"':
                p = _skip_string_literal(c, p)
                continue
            p += 1
        if p >= n:
            return None
        header = c[nm.end() : p]
        body_end = _skip_balanced(c, p)
        if body_end is None:
            return None
        body = c[p + 1 : body_end - 1]
        if word == "record":
            fields = parse_record_fields(body)
            if fields is None:
                return None
            defs[name] = {
                "kind": "record",
                "includes": _parse_includes_from_header(header),
                "fields": fields,
            }
        else:
            symbols = _parse_enum_symbols(body)
            if symbols is None:
                return None
            defs[name] = {"kind": "enum", "symbols": symbols}
        i = body_end
    return defs


def _aspect_annotation_without_version(content: str) -> dict | None:
    """Return the parsed @Aspect annotation with schemaVersion removed, or None
    when the file has no @Aspect annotation (e.g. a shared non-aspect record)."""
    bounds = find_aspect_annotation_bounds(content)
    if bounds is None:
        return None
    try:
        data = parse_annotation(content[bounds[0] : bounds[1]])
    except ValueError:
        return {"__unparseable__": content[bounds[0] : bounds[1]]}
    data.pop("schemaVersion", None)
    return data


def _defs_backward_compatible(
    base_defs: dict[str, dict], cur_defs: dict[str, dict]
) -> bool:
    """True iff cur_defs differs from base_defs only by additive, migration-free
    changes — matching report_aspect_changes' Additive bucket:

      - added optional fields
      - added enum symbols
      - entirely new type definitions
      - default-only edits on existing fields
      - non-whitelisted annotation edits on existing fields (e.g. @deprecated,
        @compliance, @UrnValidation)

    Any removal, type change, optional→required flip, added non-optional field,
    includes change, removed enum symbol, or a changed/added/removed
    `BUMP_WORTHY_ANNOTATIONS` value on an existing field (@Searchable,
    @Relationship, @SearchableRef, @TimeseriesField, @TimeseriesFieldCollection
    — these drive reindexing) is treated as incompatible (bump).
    """
    for name, bdef in base_defs.items():
        cdef = cur_defs.get(name)
        if cdef is None or cdef["kind"] != bdef["kind"]:
            return False
        if bdef["kind"] == "record":
            if bdef["includes"] != cdef["includes"]:
                return False
            base_fields = bdef["fields"]
            cur_fields = cdef["fields"]
            for fname, (btype, bopt, bann) in base_fields.items():
                cur_field = cur_fields.get(fname)
                if cur_field is None:
                    return False
                ctype, copt, cann = cur_field
                # Type change or optional→required is breaking (matches report).
                if ctype != btype:
                    return False
                if bopt and not copt:
                    return False
                # required→optional is noisy in the report (still sets
                # has_structural there); treat as bump-worthy to stay aligned.
                if (not bopt) and copt:
                    return False
                # A whitelisted annotation (index/reindex-relevant) changing
                # value, appearing, or disappearing is a real schema change.
                if bann != cann:
                    return False
            for fname, (_ctype, copt, _ann) in cur_fields.items():
                if fname not in base_fields and not copt:
                    return False
        elif not bdef["symbols"].issubset(cdef["symbols"]):
            return False
    return True


def is_backward_compatible_change(filepath: str, base_ref: str) -> bool:
    """Return True if filepath's only diff vs base_ref is backward-compatible
    (additive optional fields / enum symbols / new type defs), meaning no
    schemaVersion bump is warranted. Fails closed (returns False) on new files,
    unreadable files, @Aspect changes beyond schemaVersion, or any parse
    ambiguity.
    """
    base_content = get_file_at_branch(filepath, base_ref)
    if base_content is None:
        return False
    try:
        current_content = Path(filepath).read_text(encoding="utf-8")
    except OSError:
        return False
    if _aspect_annotation_without_version(
        base_content
    ) != _aspect_annotation_without_version(current_content):
        return False
    base_defs = parse_top_level_defs(base_content)
    cur_defs = parse_top_level_defs(current_content)
    if base_defs is None or cur_defs is None:
        return False
    return _defs_backward_compatible(base_defs, cur_defs)


# ---------------------------------------------------------------------------
# Include graph
# ---------------------------------------------------------------------------


def build_reverse_include_graph(
    all_pdl_files: list[Path],
) -> dict[str, set[str]]:
    """
    Build a reverse map: fqn → set of file paths that depend on it.

    A "depends on" edge is recorded both for `record X includes Y` clauses and
    for field-type references like `field: Y` or `field: optional Y`.

    Given that A.pdl includes B and references C as a field type, the graph
    will have:
      fqn(B) → { path(A.pdl) }
      fqn(C) → { path(A.pdl) }
    """
    reverse: dict[str, set[str]] = defaultdict(set)
    roots = get_pdl_roots()

    for pdl_path in all_pdl_files:
        try:
            content = pdl_path.read_text(encoding="utf-8")
        except OSError:
            continue

        rel = _relative_to_any_root(pdl_path, roots)
        if rel is None:
            continue

        # The PascalCase field-type scan will pick up the record's own name
        # (`record Foo { ... }` → `Foo`). Drop that self-reference here.
        own_fqn = ".".join(rel.with_suffix("").parts)

        for dep_fqn in resolve_dependencies(content):
            if dep_fqn == own_fqn:
                continue
            reverse[dep_fqn].add(str(pdl_path))

    return reverse


def find_transitively_affected_aspects(
    directly_changed: list[str],
    reverse_graph: dict[str, set[str]],
    all_pdl_files: list[Path],
) -> set[str]:
    """
    BFS from the set of directly changed files through the reverse dependency
    graph. Returns all *aspect* files (including the originals) that need a
    version bump.

    'directly changed' files don't need to be aspects themselves — a changed
    non-aspect record (whether referenced via `includes` or as a field type)
    can trigger bumps in aspects that depend on it.
    """
    # Build fqn → path index for fast lookup
    roots = get_pdl_roots()
    fqn_to_file: dict[str, str] = {}
    for pdl_path in all_pdl_files:
        rel = _relative_to_any_root(pdl_path, roots)
        if rel is None:
            continue
        fqn = ".".join(rel.with_suffix("").parts)
        fqn_to_file[fqn] = str(pdl_path)

    # Also index by repo-relative path → fqn
    file_to_fqn: dict[str, str] = {v: k for k, v in fqn_to_file.items()}

    queue: deque[str] = deque(directly_changed)
    visited: set[str] = set(directly_changed)
    affected_aspects: set[str] = set()

    while queue:
        filepath = queue.popleft()
        path = Path(filepath)
        if not path.exists():
            continue

        content = path.read_text(encoding="utf-8")
        if is_aspect(content):
            affected_aspects.add(filepath)

        # Walk up the reverse graph: find everything that includes this file
        fqn = file_to_fqn.get(filepath)
        if fqn is None:
            continue

        for parent_path in reverse_graph.get(fqn, set()):
            if parent_path not in visited:
                visited.add(parent_path)
                queue.append(parent_path)

    return affected_aspects


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bump schemaVersion on changed PDL aspect files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--base-branch",
        default=None,
        help="Branch to compare against. Defaults to the auto-detected default "
        "branch. The check is skipped entirely when the base is a releases/* or "
        "hotfixes/* branch.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing files",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="CI enforcement mode: write nothing and exit non-zero if any "
        "changed aspect still needs a schemaVersion bump",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed per-file output",
    )
    args = parser.parse_args()

    # Skip on release/hotfix lines: the version-bump check compares against
    # trunk and would demand pulling unrelated trunk schema churn into a
    # release/hotfix. When an explicit base is given (CI passes the PR target),
    # classify it directly; otherwise (local pre-commit hook) infer the nearest
    # ancestor branch from git topology.
    if args.base_branch is not None:
        if is_release_or_hotfix_branch(args.base_branch):
            print(
                f"Base branch '{args.base_branch}' is a release/hotfix branch; "
                "skipping schema version check."
            )
            return 0
        base_branch = args.base_branch
    else:
        default_branch = detect_default_branch()
        nearest = find_nearest_ancestor_release_branch(default_branch)
        if nearest:
            print(
                f"Nearest base branch '{nearest}' is a release/hotfix branch; "
                "skipping schema version check."
            )
            return 0
        base_branch = default_branch

    remote_ref = f"refs/remotes/origin/{base_branch}"
    merge_base = get_merge_base(remote_ref)

    directly_changed = get_changed_pdl_files(merge_base)

    # Drop files whose only diff vs the merge-base is comments/whitespace. A
    # doc-comment clarification carries no schema semantics, so it must neither
    # bump the edited file nor cascade a bump into aspects that reference it.
    comment_only = [
        f for f in directly_changed if is_comment_only_change(f, merge_base)
    ]
    if comment_only:
        directly_changed = [f for f in directly_changed if f not in set(comment_only)]
        if args.verbose:
            print(f"Ignoring {len(comment_only)} comment-only PDL change(s):")
            for f in comment_only:
                print(f"  {f}")
            print()

    # Drop files whose only diff vs the merge-base is backward-compatible —
    # additive optional fields, added enum symbols, or new type definitions.
    # Such changes need no migration, so they must neither bump the edited file
    # nor cascade a bump into aspects that reference it.
    backward_compatible = [
        f for f in directly_changed if is_backward_compatible_change(f, merge_base)
    ]
    if backward_compatible:
        directly_changed = [
            f for f in directly_changed if f not in set(backward_compatible)
        ]
        if args.verbose:
            print(
                f"Ignoring {len(backward_compatible)} backward-compatible PDL "
                "change(s) (additive optional fields / enum symbols only):"
            )
            for f in backward_compatible:
                print(f"  {f}")
            print()

    if not directly_changed:
        print("No changed PDL files found.")
        return 0

    # Check whether any PDL files changed on this branch also changed on the
    # base branch since divergence. If so, the branch must be rebased or
    # merged before schemaVersion can be bumped correctly.
    # Unrelated PDL changes on the base branch do not block.
    base_pdl_changes = get_base_pdl_changes(merge_base, remote_ref)
    conflicting = sorted(set(directly_changed) & set(base_pdl_changes))
    if conflicting:
        files_list = "\n".join(f"  {f}" for f in conflicting)
        print(
            f"ERROR: The following PDL file(s) also changed on {base_branch} "
            f"since this branch diverged. Please merge or rebase from "
            f"{base_branch} first:\n{files_list}",
            file=sys.stderr,
        )
        return 1

    if args.verbose:
        print(f"Comparing against branch: {base_branch} (merge-base: {merge_base[:12]})")
        print(f"Found {len(directly_changed)} directly changed PDL file(s):")
        for f in directly_changed:
            print(f"  {f}")
        print()

    # Build the include graph once across all PDL files
    all_pdl_files = find_all_pdl_files()
    reverse_graph = build_reverse_include_graph(all_pdl_files)

    # Expand to all transitively affected aspects
    to_bump = find_transitively_affected_aspects(
        directly_changed, reverse_graph, all_pdl_files
    )

    if not to_bump:
        print("No aspect files affected.")
        return 0

    # Separate directly changed aspects from implicitly affected ones for logging
    directly_changed_set = set(directly_changed)

    bumped: list[str] = []
    skipped: list[str] = []
    errors: list[str] = []

    for filepath in sorted(to_bump):
        path = Path(filepath)
        current_content = path.read_text(encoding="utf-8")

        # Use remote_ref (refs/remotes/origin/…) rather than the bare branch
        # name — local checkouts of the base branch may not exist.
        base_content = get_file_at_branch(filepath, remote_ref)
        base_version = get_schema_version(base_content) if base_content else 0
        current_version = get_schema_version(current_content)
        new_version = base_version + 1

        if current_version >= new_version:
            if args.verbose:
                reason = (
                    "direct change"
                    if filepath in directly_changed_set
                    else "implicit (depends on changed record)"
                )
                print(
                    f"SKIP  {filepath}  "
                    f"(already at v{current_version} >= v{new_version})  [{reason}]"
                )
            skipped.append(filepath)
            continue

        reason = (
            "direct change"
            if filepath in directly_changed_set
            else "implicit (depends on changed record)"
        )

        try:
            new_content = update_schema_version(current_content, new_version)
        except Exception as e:
            print(f"ERROR {filepath}: {e}", file=sys.stderr)
            errors.append(filepath)
            continue

        if args.check:
            print(
                f"NEEDS BUMP  {filepath}  v{current_version} → v{new_version}"
                f"  [{reason}]"
            )
            bumped.append(filepath)
        elif args.dry_run:
            print(
                f"BUMP  {filepath}  v{base_version} → v{new_version}"
                f"  [{reason}]  [dry-run]"
            )
            bumped.append(filepath)
        else:
            path.write_text(new_content, encoding="utf-8")
            print(f"BUMP  {filepath}  v{base_version} → v{new_version}  [{reason}]")
            bumped.append(filepath)

    verb = "need bump" if args.check else "bumped"
    print(
        f"\nSummary: {len(bumped)} {verb}, {len(skipped)} skipped, {len(errors)} errors"
    )

    if args.check and bumped:
        print(
            "\nERROR: The following aspect(s) changed but their schemaVersion was "
            "not bumped:\n"
            + "\n".join(f"  {f}" for f in bumped)
            + "\n\nRun the bump hook locally and commit the result:\n"
            "  pre-commit run bump-schema-versions --all-files\n"
            "or run the script directly:\n"
            "  python .github/scripts/bump_schema_versions.py",
            file=sys.stderr,
        )
        return 1

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
