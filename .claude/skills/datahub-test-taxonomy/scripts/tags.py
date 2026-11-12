"""Deterministic helpers for the datahub-test-taxonomy skill (no LLM).

Run from anywhere inside the repo:
    uv run --with pyyaml==6.0.3 python .claude/skills/datahub-test-taxonomy/scripts/tags.py <command> [--framework F ...]

Commands:
    changed [--base REF]  test files this branch added/modified/deleted (uncommitted work included)
    list                  discovered test files, grouped by directory
    untagged              discovered test files no tag rule matches
    prune [--write]       drop entries that match no file (deleted paths, dead globs)
    validate              tags exist in taxonomy.yaml under their dimension; every path matches a file
    enums [--write]       render taxonomy.py's enum block from taxonomy.yaml (and
                          taxonomy_overlay.py from taxonomy.overlay.yaml)

Overlays: taxonomy.overlay.yaml and each framework's test_tags.overlay.yaml hold tags
and tests for features only this repository has. Every command reads base + overlay;
validate also checks that the base files only use base taxonomy tags, so the base
files stay valid in a repository without the overlay.
"""

import argparse
import fnmatch
import json
import re
import subprocess
import sys
import textwrap
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import yaml

REPO = Path(
    subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
)
TAXONOMY_YAML = REPO / ".github/smart-testing/taxonomy.yaml"
TAXONOMY_PY = REPO / "smoke-test/smart_testing/taxonomy.py"
TAXONOMY_OVERLAY_YAML = TAXONOMY_YAML.with_name("taxonomy.overlay.yaml")
TAXONOMY_OVERLAY_PY = TAXONOMY_PY.with_name("taxonomy_overlay.py")
ENUM_BEGIN = "# BEGIN GENERATED ENUMS"
ENUM_END = "# END GENERATED ENUMS"
ENUM_CLASS_NAMES = {
    "feature-user": "FeatureUser",
    "feature-dev": "FeatureDev",
    "compound": "Compound",
    "technical-layer": "TechnicalLayer",
    "entity-scope": "EntityScope",
    "change-type": "ChangeType",
}


@dataclass(frozen=True)
class Framework:
    name: str
    tag_file: Path
    # Tag paths are relative to this directory.
    root: Path
    # Directory (relative to root) that holds the tests.
    test_dir: str
    # Basename patterns of test files.
    test_names: Tuple[str, ...]

    @property
    def overlay_file(self) -> Path:
        return self.tag_file.with_name("test_tags.overlay.yaml")

    def tag_files(self) -> List[Path]:
        return [p for p in (self.tag_file, self.overlay_file) if p.exists()]

    def rel(self, repo_path: str) -> Optional[str]:
        """Repo-relative path → tag-file path, or None when outside this framework."""
        path = REPO / repo_path
        try:
            rel = path.relative_to(self.root).as_posix()
        except ValueError:
            return None
        return rel if rel.startswith(self.test_dir + "/") else None

    def is_test(self, rel_path: str) -> bool:
        name = rel_path.rsplit("/", 1)[-1]
        return any(fnmatch.fnmatch(name, pat) for pat in self.test_names)

    def discover(self) -> List[str]:
        base = self.root / self.test_dir
        return sorted(
            p.relative_to(self.root).as_posix()
            for p in base.rglob("*")
            if p.is_file() and self.is_test(p.name) and "__pycache__" not in p.parts
        )

    def all_files(self) -> List[str]:
        """Every file under the test dir; globs may legitimately cover helpers."""
        base = self.root / self.test_dir
        return [
            p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file()
        ]


FRAMEWORKS: Dict[str, Framework] = {
    "pytest": Framework(
        name="pytest",
        tag_file=REPO / "smoke-test/smart_testing/test_tags.yaml",
        root=REPO / "smoke-test",
        test_dir="tests/e2e",
        test_names=("test_*.py", "*_test.py"),
    ),
    "playwright": Framework(
        name="playwright",
        tag_file=REPO / ".github/smart-testing/playwright/test_tags.yaml",
        root=REPO,
        test_dir="e2e-test/ui/playwright/tests",
        test_names=("*.spec.ts",),
    ),
    "ingestion": Framework(
        name="ingestion",
        tag_file=REPO / ".github/smart-testing/ingestion/test_tags.yaml",
        root=REPO,
        test_dir="metadata-ingestion/tests",
        test_names=("test_*.py", "*_test.py"),
    ),
}


def git(*args: str, check: bool = True) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, capture_output=True, text=True, check=check
    ).stdout


def load_rules(fw: Framework, overlay: bool = True) -> List[dict]:
    rules: List[dict] = []
    for path in fw.tag_files() if overlay else [fw.tag_file]:
        data = yaml.safe_load(path.read_text()) or {}
        rules.extend(data.get("tests") or [])
    return rules


def known_tags(overlay: bool = True) -> Dict[str, Set[str]]:
    known: Dict[str, Set[str]] = defaultdict(set)
    paths = [TAXONOMY_YAML]
    if overlay and TAXONOMY_OVERLAY_YAML.exists():
        paths.append(TAXONOMY_OVERLAY_YAML)
    for path in paths:
        for dim, body in yaml.safe_load(path.read_text())["dimensions"].items():
            known[dim].update(e["tag"] for e in body["tags"])
    return dict(known)


def rule_for(path: str, rules: Sequence[dict]) -> Optional[dict]:
    """Mirror of smart_testing.taxonomy.TagMap.tags_for: exact wins, then longest glob."""
    best: Optional[dict] = None
    for rule in rules:
        pattern = rule["path"]
        if pattern == path:
            return rule
        if fnmatch.fnmatch(path, pattern) and (
            best is None or len(pattern) > len(best["path"])
        ):
            best = rule
    return best


def is_glob(pattern: str) -> bool:
    return any(c in pattern for c in "*?[")


# ── commands ────────────────────────────────────────────────────────────────


def default_base() -> str:
    """The repository's default branch (origin/HEAD), else master."""
    ref = git("symbolic-ref", "--short", "refs/remotes/origin/HEAD", check=False).strip()
    return ref.removeprefix("origin/") or "master"


def resolve_base(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    try:
        out = subprocess.run(
            ["gh", "pr", "view", "--json", "baseRefName", "--jq", ".baseRefName"],
            cwd=REPO,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        pass
    return default_base()


def cmd_changed(frameworks: List[Framework], base: Optional[str]) -> dict:
    branch = resolve_base(base)
    remote_ref = branch if branch.startswith("origin/") else f"origin/{branch}"
    git("fetch", "--quiet", "origin", remote_ref.removeprefix("origin/"), check=False)
    merge_base = git("merge-base", remote_ref, "HEAD").strip()

    # Merge base vs the working tree: committed and uncommitted edits, but none of
    # the commits that landed on the base after this branch forked.
    changes: List[Tuple[str, str]] = []
    for line in git("diff", "--name-status", "--no-renames", merge_base).splitlines():
        status, path = line.split("\t", 1)
        changes.append((status[0], path))
    for path in git("ls-files", "--others", "--exclude-standard").splitlines():
        changes.append(("A", path))

    result: dict = {"base": remote_ref, "merge_base": merge_base}
    for fw in frameworks:
        buckets: Dict[str, List[str]] = {"added": [], "modified": [], "deleted": []}
        for status, repo_path in changes:
            rel = fw.rel(repo_path)
            if rel is None or not fw.is_test(rel):
                continue
            key = {"A": "added", "D": "deleted"}.get(status, "modified")
            buckets[key].append(rel)
        result[fw.name] = {k: sorted(set(v)) for k, v in buckets.items()}
    return result


def cmd_list(frameworks: List[Framework]) -> dict:
    out: dict = {}
    for fw in frameworks:
        groups: Dict[str, List[str]] = defaultdict(list)
        for path in fw.discover():
            groups[path.rsplit("/", 1)[0]].append(path)
        out[fw.name] = dict(sorted(groups.items()))
    return out


def cmd_untagged(frameworks: List[Framework]) -> dict:
    return {
        fw.name: [p for p in fw.discover() if rule_for(p, load_rules(fw)) is None]
        for fw in frameworks
    }


def _drop_entries(text: str, paths: Set[str]) -> str:
    """Remove `- path: "<p>"` entry blocks (up to the next entry/comment/section)."""
    lines = text.splitlines(keepends=True)
    out: List[str] = []
    skipping = False
    for line in lines:
        m = re.match(r'\s*- path:\s*["\']?([^"\']+)["\']?\s*$', line)
        if m:
            skipping = m.group(1) in paths
            if skipping:
                continue
        elif skipping and (line.strip() == "" or line.lstrip().startswith("#")):
            skipping = False
        if not skipping:
            out.append(line)
    return re.sub(r"\n{3,}", "\n\n", "".join(out))


def cmd_prune(frameworks: List[Framework], write: bool) -> dict:
    """Entries that can no longer match anything: exact paths that are gone, and
    globs that match no file. Both are dead weight, so --write removes both."""
    out: dict = {}
    for fw in frameworks:
        files = fw.all_files()
        dead = [
            rule["path"]
            for rule in load_rules(fw)
            if (
                not any(fnmatch.fnmatch(f, rule["path"]) for f in files)
                if is_glob(rule["path"])
                else not (fw.root / rule["path"]).is_file()
            )
        ]
        if write and dead:
            for path in fw.tag_files():
                path.write_text(_drop_entries(path.read_text(), set(dead)))
        out[fw.name] = {"removed" if write else "would_remove": dead}
    return out


def cmd_validate(frameworks: List[Framework]) -> Tuple[dict, bool]:
    known = known_tags()
    base_known = known_tags(overlay=False)
    report: dict = {}
    ok = True
    for fw in frameworks:
        files = fw.all_files()
        problems: List[str] = []
        for rule in load_rules(fw, overlay=False):
            for dim, values in (rule.get("tags") or {}).items():
                for value in values or []:
                    if value in known.get(dim, set()) - base_known.get(dim, set()):
                        problems.append(
                            f"{rule['path']}: overlay tag {value!r} in the base tag file; "
                            "move the entry to the overlay"
                        )
        for rule in load_rules(fw):
            pattern = rule["path"]
            for dim, values in (rule.get("tags") or {}).items():
                if dim not in known:
                    problems.append(f"{pattern}: unknown dimension {dim!r}")
                    continue
                for value in values or []:
                    if value not in known[dim]:
                        problems.append(f"{pattern}: {dim} tag {value!r} not in taxonomy")
            if not any(fnmatch.fnmatch(f, pattern) for f in files):
                problems.append(f"{pattern}: matches no file")
        report[fw.name] = problems or "ok"
        ok = ok and not problems
    return report, ok


def render_enums() -> str:
    dims = yaml.safe_load(TAXONOMY_YAML.read_text())["dimensions"]
    blocks: List[str] = []
    for dim, cls in ENUM_CLASS_NAMES.items():
        body = dims[dim]
        doc = textwrap.fill(
            body["description"],
            width=88,
            initial_indent='    """',
            subsequent_indent="    ",
        )
        lines = [f"class {cls}(str, Enum):", doc + '"""', ""]
        for entry in body["tags"]:
            member = re.sub(r"[-:]", "_", entry["tag"]).upper()
            lines.append(f'    {member} = "{entry["tag"]}"')
        blocks.append("\n".join(lines))
    return "\n\n\n".join(blocks)


def render_overlay_module() -> str:
    dims = yaml.safe_load(TAXONOMY_OVERLAY_YAML.read_text())["dimensions"]
    lines = [
        '"""Generated from .github/smart-testing/taxonomy.overlay.yaml by the',
        "datahub-test-taxonomy skill (`tags.py enums --write`). Edit the yaml, not this file.",
        "",
        "Tags for features only this repository has; smart_testing.taxonomy merges them",
        'into KNOWN_TAGS."""',
        "",
        "from typing import Dict, FrozenSet",
        "",
        "OVERLAY_TAGS: Dict[str, FrozenSet[str]] = {",
    ]
    for dim, body in dims.items():
        lines.append(f'    "{dim}": frozenset(')
        lines.append("        {")
        lines.extend(f'            "{entry["tag"]}",' for entry in body["tags"])
        lines.append("        }")
        lines.append("    ),")
    lines.append("}")
    return "\n".join(lines) + "\n"


def cmd_enums(write: bool) -> str:
    text = TAXONOMY_PY.read_text()
    begin = text.index(ENUM_BEGIN) + len(ENUM_BEGIN)
    end = text.index(ENUM_END)
    current = text[begin:end].strip("\n")
    rendered = render_enums()
    overlay = render_overlay_module() if TAXONOMY_OVERLAY_YAML.exists() else None
    overlay_current = (
        TAXONOMY_OVERLAY_PY.read_text() if TAXONOMY_OVERLAY_PY.exists() else None
    )
    if current == rendered and overlay == overlay_current:
        return "unchanged"
    if not write:
        return "\n\n".join(r for r in (rendered, overlay) if r)
    TAXONOMY_PY.write_text(text[:begin] + "\n" + rendered + "\n\n\n" + text[end:])
    if overlay is None:
        TAXONOMY_OVERLAY_PY.unlink(missing_ok=True)
    else:
        TAXONOMY_OVERLAY_PY.write_text(overlay)
    return "written"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "command",
        choices=["changed", "list", "untagged", "prune", "validate", "enums"],
    )
    parser.add_argument(
        "--framework",
        action="append",
        choices=[*FRAMEWORKS, "all"],
        help="repeatable; default all",
    )
    parser.add_argument("--base", help="target branch (changed); default PR base or the repo's default branch")
    parser.add_argument("--write", action="store_true", help="apply changes (prune, enums)")
    args = parser.parse_args()

    names = args.framework or ["all"]
    # "all" means every framework this repository keeps a tag file for.
    frameworks = (
        [fw for fw in FRAMEWORKS.values() if fw.tag_file.exists()]
        if "all" in names
        else [FRAMEWORKS[n] for n in dict.fromkeys(names)]
    )

    ok = True
    if args.command == "changed":
        result: object = cmd_changed(frameworks, args.base)
    elif args.command == "list":
        result = cmd_list(frameworks)
    elif args.command == "untagged":
        result = cmd_untagged(frameworks)
    elif args.command == "prune":
        result = cmd_prune(frameworks, args.write)
    elif args.command == "validate":
        result, ok = cmd_validate(frameworks)
    else:
        result = cmd_enums(args.write)

    print(result if isinstance(result, str) else json.dumps(result, indent=2))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
