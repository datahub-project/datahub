#!/usr/bin/env python3
"""Fail when a test class added or modified in a PR did not execute in CI.

A test that is never run proves nothing, and the ways to silently not run one are many:
a TestNG suite XML whose <packages>/<classes> don't match the class, a Gradle
include/exclude filter, a metadata-io suite split (testOther / testOpenSearch / testEs8 ...)
that no suite picks up, a JUnit tag or TestNG group, or CI path filters skipping a module.
All of them look the same in the JUnit XML that CI uploads: the class has no executed test
case. This script checks exactly that, so nobody has to eyeball suite configuration.

Inputs:
  * the changed files of the PR (``--base``/``--head`` git refs, or an explicit list from
    ``--changed-files-from``, e.g. the GitHub "list PR files" API);
  * one or more directories holding the JUnit XML CI produced for the run (the downloaded
    ``build-and-test-*`` / ``Test Results (metadata-io *)`` / ``metadata-ingestion-test-results-*``
    artifacts). Every ``*.xml`` below them is read; non-JUnit XML is ignored.

A Java/Kotlin/Groovy test class is a top-level class in a test source dir (``src/test``,
``src/integTest``, ``src/integrationTest``, ``datahub-frontend/test``, and the custom
``src/sparkSmoke4`` / ``src/productUpdateCiGate`` sets) whose body carries a test annotation
(``@Test``, ``@ParameterizedTest``, ``@RepeatedTest``, ``@TestFactory``, ``@TestTemplate``),
that is annotated ``@Test`` at class level (TestNG), or that is concrete and extends a test
class (all its tests inherited, e.g. a per-backend ``FooOpenSearchTest``). It counts as run
when at least one non-skipped ``<testcase>`` has ``classname`` equal to its FQCN or to a
nested class of it (``Outer$Inner``, JUnit 5 ``@Nested``). An abstract class or interface is
never instantiated by the runner; it counts as run when any concrete subclass (found by
``extends``/``implements`` across all test sources, transitively) ran.

A Python test module (``tests/**/test_*.py`` or ``*_test.py`` with a ``def test*``) counts as
run when a non-skipped ``<testcase>`` has ``classname`` equal to its dotted module path from
the ``tests`` dir (``tests.unit.test_foo``) or to a class inside it.

Opt-outs live in ``.github/new-tests-ran-optout.json``: ``{"<FQCN | python module | repo
path, fnmatch patterns allowed>": "<reason>"}``. A reason is required; keys starting with
``_`` are comments.

Exit codes: 0 every checked class ran, 1 at least one did not, 2 usage / input error.

Typical local run against a real CI run::

    gh run download <run-id> -R datahub-project/datahub -D /tmp/run -p 'build-and-test-*'
    gh pr diff <pr> -R datahub-project/datahub --name-only > /tmp/files.txt
    python3 .github/scripts/check_new_tests_ran.py \\
        --changed-files-from /tmp/files.txt --results-dir /tmp/run --language java
"""

import argparse
import fnmatch
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

DEFAULT_OPTOUT_FILE = ".github/new-tests-ran-optout.json"

JVM_EXTENSIONS = (".java", ".kt", ".groovy")
# Path fragments that mark a JVM test source set. datahub-frontend is a Play app whose tests
# live in <module>/test rather than <module>/src/test.
JVM_TEST_DIR_MARKERS = (
    "/src/test/",
    "/src/integTest/",
    "/src/integrationTest/",
    # Custom Test-task source sets: acryl-spark-lineage's Spark 4 smoke tests and
    # metadata-service/configuration's product-update CI gate.
    "/src/sparkSmoke4/",
    "/src/productUpdateCiGate/",
)
JVM_TEST_DIR_PREFIXES = ("datahub-frontend/test/",)

# `@Test` and friends, optionally fully qualified. `\b` after the name keeps
# @TestConfiguration / @TestPropertySource / @Testcontainers out; requiring the name right
# after `@` or a `pkg.` qualifier keeps @SpringBootTest / @DataJpaTest out.
TEST_ANNOTATION_RE = re.compile(
    r"@(?:[A-Za-z_][\w]*\.)*(?:Test|ParameterizedTest|RepeatedTest|TestFactory|TestTemplate)\b"
)
TYPE_DECL_RE = re.compile(
    r"(?<![\w$.])((?:(?:public|protected|private|internal|abstract|open|final|static|sealed"
    r"|non-sealed|strictfp|data|inner)\s+)*)"
    r"(class|interface|enum|record|object)\s+([^\W\d][\w$]*)"
)
JAVA_PACKAGE_RE = re.compile(r"^\s*package\s+([\w.]+)\s*;?", re.MULTILINE)
JAVA_IMPORT_RE = re.compile(r"^\s*import\s+(?!static\b)(\w+(?:\.\w+)*(?:\.\*)?)\s*;?", re.MULTILINE)
PY_TEST_FUNC_RE = re.compile(r"^\s*(?:async\s+)?def\s+test", re.MULTILINE)


# ── JUnit XML ────────────────────────────────────────────────────────────────────────────────


@dataclass
class CaseCounts:
    executed: int = 0
    skipped: int = 0


@dataclass
class Results:
    by_classname: Dict[str, CaseCounts] = field(default_factory=lambda: defaultdict(CaseCounts))
    junit_files: int = 0
    unreadable_files: List[str] = field(default_factory=list)

    def add(self, classname: str, skipped: bool) -> None:
        counts = self.by_classname[classname]
        if skipped:
            counts.skipped += 1
        else:
            counts.executed += 1


def load_results(dirs: Sequence[str]) -> Results:
    results = Results()
    for d in dirs:
        for path in sorted(Path(d).rglob("*.xml")):
            if not path.is_file():
                continue
            try:
                root = ET.parse(path).getroot()
            except (ET.ParseError, OSError) as e:
                results.unreadable_files.append(f"{path}: {e}")
                continue
            if root.tag not in ("testsuite", "testsuites"):
                continue
            results.junit_files += 1
            for case in root.iter("testcase"):
                classname = case.get("classname") or ""
                if not classname:
                    continue
                skipped = case.find("skipped")
                # pytest reports xfail as <skipped type="pytest.xfail">, but the test did run.
                # xfail(run=False) produces the same element with a "[NOTRUN]" reason.
                ran_anyway = (
                    skipped is not None
                    and skipped.get("type") == "pytest.xfail"
                    and "[NOTRUN]" not in (skipped.get("message") or "")
                )
                results.add(classname, skipped is not None and not ran_anyway)
    return results


def counts_for_prefix(results: Results, name: str, separators: str) -> CaseCounts:
    """Sum counts over classname == name or classname starting with name + one of separators."""
    total = CaseCounts()
    prefixes = tuple(name + s for s in separators)
    for classname, counts in results.by_classname.items():
        if classname == name or classname.startswith(prefixes):
            total.executed += counts.executed
            total.skipped += counts.skipped
    return total


# ── Source parsing ───────────────────────────────────────────────────────────────────────────


# Order matters: a text block must win over a plain string, a comment over a `/` operator.
# Unterminated comments/text blocks run to end of file, like the compiler would report.
_NOISE_RE = re.compile(
    r"//[^\n]*"
    r"|/\*.*?(?:\*/|\Z)"
    r'|"""[\s\S]*?(?:"""|\Z)'
    r'|"(?:\\.|[^"\\\n])*"?'
    r"|'(?:\\.|[^'\\\n])*'?",
    re.DOTALL,
)
_NOT_NEWLINE_RE = re.compile(r"[^\n]")


def strip_comments_and_strings(text: str) -> str:
    """Blank out comments and string/char literals, keeping offsets and newlines intact.

    Without this, `{` inside a string or `@Test` inside a comment would corrupt the scan.
    Handles //, /* */, "...", '...', and Java text blocks / Kotlin raw strings (\"\"\"...\"\"\").
    """
    return _NOISE_RE.sub(lambda m: _NOT_NEWLINE_RE.sub(" ", m.group()), text)


@dataclass(frozen=True)
class TypeDecl:
    name: str
    kind: str  # class | interface | enum | record | object
    abstract: bool
    has_tests: bool
    supertypes: Tuple[str, ...]  # as written, generics stripped: `Base` or `a.b.Base`
    package: Optional[str] = None
    imports: Tuple[str, ...] = ()  # `a.b.C` and wildcard `a.b.*` imports of the file

    @property
    def fqcn(self) -> str:
        return f"{self.package}.{self.name}" if self.package else self.name


def _simple(type_name: str) -> str:
    return type_name.split(".")[-1]


_BRACE_RE = re.compile(r"[{}]")


def _matching_brace(text: str, open_idx: int) -> int:
    depth = 0
    for m in _BRACE_RE.finditer(text, open_idx):
        depth += 1 if m.group() == "{" else -1
        if depth == 0:
            return m.start()
    return len(text) - 1


def _strip_generics(header: str) -> str:
    out, depth = [], 0
    for ch in header:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(ch)
    return "".join(out)


def _supertypes(header: str, kotlin: bool) -> Tuple[str, ...]:
    header = _strip_generics(header)
    names: List[str] = []
    if kotlin:
        # Drop the primary constructor `(val x: Int)` so its `:` is not taken for the supertype list.
        header = re.sub(r"^\s*(?:@?[\w.]+\s*)*\([^)]*\)", "", header)
        m = re.search(r":\s*(.+?)(?=\s+where\b|$)", header, re.DOTALL)
        if m:
            names += [p.strip() for p in m.group(1).split(",")]
    else:
        for kw in ("extends", "implements"):
            m = re.search(rf"\b{kw}\s+(.+?)(?=\bimplements\b|\bpermits\b|$)", header, re.DOTALL)
            if m:
                names += [p.strip() for p in m.group(1).split(",")]
    out = []
    for n in names:
        n = re.sub(r"\(.*", "", n, flags=re.DOTALL).strip()  # Kotlin `Base()` constructor call
        if n:
            out.append(n)
    return tuple(out)


def parse_jvm_types(text: str, kotlin: bool = False) -> Tuple[Optional[str], List[TypeDecl]]:
    """Return (package, top-level type declarations) of a Java/Kotlin/Groovy source file."""
    code = strip_comments_and_strings(text)
    pkg_match = JAVA_PACKAGE_RE.search(code)
    package = pkg_match.group(1) if pkg_match else None
    imports = tuple(JAVA_IMPORT_RE.findall(code))

    decls: List[TypeDecl] = []
    depth = 0
    pos = 0
    region_start = 0
    while True:
        m = TYPE_DECL_RE.search(code, pos)
        if not m:
            break
        depth += code.count("{", pos, m.start()) - code.count("}", pos, m.start())
        if depth != 0:
            pos = m.end()
            continue
        open_idx = code.find("{", m.end())
        following = TYPE_DECL_RE.search(code, m.end(), open_idx if open_idx != -1 else len(code))
        if open_idx == -1 or following:
            # Body-less declaration (Kotlin `class FooTest : BaseTest()`): no methods of its
            # own, but it may inherit tests. Any `{` belongs to the next type, and annotations
            # after the header belong to the next type too.
            header_end = following.start() if following else len(code)
            header = code[m.end() : header_end].split("@", 1)[0]
            modifiers = m.group(1).split()
            decls.append(
                TypeDecl(
                    name=m.group(3),
                    kind=m.group(2),
                    abstract="abstract" in modifiers or m.group(2) == "interface",
                    has_tests=bool(TEST_ANNOTATION_RE.search(code[region_start : m.start()])),
                    supertypes=_supertypes(header, kotlin),
                    package=package,
                    imports=imports,
                )
            )
            if not following:
                break
            region_start = m.end() + len(header)
            pos = following.start()
            continue
        close_idx = _matching_brace(code, open_idx)
        modifiers = m.group(1).split()
        kind = m.group(2)
        # Annotations between the previous top-level type and this one belong to this one
        # (class-level TestNG `@Test`); imports in that span never match.
        region = code[region_start : close_idx + 1]
        decls.append(
            TypeDecl(
                name=m.group(3),
                kind=kind,
                abstract="abstract" in modifiers or kind == "interface",
                has_tests=bool(TEST_ANNOTATION_RE.search(region)),
                supertypes=_supertypes(code[m.end() : open_idx], kotlin),
                package=package,
                imports=imports,
            )
        )
        region_start = close_idx + 1
        pos = close_idx + 1
    return package, decls


def is_jvm_test_source(path: str) -> bool:
    if not path.endswith(JVM_EXTENSIONS):
        return False
    p = "/" + path
    return any(marker in p for marker in JVM_TEST_DIR_MARKERS) or path.startswith(
        JVM_TEST_DIR_PREFIXES
    )


def is_python_test_source(path: str) -> bool:
    name = os.path.basename(path)
    if not name.endswith(".py") or not (name.startswith("test_") or name.endswith("_test.py")):
        return False
    return "tests" in path.split("/")[:-1]


def python_module_name(path: str) -> str:
    """Dotted module path from the `tests` dir down, matching pytest's JUnit classname.

    pytest reports classname relative to its rootdir (the Python module root, e.g.
    metadata-ingestion/), and every Python module here keeps tests under <root>/tests.
    """
    parts = path[: -len(".py")].split("/")
    return ".".join(parts[parts.index("tests") :])


# ── Evaluation ───────────────────────────────────────────────────────────────────────────────


@dataclass
class Checked:
    path: str
    name: str  # FQCN or python module
    ok: bool
    detail: str
    opted_out_reason: Optional[str] = None


@dataclass
class Report:
    checked: List[Checked] = field(default_factory=list)
    ignored: List[Tuple[str, str]] = field(default_factory=list)  # (path or name, why)

    @property
    def failures(self) -> List[Checked]:
        return [c for c in self.checked if not c.ok and c.opted_out_reason is None]


class SourceIndex:
    """Lazily parsed JVM test sources of the repo, for abstract-base → subclass lookup."""

    def __init__(self, paths: Iterable[str], read: Callable[[str], Optional[str]]):
        self._paths = [p for p in paths if is_jvm_test_source(p)]
        self._read = read
        self._built = False
        # simple name -> [(fqcn, decl)] of the types that extend/implement it
        self._subclasses: Dict[str, List[Tuple[str, TypeDecl]]] = defaultdict(list)
        # simple name -> decls with that name (simple-name resolution, like the subclass lookup)
        self._by_name: Dict[str, List[TypeDecl]] = defaultdict(list)

    def _build(self) -> None:
        if self._built:
            return
        self._built = True
        for p in self._paths:
            text = self._read(p)
            if text is None:
                continue
            package, decls = parse_jvm_types(text, kotlin=p.endswith(".kt"))
            for d in decls:
                self._by_name[d.name].append(d)
                for sup in d.supertypes:
                    self._subclasses[_simple(sup)].append((d.fqcn, d))

    def inherits_tests(self, decl: TypeDecl) -> bool:
        """True when a supertype (transitively, within test sources) declares test methods.

        A concrete `FooEs8Test extends FooTestBase` with only overrides is exactly the class a
        suite split drops, so it must be checked even though it has no @Test of its own.
        """
        self._build()
        seen: Set[int] = set()
        queue = [decl]
        while queue:
            child = queue.pop()
            for name in child.supertypes:
                for parent in self._resolve(name, child):
                    if id(parent) in seen:
                        continue
                    seen.add(id(parent))
                    if parent.has_tests:
                        return True
                    queue.append(parent)
        return False

    def _resolve(self, type_name: str, child: TypeDecl) -> List[TypeDecl]:
        """Test-source classes a supertype reference in `child` can mean, resolved like javac.

        A qualified name must match exactly. A simple name resolves through an explicit
        import, then a wildcard import, then the child's own package. If none match, the
        supertype is not a test-source class (a main-source or library type that happens to
        share a test class's simple name).
        """
        candidates = self._by_name.get(_simple(type_name), [])
        if "." in type_name:
            return [d for d in candidates if d.fqcn == type_name]
        explicit = [d for d in candidates if d.fqcn in child.imports]
        if explicit:
            return explicit
        wildcard = [d for d in candidates if f"{d.package}.*" in child.imports]
        if wildcard:
            return wildcard
        return [d for d in candidates if d.package == child.package]

    def concrete_subclasses(self, base: TypeDecl) -> List[str]:
        """FQCNs of concrete classes that extend `base` (transitively), resolved per edge."""
        self._build()
        seen: Set[str] = set()
        concrete: List[str] = []
        queue = [base]
        while queue:
            parent = queue.pop()
            for fqcn, decl in self._subclasses.get(parent.name, []):
                if fqcn in seen or not any(
                    r.fqcn == parent.fqcn
                    for sup in decl.supertypes
                    if _simple(sup) == parent.name
                    for r in self._resolve(sup, decl)
                ):
                    continue
                seen.add(fqcn)
                if decl.abstract:
                    queue.append(decl)
                else:
                    concrete.append(fqcn)
        return sorted(concrete)


def _describe(counts: CaseCounts) -> str:
    if counts.executed:
        return f"{counts.executed} executed" + (
            f", {counts.skipped} skipped" if counts.skipped else ""
        )
    if counts.skipped:
        return f"0 executed, all {counts.skipped} skipped"
    return "no test cases in the JUnit results"


def evaluate(
    changed: Sequence[str],
    read: Callable[[str], Optional[str]],
    results: Results,
    optouts: Dict[str, str],
    languages: Set[str],
    source_index: SourceIndex,
) -> Report:
    report = Report()

    def optout_for(path: str, name: str) -> Optional[str]:
        for key, reason in optouts.items():
            if fnmatch.fnmatchcase(name, key) or fnmatch.fnmatchcase(path, key):
                return reason
        return None

    for path in sorted(set(changed)):
        if "java" in languages and is_jvm_test_source(path):
            text = read(path)
            if text is None:
                report.ignored.append((path, "file not readable (deleted?)"))
                continue
            package, decls = parse_jvm_types(text, kotlin=path.endswith(".kt"))
            test_decls = [
                d
                for d in decls
                if d.has_tests or (not d.abstract and source_index.inherits_tests(d))
            ]
            if not test_decls:
                report.ignored.append((path, "no test annotations (helper/fixture)"))
                continue
            for d in test_decls:
                fqcn = f"{package}.{d.name}" if package else d.name
                if d.abstract:
                    subs = source_index.concrete_subclasses(d)
                    ran = [s for s in subs if counts_for_prefix(results, s, "$").executed > 0]
                    if ran:
                        ok = True
                        detail = f"abstract; ran via {', '.join(ran[:3])}" + (
                            f" (+{len(ran) - 3} more)" if len(ran) > 3 else ""
                        )
                    elif subs:
                        ok = False
                        detail = (
                            "abstract; none of its concrete subclasses executed "
                            f"({', '.join(subs[:3])}{' ...' if len(subs) > 3 else ''})"
                        )
                    else:
                        ok = False
                        detail = "abstract with no concrete subclass in test sources"
                else:
                    counts = counts_for_prefix(results, fqcn, "$")
                    ok = counts.executed > 0
                    detail = _describe(counts)
                report.checked.append(
                    Checked(path, fqcn, ok, detail, None if ok else optout_for(path, fqcn))
                )
        elif "python" in languages and is_python_test_source(path):
            text = read(path)
            if text is None:
                report.ignored.append((path, "file not readable (deleted?)"))
                continue
            if not PY_TEST_FUNC_RE.search(text):
                report.ignored.append((path, "no test functions"))
                continue
            module = python_module_name(path)
            counts = counts_for_prefix(results, module, ".")
            ok = counts.executed > 0
            report.checked.append(
                Checked(
                    path, module, ok, _describe(counts), None if ok else optout_for(path, module)
                )
            )
    return report


# ── I/O helpers ──────────────────────────────────────────────────────────────────────────────


def _git(repo_root: str, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", repo_root, *args], check=True, capture_output=True, text=True
    ).stdout


def changed_files_from_git(repo_root: str, base: str, head: str) -> List[str]:
    merge_base = _git(repo_root, "merge-base", base, head).strip()
    out = _git(repo_root, "diff", "-z", "--name-only", "--diff-filter=AMR", "-M", merge_base, head)
    return [p for p in out.split("\0") if p]


class GitBlobReader:
    """Reads files at a git rev through one long-lived `git cat-file --batch`.

    A `git show` per file is ~10x slower, and the abstract-base subclass index reads every test
    source (~2000 files here).
    """

    def __init__(self, repo_root: str, rev: str):
        self._rev = rev
        self._proc = subprocess.Popen(
            ["git", "-C", repo_root, "cat-file", "--batch"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    def __call__(self, path: str) -> Optional[str]:
        assert self._proc.stdin is not None and self._proc.stdout is not None
        self._proc.stdin.write(f"{self._rev}:{path}\n".encode())
        self._proc.stdin.flush()
        header = self._proc.stdout.readline().decode().split()
        if len(header) < 3 or header[1] != "blob":  # "<spec> missing"
            return None
        data = self._proc.stdout.read(int(header[2]) + 1)[:-1]  # drop the trailing LF
        return data.decode("utf-8", errors="replace")

    def close(self) -> None:
        assert self._proc.stdin is not None and self._proc.stdout is not None
        self._proc.stdin.close()
        self._proc.wait()
        self._proc.stdout.close()


def read_worktree_file(repo_root: str, path: str) -> Optional[str]:
    try:
        return (Path(repo_root) / path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def list_repo_files(repo_root: str, rev: Optional[str]) -> List[str]:
    args = ["ls-tree", "-r", "-z", "--name-only", rev] if rev else ["ls-files", "-z"]
    try:
        return [p for p in _git(repo_root, *args).split("\0") if p]
    except (subprocess.CalledProcessError, FileNotFoundError):
        root = Path(repo_root)
        return [str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()]


def load_optouts(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a JSON object of name -> reason")
    bad = [k for k, v in data.items() if not isinstance(v, str) or not v.strip()]
    if bad:
        raise ValueError(f"{path}: opt-out entries need a non-empty reason: {', '.join(bad)}")
    return {k: v for k, v in data.items() if not k.startswith("_")}


def path_selected(path: str, includes: Sequence[str], excludes: Sequence[str]) -> bool:
    if includes and not any(fnmatch.fnmatch(path, g) for g in includes):
        return False
    return not any(fnmatch.fnmatch(path, g) for g in excludes)


def render(report: Report, results: Results, annotate: bool) -> str:
    lines: List[str] = []
    lines.append(
        f"Read {results.junit_files} JUnit XML files "
        f"({len(results.by_classname)} distinct test classnames)."
    )
    for bad in results.unreadable_files:
        lines.append(f"WARNING: unreadable XML skipped: {bad}")
    if not report.checked:
        lines.append("No added or modified test classes in scope.")
    for c in report.checked:
        if c.ok:
            status = "RAN    "
        elif c.opted_out_reason is not None:
            status = "OPT-OUT"
        else:
            status = "NOT RUN"
        suffix = f" — opted out: {c.opted_out_reason}" if c.opted_out_reason else ""
        lines.append(f"  {status} {c.name} ({c.detail}){suffix}  [{c.path}]")
    for path, why in report.ignored:
        lines.append(f"  skipped {path}: {why}")
    if report.failures:
        lines.append("")
        lines.append(
            f"FAIL: {len(report.failures)} added/modified test class(es) did not execute in this "
            "CI run. Fix the suite wiring (TestNG suite XML, Gradle include/exclude, test task, "
            "CI job) so they run, or add them to "
            f"{DEFAULT_OPTOUT_FILE} with a reason."
        )
        if annotate:
            for c in report.failures:
                lines.append(
                    f"::error file={c.path},title=Test class did not run::{c.name}: {c.detail}"
                )
    return "\n".join(lines)


def render_markdown(report: Report) -> str:
    if not report.checked:
        return "### New/modified tests ran\n\nNo added or modified test classes in scope.\n"
    rows = ["| Status | Test | Result |", "|---|---|---|"]
    for c in report.checked:
        status = "✅ ran" if c.ok else ("⏭️ opt-out" if c.opted_out_reason else "❌ not run")
        detail = c.detail + (f" — {c.opted_out_reason}" if c.opted_out_reason else "")
        rows.append(f"| {status} | `{c.name}` | {detail} |")
    head = "### New/modified tests ran\n\n"
    if report.failures:
        head += f"**{len(report.failures)} test class(es) did not execute.**\n\n"
    return head + "\n".join(rows) + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--repo-root", default=".")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--base", help="Base ref; changed files = diff(merge-base(base, head), head)")
    src.add_argument(
        "--changed-files-from",
        help="File listing changed repo-relative paths, one per line ('-' for stdin)",
    )
    src.add_argument(
        "--all", action="store_true", help="Check every test source in the repo (census mode)"
    )
    ap.add_argument(
        "--head",
        help="Read sources at this git rev instead of the working tree (also diff head for --base)",
    )
    ap.add_argument(
        "--results-dir",
        action="append",
        required=True,
        help="Directory with JUnit XML (searched recursively); repeatable",
    )
    ap.add_argument(
        "--language",
        action="append",
        choices=["java", "python"],
        help="Languages to check (default: java). 'java' covers Java/Kotlin/Groovy.",
    )
    ap.add_argument("--include", action="append", default=[], help="fnmatch glob of paths to check")
    ap.add_argument("--exclude", action="append", default=[], help="fnmatch glob of paths to skip")
    ap.add_argument(
        "--optout-file", default=None, help=f"default: <repo-root>/{DEFAULT_OPTOUT_FILE}"
    )
    ap.add_argument("--github-annotations", action="store_true", help="Emit ::error annotations")
    args = ap.parse_args(argv)

    repo_root = args.repo_root
    languages = set(args.language or ["java"])
    if args.head:
        try:
            _git(repo_root, "rev-parse", "--verify", "--quiet", f"{args.head}^{{commit}}")
        except (subprocess.CalledProcessError, OSError):
            # Otherwise every file reads as "missing" and the check passes vacuously.
            print(f"error: --head {args.head} is not a commit in {repo_root}", file=sys.stderr)
            return 2
    git_reader = GitBlobReader(repo_root, args.head) if args.head else None
    try:
        return _run(args, repo_root, languages, git_reader)
    finally:
        if git_reader:
            git_reader.close()


def _run(
    args: argparse.Namespace,
    repo_root: str,
    languages: Set[str],
    git_reader: Optional[GitBlobReader],
) -> int:
    reader: Callable[[str], Optional[str]] = git_reader or (
        lambda path: read_worktree_file(repo_root, path)
    )

    try:
        if args.base:
            changed = changed_files_from_git(repo_root, args.base, args.head or "HEAD")
        elif args.all:
            changed = list_repo_files(repo_root, args.head)
        else:
            stream = sys.stdin if args.changed_files_from == "-" else open(args.changed_files_from)
            with stream:
                changed = [line.strip() for line in stream if line.strip()]
        optouts = load_optouts(Path(args.optout_file or Path(repo_root) / DEFAULT_OPTOUT_FILE))
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    for d in args.results_dir:
        if not Path(d).is_dir():
            print(f"error: results dir {d} does not exist", file=sys.stderr)
            return 2

    changed = [p for p in changed if path_selected(p, args.include, args.exclude)]
    results = load_results(args.results_dir)
    index = SourceIndex(list_repo_files(repo_root, args.head), reader)
    report = evaluate(changed, reader, results, optouts, languages, index)

    print(render(report, results, args.github_annotations))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(render_markdown(report))
    return 1 if report.failures else 0


if __name__ == "__main__":
    sys.exit(main())
