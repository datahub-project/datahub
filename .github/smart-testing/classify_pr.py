"""Classify a PR against the smart-testing taxonomy (runs in CI's smoke_test_matrix job).

Framework-agnostic: validates the LLM answer against ``taxonomy.yaml`` next to
this script, plus ``taxonomy.overlay.yaml`` when present, and writes ``smart_tags`` to ``$GITHUB_OUTPUT``, a JSON object of
``{"<dimension>": [tags...], "confidence": ...}``. The request schema is built
from that file, so the model can only emit known tags. Each framework's
selector reads it (smoke-test pytest: ``SMOKE_SMART_TAGS``). An empty value
means "no trusted selection" (the smoke-test matrix runs its p0 fail-safe) and
is the answer to every failure, so a broken classifier costs time, not coverage.

The ``change-type`` dimension (CI, build, test infrastructure, docs, tooling) is
also set from the changed paths: each file matching a tag's ``paths`` globs in
``taxonomy.yaml`` adds that tag, and a PR made only of such files is classified
without calling the model. ``pytest-plumbing`` (a change to the shared pytest
harness) comes only from paths, so the request schema leaves it out.

Alongside ``smart_tags`` it writes ``failsafe_reason`` (empty unless it fell
back), ``confidence``, ``model``, ``classifier_duration_s`` and, when the
model's answer parses as JSON, ``proposed_tags``: that answer before
validation, so a fallback can be reviewed. CI sends these to PostHog. It also
writes ``reasoning`` (the model's one-line justification) and ``changed_files``
for the PR comment.

Usage:
    pip install -r .github/smart-testing/requirements.txt
    python .github/smart-testing/classify_pr.py            # CI mode
    python .github/smart-testing/classify_pr.py --dry-run  # no LLM call

Environment:
    GITHUB_REPOSITORY  owner/repo
    BASE_SHA, HEAD_SHA PR base and head commits (any target branch)
    GH_TOKEN           token for ``gh api`` (contents:read is enough)
    PR_LABELS          comma-separated labels; ``run-all-tests`` forces full suite
    ANTHROPIC_FEDERATION_RULE_ID, ANTHROPIC_ORGANIZATION_ID,
    ANTHROPIC_SERVICE_ACCOUNT_ID, ANTHROPIC_WORKSPACE_ID,
    ANTHROPIC_IDENTITY_TOKEN_FILE
                       Workload Identity Federation settings read by the
                       Anthropic SDK (ANTHROPIC_API_KEY also works, and
                       takes precedence if set)
"""

import argparse
import fnmatch
import functools
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import anthropic
import yaml
from smart_testing_constants import (
    CHANGE_TYPE,
    FEATURE_DIMENSIONS,
    PYTEST_PLUMBING,
    RUN_ALL_TESTS_LABEL,
    TEST_INFRA,
    FailSafeReason,
)

TAXONOMY_YAML = Path(__file__).resolve().parent / "taxonomy.yaml"
# Tags for features only one repository has; same structure, merged into the base.
TAXONOMY_OVERLAY_YAML = TAXONOMY_YAML.with_name("taxonomy.overlay.yaml")
MODEL = "claude-haiku-5-5"

# The compare API returns at most this many files. A PR at the cap may be
# missing files, so it runs the full suite instead of a partial guess.
COMPARE_FILE_LIMIT = 300
# A PR with no feature tags runs only the canary (always-run modules plus its
# own changed tests) when it is this small. A larger one has a blast radius the
# classifier did not pin down, so it gets the p0 fail-safe (empty tags).
CANARY_MAX_CHANGED_FILES = 15
# Patch budget for the prompt. The file list is always sent in full; patches
# are trimmed per file, then dropped once the total budget is spent.
MAX_PATCH_BYTES_PER_FILE = 4_000
MAX_PATCH_BYTES_TOTAL = 60_000

SYSTEM_PROMPT = """\
You classify code changes against a concept taxonomy for test selection.
Use ONLY tags that appear in the taxonomy provided, under the dimension they
are listed in. Do not invent new tags."""

USER_PROMPT_TEMPLATE = """\
## Changed files (status, path):
{changed_files}

## Patches (trimmed; some files may have none):
{patches}

## Taxonomy of known concepts:
{taxonomy_content}

Classify this change. For entity-registry.yml and *.pdl changes, determine
WHICH specific entities and aspects changed — do not tag the whole registry.
For application.yaml, Spring factory, or configuration file changes, identify
which FEATURE the config controls — tag the feature, not just the infrastructure
layer. A GraphQL concurrency config change affects search; an auth config change
affects authentication.

Tag `change-type` for CI, build tooling, test infrastructure, docs or developer
tooling. When the change touches no product code, `change-type` tags with no
feature tags are a complete HIGH-confidence answer: do not borrow feature tags,
and do not lower confidence because no feature tag fits. When it also changes
product code, tag those features as usual; `change-type` adds to feature tags and
never replaces them."""


class FailSafe(Exception):
    """No trusted selection: CI runs its p0 fail-safe."""

    def __init__(self, reason: FailSafeReason, confidence: str = "") -> None:
        super().__init__(reason.value)
        self.reason = reason
        self.confidence = confidence


def log(msg: str) -> None:
    print(f"[smart-testing] {msg}", file=sys.stderr)


def write_output(key: str, value: str) -> None:
    output_file = os.environ.get("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"{key}={value}\n")


def load_dimensions(path: Path = TAXONOMY_YAML) -> Dict[str, dict]:
    return yaml.safe_load(path.read_text())["dimensions"]


PathRules = Tuple[Tuple[str, str], ...]


def load_path_rules() -> PathRules:
    """(glob, change-type tag) pairs from the change-type tags' ``paths``.

    The overlay adds globs for files only its repository has under
    ``change-type-paths`` (tag → globs), so every base glob matches in both.
    """
    rules = [
        (pattern, entry["tag"])
        for entry in load_dimensions(TAXONOMY_YAML)[CHANGE_TYPE]["tags"]
        for pattern in entry.get("paths") or []
    ]
    if TAXONOMY_OVERLAY_YAML.exists():
        extra = yaml.safe_load(TAXONOMY_OVERLAY_YAML.read_text()).get(
            "change-type-paths"
        )
        rules.extend((p, tag) for tag, patterns in (extra or {}).items() for p in patterns)
    return tuple(rules)


def change_types_for(path: str, rules: PathRules) -> Set[str]:
    """change-type tags a file's path implies; empty for product files.

    Matching ignores case, as the old docs suffix check did (``.PNG``).
    Anything no glob matches counts as product code, including docker/ and
    extension-less files such as Dockerfiles, so an unknown kind of file errs
    toward running tests.
    """
    lowered = path.lower()
    tags = {
        tag for pattern, tag in rules if fnmatch.fnmatchcase(lowered, pattern.lower())
    }
    if PYTEST_PLUMBING in tags:
        tags.add(TEST_INFRA)
    return tags


@dataclass
class ChangedFile:
    path: str
    status: str
    patch: str


def get_pr_changes() -> List[ChangedFile]:
    """Files and patches this PR introduces, whatever its target branch.

    Three dots compare HEAD against the merge base, so commits that landed on
    the target branch after the PR branched off are excluded however far behind
    the branch is. A two-dot diff against the base tip (what dorny/paths-filter
    does without a token) would include them.
    """
    repo = os.environ["GITHUB_REPOSITORY"]
    base, head = os.environ["BASE_SHA"], os.environ["HEAD_SHA"]
    result = subprocess.run(
        ["gh", "api", f"repos/{repo}/compare/{base}...{head}"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    files = json.loads(result.stdout).get("files") or []
    return [
        ChangedFile(
            path=f["filename"], status=f.get("status", ""), patch=f.get("patch", "")
        )
        for f in files
    ]


def format_patches(changed: List[ChangedFile]) -> str:
    parts: List[str] = []
    budget = MAX_PATCH_BYTES_TOTAL
    for f in changed:
        if not f.patch or budget <= 0:
            continue
        patch = f.patch.encode()[:MAX_PATCH_BYTES_PER_FILE].decode(errors="ignore")
        if len(f.patch.encode()) > MAX_PATCH_BYTES_PER_FILE:
            patch += "\n[... patch trimmed]"
        patch = patch.encode()[:budget].decode(errors="ignore")
        budget -= len(patch.encode())
        parts.append(f"### {f.path}\n{patch}")
    return "\n\n".join(parts) or "(none)"


def taxonomy_files() -> List[Path]:
    return [p for p in (TAXONOMY_YAML, TAXONOMY_OVERLAY_YAML) if p.exists()]


def load_known_tags() -> Dict[str, Set[str]]:
    """Dimension → tags the model may answer with, from the taxonomy and its overlay.

    Path-assigned tags (``path-assigned: true``) are left out: they come from
    ``change_types_for`` alone, so the request schema never offers them.
    """
    known: Dict[str, Set[str]] = {}
    for path in taxonomy_files():
        for dim, body in load_dimensions(path).items():
            known.setdefault(dim, set()).update(
                entry["tag"] for entry in body["tags"] if not entry.get("path-assigned")
            )
    return known


CONFIDENCE_LEVELS = ("HIGH", "MEDIUM", "LOW")


def classification_schema(known: Dict[str, Set[str]]) -> Dict[str, object]:
    """Response schema. Tag enums come from the taxonomy, so unknown tags cannot be sampled.

    Every property is required: Anthropic counts optional properties toward the
    structured-output complexity limit, and an empty array means "no tags".
    """
    properties: Dict[str, object] = {
        dim: {"type": "array", "items": {"type": "string", "enum": sorted(tags)}}
        for dim, tags in known.items()
    }
    properties["confidence"] = {"type": "string", "enum": list(CONFIDENCE_LEVELS)}
    properties["reasoning"] = {
        "type": "string",
        "description": "One short sentence explaining the classification.",
    }
    return {
        "type": "object",
        "properties": properties,
        "required": [*known.keys(), "confidence", "reasoning"],
        "additionalProperties": False,
    }


@dataclass
class PRTags:
    tags: Dict[str, Set[str]]
    confidence: str

    def to_json(self) -> str:
        payload: Dict[str, object] = {
            dim: sorted(values) for dim, values in self.tags.items() if values
        }
        payload["confidence"] = self.confidence
        return json.dumps(payload, sort_keys=True)


@functools.lru_cache(maxsize=None)
def anthropic_client() -> anthropic.Anthropic:
    """One client per process. With Workload Identity Federation the client
    exchanges the GitHub identity token once and caches the access token. A
    second client exchanges the same identity token again, and that exchange
    fails with HTTP 401 (seen when replay_prs.py classified PRs back to back).
    """
    return anthropic.Anthropic()


def call_llm(changed: List[ChangedFile], known: Dict[str, Set[str]]) -> str:
    client = anthropic_client()
    response = client.messages.create(
        model=MODEL,
        # Haiku 5.5 thinks before answering, and the thinking counts against
        # max_tokens. Low effort keeps it short; 4096 leaves room for both.
        max_tokens=4096,
        output_config={
            "effort": "low",
            "format": {"type": "json_schema", "schema": classification_schema(known)},
        },
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": USER_PROMPT_TEMPLATE.format(
                    changed_files="\n".join(f"{f.status}\t{f.path}" for f in changed),
                    patches=format_patches(changed),
                    taxonomy_content="\n".join(p.read_text() for p in taxonomy_files()),
                ),
            }
        ],
    )
    # refusal and max_tokens can return text that does not match the schema.
    if response.stop_reason != "end_turn":
        log(f"ERROR: LLM stop_reason {response.stop_reason!r}")
        raise RuntimeError(f"LLM stop_reason {response.stop_reason}")
    return next((b.text for b in response.content if b.type == "text"), "")


def record_llm_output(text: str) -> None:
    """Log and output the model's whole answer before any validation or fail-safe."""
    try:
        compact = json.dumps(json.loads(text), sort_keys=True)
    except json.JSONDecodeError:
        log(f"LLM output (unparseable): {text.encode('unicode_escape').decode()}")
        return
    log(f"LLM output: {compact}")
    write_output("proposed_tags", compact)


def pr_tags_from_llm(text: str, known: Dict[str, Set[str]]) -> PRTags:
    """Validated PR tags; raises FailSafe when the answer can't be trusted."""
    data = json.loads(text)
    confidence = str(data.pop("confidence", "UNKNOWN")).upper()
    # Written to $GITHUB_OUTPUT and PostHog: anything but a known level is
    # recorded as UNKNOWN. The tags are still used.
    if confidence not in CONFIDENCE_LEVELS:
        log(f"Unexpected confidence {confidence!r}; recording UNKNOWN")
        confidence = "UNKNOWN"
    reasoning = data.pop("reasoning", "")
    if reasoning:
        log(f"LLM reasoning: {reasoning}")
        write_output("reasoning", " ".join(str(reasoning).split()))
    extra = sorted(set(data) - set(known))
    if extra:
        log(f"Ignoring unexpected LLM keys: {extra}")

    tags: Dict[str, Set[str]] = {}
    dropped = False
    for dim, allowed in known.items():
        # Enum-constrained sampling does not guarantee capitalization.
        canonical = {tag.lower(): tag for tag in allowed}
        kept: Set[str] = set()
        unknown: List[str] = []
        for raw in data.get(dim) or []:
            tag = canonical.get(str(raw).lower())
            if tag is None:
                unknown.append(str(raw))
            else:
                kept.add(tag)
        if unknown:
            dropped = True
            log(f"ERROR: LLM returned unknown {dim} tag(s) {sorted(unknown)}; dropped")
        if kept:
            tags[dim] = kept

    if confidence == "LOW":
        log("LOW confidence — running full suite")
        raise FailSafe(FailSafeReason.LOW_CONFIDENCE, confidence)
    if dropped and not any(tags.get(dim) for dim in FEATURE_DIMENSIONS):
        log("Only unknown feature tags remained — running full suite")
        raise FailSafe(FailSafeReason.UNKNOWN_FEATURE_TAGS, confidence)
    return PRTags(tags=tags, confidence=confidence)


def canary_or_fail_safe(pr: PRTags, changed_count: int) -> PRTags:
    """Keep a no-feature-tags result only for small PRs; otherwise run p0."""
    if any(pr.tags.get(dim) for dim in FEATURE_DIMENSIONS):
        return pr
    if changed_count < CANARY_MAX_CHANGED_FILES:
        log(f"No feature tags, {changed_count} changed files — canary only")
        return pr
    log(
        f"No feature tags but {changed_count} changed files "
        f"(>= {CANARY_MAX_CHANGED_FILES}) — p0 fail-safe"
    )
    raise FailSafe(FailSafeReason.LARGE_PR_NO_FEATURE_TAGS, pr.confidence)


def classify(dry_run: bool) -> PRTags:
    labels = os.environ.get("PR_LABELS", "").split(",")
    if RUN_ALL_TESTS_LABEL in labels:
        log("run-all-tests label — running full suite")
        raise FailSafe(FailSafeReason.RUN_ALL_TESTS_LABEL)
    return classify_changes(get_pr_changes(), dry_run)


def classify_changes(changed: List[ChangedFile], dry_run: bool) -> PRTags:
    """Classify a PR from its changed files."""
    log(f"Changed files: {len(changed)}")
    write_output("changed_files", str(len(changed)))
    if not changed:
        log("No changed files reported — running full suite")
        raise FailSafe(FailSafeReason.NO_CHANGED_FILES)
    if len(changed) >= COMPARE_FILE_LIMIT:
        log(f"{len(changed)} files hits the compare API cap — running full suite")
        raise FailSafe(FailSafeReason.COMPARE_FILE_CAP)
    rules = load_path_rules()
    per_file = [change_types_for(f.path, rules) for f in changed]
    path_tags: Set[str] = set().union(*per_file)
    if all(per_file):
        log(f"Only non-product files changed: change-type {sorted(path_tags)}")
        write_output(
            "reasoning",
            "Only CI, test-infrastructure, docs or tooling files changed, so the classifier was not called.",
        )
        return canary_or_fail_safe(
            PRTags(tags={CHANGE_TYPE: path_tags}, confidence="HIGH"), len(changed)
        )
    if dry_run:
        log("Dry run — skipping LLM call; running full suite")
        raise FailSafe(FailSafeReason.DRY_RUN)

    log("Classifying PR with LLM...")
    known = load_known_tags()
    # Product files first: the patch budget runs out in file order, and a big
    # tooling diff would otherwise hide the product patches from the model.
    product_first = sorted(changed, key=lambda f: bool(change_types_for(f.path, rules)))
    text = call_llm(product_first, known)
    record_llm_output(text)
    pr = pr_tags_from_llm(text, known)
    # Add the change types the paths imply to the model's answer. This is the
    # only way pytest-plumbing gets in, since the schema does not offer it.
    if path_tags:
        pr.tags[CHANGE_TYPE] = pr.tags.get(CHANGE_TYPE, set()) | path_tags
    return canary_or_fail_safe(pr, len(changed))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="No LLM call")
    args = parser.parse_args()

    pr: Optional[PRTags] = None
    failsafe_reason: Optional[FailSafeReason] = None
    confidence = ""
    start = time.monotonic()
    try:
        pr = classify(args.dry_run)
        confidence = pr.confidence
    except FailSafe as exc:
        failsafe_reason = exc.reason
        confidence = exc.confidence
    except Exception as exc:
        log(f"ERROR: {exc!r} — running full suite")
        failsafe_reason = FailSafeReason.ERROR
    duration = time.monotonic() - start

    smart_tags = pr.to_json() if pr else ""
    log(f"PR tags: {smart_tags or '(none: p0 fail-safe)'}")
    write_output("smart_tags", smart_tags)
    write_output(
        "failsafe_reason", "" if failsafe_reason is None else failsafe_reason.value
    )
    write_output("confidence", confidence)
    write_output("model", MODEL)
    write_output("classifier_duration_s", f"{duration:.1f}")


if __name__ == "__main__":
    main()
