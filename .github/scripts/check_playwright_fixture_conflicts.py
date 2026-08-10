#!/usr/bin/env python3
"""
Detect entities redefined with conflicting payloads across Playwright fixture files.

Every Playwright spec file can bring its own tests/{feature}/fixtures/data.json,
seeded on top of the shared test-data/data.json (see seeding.fixture.ts). Aspect
ingestion REPLACES a record rather than merging it, so when two different fixture
files define the same (urn, aspectName) with different payloads, whichever one
seeds last silently wins. Global-vs-feature ordering is deterministic (feature
always seeds after global), but feature-vs-feature ordering depends on worker
scheduling -- invisible at workers: 1, a live flake source as workers_per_shard
increases (see PFP-5479 / PFP-5484, where this exact pattern broke a relationship
a global seed had correctly set).

This script has no way to know whether a given collision is accidental (two
features happened to reuse a name) or intentional (a feature deliberately
customizes a shared entity, relying on seed-order to win) -- it only reports
disagreement. New conflicts introduced since the checked-in baseline fail the
check; pre-existing ones are grandfathered in until someone deliberately
resolves them (see BASELINE_FILE).
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

PEGASUS2AVRO_PREFIX = re.compile(r"com\.linkedin\.pegasus2avro\.")

# Class names whose simple-name-to-aspectName conversion isn't a plain
# lowercase-first-letter (the acronym prefix breaks that rule).
ASPECT_NAME_OVERRIDES = {
    "MLFeatureProperties": "mlFeatureProperties",
    "MLFeatureTableProperties": "mlFeatureTableProperties",
    "MLModelProperties": "mlModelProperties",
    "MLModelGroupProperties": "mlModelGroupProperties",
    "MLPrimaryKeyProperties": "mlPrimaryKeyProperties",
}

# (urn, aspectName) pairs already known to conflict as of the initial baseline.
# See helpers/seeder-utils.ts / fixtures/seeding.fixture.ts for why this can
# happen at all, and PFP-5492 for the full inventory and suggested fixes.
# Regenerate with: python3 check_playwright_fixture_conflicts.py --write-baseline
DEFAULT_BASELINE_FILE = Path(__file__).parent / "playwright_fixture_conflicts_baseline.json"


def discover_fixture_files(playwright_dir: Path) -> List[Path]:
    files = [playwright_dir / "test-data" / "data.json"]
    files.extend(sorted((playwright_dir / "tests").glob("*/fixtures/data.json")))
    return [f for f in files if f.exists()]


def simple_class_name(class_name: str) -> str:
    return class_name.rsplit(".", 1)[-1]


def class_name_to_aspect_name(class_name: str) -> str:
    simple = simple_class_name(class_name)
    if simple in ASPECT_NAME_OVERRIDES:
        return ASPECT_NAME_OVERRIDES[simple]
    return simple[0].lower() + simple[1:] if simple else simple


def normalize_payload(value: object) -> object:
    """
    Strip the legacy pegasus2avro namespace so a snapshot-format aspect and its
    functionally-identical native-MCP counterpart compare equal (mirrors the
    same normalisation ingestMcps applies before posting -- see
    fixtures/seeding.fixture.ts's raw.replace(/com\\.linkedin\\.pegasus2avro\\./g, ...)).
    """
    if isinstance(value, str):
        return PEGASUS2AVRO_PREFIX.sub("com.linkedin.", value)
    if isinstance(value, list):
        return [normalize_payload(v) for v in value]
    if isinstance(value, dict):
        return {PEGASUS2AVRO_PREFIX.sub("com.linkedin.", k): normalize_payload(v) for k, v in value.items()}
    return value


def extract_entries(data: object, file_label: str) -> List[Tuple[str, str, object]]:
    """Return (urn, aspectName, normalized_payload) for every aspect in one fixture file."""
    if not isinstance(data, list):
        return []

    entries: List[Tuple[str, str, object]] = []
    for mcp in data:
        if not isinstance(mcp, dict):
            continue

        snapshot = mcp.get("proposedSnapshot")
        if isinstance(snapshot, dict):
            for snap_value in snapshot.values():
                urn = snap_value.get("urn") if isinstance(snap_value, dict) else None
                aspects = snap_value.get("aspects") if isinstance(snap_value, dict) else None
                if not urn or not isinstance(aspects, list):
                    continue
                for aspect in aspects:
                    if not isinstance(aspect, dict):
                        continue
                    for class_name, payload in aspect.items():
                        aspect_name = class_name_to_aspect_name(class_name)
                        entries.append((urn, aspect_name, normalize_payload(payload)))
            continue

        entity_urn = mcp.get("entityUrn")
        aspect_name = mcp.get("aspectName")
        aspect = mcp.get("aspect")
        if not entity_urn or not aspect_name or not isinstance(aspect, dict):
            continue
        raw_value = aspect.get("value")
        if not isinstance(raw_value, str):
            continue
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            print(f"Warning: {file_label}: could not parse aspect.value JSON for {entity_urn}/{aspect_name}", file=sys.stderr)
            continue
        entries.append((entity_urn, aspect_name, normalize_payload(payload)))

    return entries


def find_conflicts(
    fixture_files: List[Path], playwright_dir: Path
) -> Dict[Tuple[str, str], List[Tuple[str, object]]]:
    """
    Group every (urn, aspectName) by its distinct payloads across files.
    Returns only keys with more than one distinct payload, mapped to
    [(file_label, payload), ...] -- one entry per distinct payload, each
    annotated with the first file that produced it.
    """
    by_key: Dict[Tuple[str, str], List[Tuple[str, object]]] = {}

    for path in fixture_files:
        file_label = str(path.relative_to(playwright_dir))
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"Warning: {file_label}: invalid JSON ({e})", file=sys.stderr)
            continue

        for urn, aspect_name, payload in extract_entries(data, file_label):
            key = (urn, aspect_name)
            existing = by_key.setdefault(key, [])
            if not any(payload == seen_payload for _, seen_payload in existing):
                existing.append((file_label, payload))

    return {key: variants for key, variants in by_key.items() if len(variants) > 1}


def load_baseline(baseline_file: Path) -> set:
    if not baseline_file.exists():
        return set()
    data = json.loads(baseline_file.read_text(encoding="utf-8"))
    return {(item["urn"], item["aspectName"]) for item in data}


def write_baseline(baseline_file: Path, conflicts: Dict[Tuple[str, str], List[Tuple[str, object]]]) -> None:
    data = [
        {"urn": urn, "aspectName": aspect_name, "files": sorted(f for f, _ in variants)}
        for (urn, aspect_name), variants in sorted(conflicts.items())
    ]
    baseline_file.write_text(json.dumps(data, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--playwright-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "e2e-test" / "ui" / "playwright",
        help="Path to e2e-test/ui/playwright",
    )
    parser.add_argument("--baseline-file", type=Path, default=DEFAULT_BASELINE_FILE)
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="Write current conflicts as the new baseline instead of checking against it",
    )
    args = parser.parse_args()

    if not args.playwright_dir.is_dir():
        print(f"Error: playwright dir does not exist: {args.playwright_dir}", file=sys.stderr)
        sys.exit(1)

    fixture_files = discover_fixture_files(args.playwright_dir)
    conflicts = find_conflicts(fixture_files, args.playwright_dir)

    if args.write_baseline:
        write_baseline(args.baseline_file, conflicts)
        print(f"Wrote {len(conflicts)} known conflict(s) to {args.baseline_file}")
        return

    baseline = load_baseline(args.baseline_file)
    new_conflicts = {key: variants for key, variants in conflicts.items() if key not in baseline}

    if not new_conflicts:
        print(f"No new cross-fixture conflicts ({len(conflicts)} pre-existing, baselined).")
        return

    print(
        f"Found {len(new_conflicts)} NEW cross-fixture entity conflict(s) "
        f"(plus {len(conflicts) - len(new_conflicts)} pre-existing, baselined):\n",
        file=sys.stderr,
    )
    cross_file_count = 0
    same_file_count = 0
    for (urn, aspect_name), variants in sorted(new_conflicts.items()):
        distinct_files = {file_label for file_label, _ in variants}
        same_file = len(distinct_files) == 1
        same_file_count += same_file
        cross_file_count += not same_file
        tag = "SAME FILE, duplicate entry" if same_file else "cross-file"
        print(f"  {urn}  [{aspect_name}]  ({tag})", file=sys.stderr)
        for file_label, payload in variants:
            snippet = json.dumps(payload)
            if len(snippet) > 160:
                snippet = snippet[:160] + "..."
            print(f"    {file_label}: {snippet}", file=sys.stderr)
        print(file=sys.stderr)

    print(
        f"{cross_file_count} conflict(s) span two or more fixture files; {same_file_count} are the "
        "same file defining the same entity's aspect twice with different data (almost certainly a "
        "copy-paste bug, not an ordering race).\n\n"
        "Aspect ingestion REPLACES rather than merges, so whichever entry seeds last wins --\n"
        "deterministic between the global seed and a feature seed (feature always wins), but\n"
        "dependent on worker scheduling order between two feature fixtures, and on array\n"
        "order within a single file. Either give the entity a feature-scoped URN, or converge\n"
        "on one definition.\n\n"
        "If a cross-file collision is intentional and already relied upon, add it to the baseline:\n"
        f"  python3 {Path(__file__).name} --write-baseline",
        file=sys.stderr,
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
