"""Export GMS's GraphQL schema at each pinned version, for the snapshot tests.

Reads tests/graphql_schemas.json and writes build/graphql-schemas/<name>.graphql:
the concatenated SDL GMS serves at that commit, or in the checkout for entries
without a commit. Commits missing from a shallow clone are fetched by tag.
"""

import json
import pathlib
import subprocess
import sys

PACKAGE_DIR = pathlib.Path(__file__).resolve().parent.parent
REPO_ROOT = PACKAGE_DIR.parent.parent
SCHEMA_DIR = "datahub-graphql-core/src/main/resources"
OUT_DIR = PACKAGE_DIR / "build" / "graphql-schemas"


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, check=True, capture_output=True, text=True
    ).stdout


def has_commit(commit: str) -> bool:
    result = subprocess.run(
        ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
        cwd=REPO_ROOT,
        capture_output=True,
    )
    return result.returncode == 0


def sdl_at(commit: str, tag: str) -> str:
    if not has_commit(commit):
        git("fetch", "--quiet", "--depth=1", "origin", "tag", tag)
    tagged = git("rev-parse", f"{tag}^{{commit}}").strip()
    if tagged != commit:
        raise SystemExit(f"{tag} is {tagged}, but the pinned commit is {commit}")
    files = [
        f
        for f in git("ls-tree", "-r", "--name-only", commit, SCHEMA_DIR).split()
        if f.endswith(".graphql")
    ]
    return "\n".join(git("show", f"{commit}:{f}") for f in sorted(files))


def sdl_in_checkout() -> str:
    files = sorted((REPO_ROOT / SCHEMA_DIR).rglob("*.graphql"))
    return "\n".join(f.read_text(encoding="utf-8") for f in files)


def main() -> None:
    pins = json.loads((PACKAGE_DIR / "tests" / "graphql_schemas.json").read_text())
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for entry in pins["schemas"]:
        sdl = (
            sdl_at(entry["commit"], entry["tag"])
            if entry["commit"]
            else sdl_in_checkout()
        )
        (OUT_DIR / f"{entry['name']}.graphql").write_text(sdl, encoding="utf-8")
        print(f"{entry['name']}: {len(sdl) // 1024} KiB", file=sys.stderr)


if __name__ == "__main__":
    main()
