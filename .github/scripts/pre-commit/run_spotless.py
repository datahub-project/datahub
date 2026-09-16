"""Run the same matching module-wide Spotless tasks in one Gradle invocation."""

import argparse
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--projects", required=True)
    parser.add_argument("filenames", nargs="*")
    args = parser.parse_args()
    projects = args.projects.split(",")
    # Keep both parent and child tasks: their formatting targets may differ.
    tasks = sorted(
        {
            ":" + project.replace("/", ":") + ":spotlessApply"
            for project in projects
            if any(
                filename.startswith(project + "/") and filename.endswith(".java")
                for filename in args.filenames
            )
        }
    )
    if not tasks:
        return 0
    root = Path(__file__).resolve().parents[3]
    return subprocess.call(
        [str(root / "gradlew"), *tasks, "-x", "generateGitPropertiesGlobal"],
        cwd=root,
    )


if __name__ == "__main__":
    raise SystemExit(main())
