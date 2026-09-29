"""Run the same matching module-wide Spotless tasks in one Gradle invocation."""

import argparse
import subprocess
from pathlib import Path


def _matching_projects(projects: list[str], filenames: list[str]) -> list[str]:
    return sorted(
        {
            project
            for project in projects
            if any(
                filename.startswith(project + "/") and filename.endswith(".java")
                for filename in filenames
            )
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--projects", required=True)
    parser.add_argument("filenames", nargs="*")
    args = parser.parse_args()
    projects = args.projects.split(",")
    matching_projects = _matching_projects(projects, args.filenames)
    if not matching_projects:
        return 0

    root = Path(__file__).resolve().parents[3]
    tasks = [
        ":" + project.replace("/", ":") + ":spotlessApply"
        for project in matching_projects
    ]
    return subprocess.call(
        [str(root / "gradlew"), *tasks, "-x", "generateGitPropertiesGlobal"],
        cwd=root,
    )


if __name__ == "__main__":
    raise SystemExit(main())
