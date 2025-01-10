"""Generate pre-commit hooks for Java and Python projects.

This script scans a repository for Java and Python projects and generates appropriate
pre-commit hooks for linting and formatting. It also merges in additional hooks from
an override file.
"""

import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
import datetime

import yaml


class ProjectType(Enum):
    """Types of projects supported for hook generation."""

    JAVA = auto()
    PYTHON = auto()


@dataclass
class Project:
    """Represents a project found in the repository."""

    path: str
    type: ProjectType

    @property
    def gradle_path(self) -> str:
        """Convert path to Gradle task format."""
        return ":" + self.path.replace("/", ":")

    @property
    def project_id(self) -> str:
        """Generate a unique identifier for the project."""
        return self.path.replace("/", "-").replace(".", "-")


class ProjectFinder:
    """Find Java and Python projects in a repository."""

    JAVA_PATTERNS = [
        "plugins.hasPlugin('java')",
        "apply plugin: 'java'",
        "id 'java'",
        "id 'java-library'",
        "plugins.hasPlugin('java-library')",
        "apply plugin: 'java-library'",
        "plugins.hasPlugin('pegasus')",
        "org.springframework.boot",
    ]

    EXCLUDED_DIRS = {".git", "build", "node_modules", ".tox", "venv"}
    SOURCE_EXTENSIONS = {".java", ".kt", ".groovy"}

    def __init__(self, root_dir: str):
        self.root_path = Path(root_dir)

    def find_all_projects(self) -> list[Project]:
        """Find all Java and Python projects in the repository."""
        java_projects = self._find_java_projects()
        python_projects = self._find_python_projects()

        all_projects = []
        all_projects.extend(
            Project(path=p, type=ProjectType.JAVA) for p in java_projects
        )
        all_projects.extend(
            Project(path=p, type=ProjectType.PYTHON) for p in python_projects
        )

        return sorted(all_projects, key=lambda p: p.path)

    def _find_java_projects(self) -> set[str]:
        """Find all Java projects by checking build.gradle files."""
        java_projects = set()

        # Search both build.gradle and build.gradle.kts
        for pattern in ["build.gradle", "build.gradle.kts"]:
            for gradle_file in self.root_path.rglob(pattern):
                if self._should_skip_directory(gradle_file.parent):
                    continue

                if self._is_java_project(gradle_file):
                    java_projects.add(self._get_relative_path(gradle_file.parent))

        return {
            p
            for p in java_projects
            if "buildSrc" not in p and "spark-smoke-test" not in p and p != "."
        }

    def _find_python_projects(self) -> set[str]:
        """Find all Python projects by checking for setup.py or pyproject.toml."""
        python_projects = set()

        for file_name in ["setup.py", "pyproject.toml"]:
            for path in self.root_path.rglob(file_name):
                if self._should_skip_directory(path.parent):
                    continue

                rel_path = self._get_relative_path(path.parent)
                if "examples" not in rel_path:
                    python_projects.add(rel_path)

        return python_projects

    def _should_skip_directory(self, path: Path) -> bool:
        """Check if directory should be skipped."""
        return any(
            part in self.EXCLUDED_DIRS or part.startswith(".") for part in path.parts
        )

    def _is_java_project(self, gradle_file: Path) -> bool:
        """Check if a Gradle file represents a Java project."""
        try:
            content = gradle_file.read_text()
            has_java_plugin = any(pattern in content for pattern in self.JAVA_PATTERNS)

            if has_java_plugin:
                # Verify presence of source files
                return any(
                    list(gradle_file.parent.rglob(f"*{ext}"))
                    for ext in self.SOURCE_EXTENSIONS
                )
            return False

        except Exception as e:
            print(f"Warning: Error reading {gradle_file}: {e}")
            return False

    def _get_relative_path(self, path: Path) -> str:
        """Get relative path from root, normalized with forward slashes."""
        return str(path.relative_to(self.root_path)).replace("\\", "/")


class HookGenerator:
    """Generate pre-commit hooks for projects."""

    def __init__(self, projects: list[Project], override_file: str = None):
        self.projects = projects
        self.override_file = override_file

    def generate_config(self) -> dict:
        """Generate the complete pre-commit config."""
        hooks = []

        for project in self.projects:
            if project.type == ProjectType.PYTHON:
                hooks.append(self._generate_lint_fix_hook(project))
            else:  # ProjectType.JAVA
                hooks.append(self._generate_spotless_hook(project))

        config = {"repos": [{"repo": "local", "hooks": hooks}]}
        
        # Merge override hooks if they exist
        if self.override_file and os.path.exists(self.override_file):
            try:
                with open(self.override_file, 'r') as f:
                    override_config = yaml.safe_load(f)
                
                if override_config and 'repos' in override_config:
                    for override_repo in override_config['repos']:
                        matching_repo = next(
                            (repo for repo in config['repos'] 
                             if repo['repo'] == override_repo['repo']),
                            None
                        )
                        
                        if matching_repo:
                            matching_repo['hooks'].extend(override_repo.get('hooks', []))
                        else:
                            config['repos'].append(override_repo)
                
                print(f"Merged additional hooks from {self.override_file}")
            except Exception as e:
                print(f"Warning: Error reading override file {self.override_file}: {e}")

        return config

    def _generate_lint_fix_hook(self, project: Project) -> dict:
        """Generate a lint-fix hook for Python projects."""
        return {
            "id": f"{project.project_id}-lint-fix",
            "name": f"{project.path} Lint Fix",
            "entry": f"./gradlew {project.gradle_path}:lintFix",
            "language": "system",
            "files": f"^{project.path}/.*\\.py$",
            "pass_filenames": False,
        }

    def _generate_spotless_hook(self, project: Project) -> dict:
        """Generate a spotless hook for Java projects."""
        return {
            "id": f"{project.project_id}-spotless",
            "name": f"{project.path} Spotless Apply",
            "entry": f"./gradlew {project.gradle_path}:spotlessApply",
            "language": "system",
            "files": f"^{project.path}/.*\\.java$",
            "pass_filenames": False,
        }


class PrecommitDumper(yaml.Dumper):
    """Custom YAML dumper that maintains proper indentation."""

    def increase_indent(self, flow=False, *args, **kwargs):
        return super().increase_indent(flow=flow, indentless=False)


def write_yaml_with_spaces(file_path: str, data: dict):
    """Write YAML file with extra spacing between hooks and a timestamp header."""
    with open(file_path, "w") as f:
        # Add timestamp header
        current_time = datetime.datetime.now(datetime.timezone.utc)
        formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        header = f"# Auto-generated by .github/scripts/generate_pre_commit.py at {formatted_time}\n"
        f.write(header)
        header = f"# Do not edit this file directly. Run the script to regenerate.\n"
        f.write(header)
        header = f"# Add additional hooks in .github/scripts/pre-commit-override.yaml\n"
        f.write(header)

        # Write the YAML content
        yaml_str = yaml.dump(
            data, Dumper=PrecommitDumper, sort_keys=False, default_flow_style=False
        )

        # Add extra newline between hooks
        lines = yaml_str.split("\n")
        result = []
        in_hook = False

        for line in lines:
            if line.strip().startswith("- id:"):
                if in_hook:  # If we were already in a hook, add extra newline
                    result.append("")
                in_hook = True
            elif not line.strip() and in_hook:
                in_hook = False

            result.append(line)

        f.write("\n".join(result))


def main():
    root_dir = os.path.abspath(os.curdir)
    override_file = ".github/scripts/pre-commit-override.yaml"

    # Find projects
    finder = ProjectFinder(root_dir)
    projects = finder.find_all_projects()

    # Print summary
    print("Found projects:")
    print("\nJava projects:")
    for project in projects:
        if project.type == ProjectType.JAVA:
            print(f"  - {project.path}")

    print("\nPython projects:")
    for project in projects:
        if project.type == ProjectType.PYTHON:
            print(f"  - {project.path}")

    # Generate and write config
    generator = HookGenerator(projects, override_file)
    config = generator.generate_config()
    write_yaml_with_spaces(".pre-commit-config.yaml", config)

    print("\nGenerated .pre-commit-config.yaml")


if __name__ == "__main__":
    main()