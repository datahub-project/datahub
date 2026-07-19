"""Generate pre-commit hooks for Java and Python projects.

This script scans a repository for Java and Python projects and generates appropriate
pre-commit hooks for linting and formatting. It also merges in additional hooks from
an override file.
"""

import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

import yaml


class ProjectType(Enum):
    """Types of projects supported for hook generation."""

    JAVA = auto()
    PYTHON = auto()
    PRETTIER = auto()


@dataclass
class Project:
    """Represents a project found in the repository."""

    path: str
    type: ProjectType
    taskName: str | None = None  # Used for prettier projects
    filePattern: str | None = None  # Used for prettier projects

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
        # Python module dirs that have a build.gradle (and thus a Gradle-managed
        # venv). These are the dirs the collapsed repo-wide ruff hooks cover.
        # Non-module .py (e.g. .github/scripts/, docker/, python-build/) is
        # intentionally NOT covered — it had no ruff coverage before and would
        # fail under ruff's default rules.
        ruff_module_dirs: list[str] = []
        # Subset of ruff_module_dirs whose build.gradle lint task runs mypy.
        # These are the dirs the single repo-wide mypy hook covers.
        mypy_module_dirs: list[str] = []

        for project in self.projects:
            if project.type == ProjectType.PYTHON:
                # The mypy hook runs each module's own pinned mypy from its
                # Gradle-managed venv (<module>/venv/bin/mypy), which needs the
                # module's deps installed. Only emit it for modules that actually
                # have a build.gradle (and thus an installDev task that builds
                # that venv); modules without one have no module mypy to use, so
                # they get no mypy coverage. (ruff is repo-wide and needs no
                # module venv — it is emitted once below.)
                if not os.path.exists(os.path.join(project.path, "build.gradle")):
                    print(
                        f"Skipping mypy hook for {project.path}: "
                        "no build.gradle (no Gradle-managed venv / module mypy)"
                    )
                    continue
                ruff_module_dirs.append(project.path)
                if self._module_has_mypy(project):
                    mypy_module_dirs.append(project.path)
            elif project.type == ProjectType.JAVA:
                hooks.append(self._generate_spotless_hook(project))
            elif project.type == ProjectType.PRETTIER:
                hooks.append(self._generate_prettier_hook(project))
            else:
                print(f"Warning: Unsupported project type {project.type} for {project.path}")

        # Collapse all per-module ruff hooks into two repo-wide hooks (one check,
        # one format) using the official ruff Docker image (pinned to the latest
        # version pinned across modules — metadata-ingestion pins 0.15.18, the
        # rest 0.11.7). Using the Docker image (language: docker_image) avoids
        # needing any system/venv ruff install and gives a reproducible version
        # across all machines. ruff does per-file config discovery, so each
        # module's own [tool.ruff] in its pyproject.toml still applies (the repo
        # is mounted into the container at the same path). ruff never imports
        # the code or its deps, so no module venv is required. The `files` regex
        # is a union of the module dirs so non-module .py (which has no ruff
        # config) is not newly linted.
        if ruff_module_dirs:
            dirs_alt = "|".join(d.replace(".", r"\.") for d in ruff_module_dirs)
            # Match .py plus pyproject.toml/ruff.toml in these modules: ruff config
            # lives in those toml files, so editing them can change lint/format
            # results and must retrigger the hook (not just .py edits).
            files_regex = (
                f"^({dirs_alt})/(?:.*\\.py$|(?:.*/)?(?:pyproject|ruff)\\.toml$)"
            )
            exclude_re = r"(^|/)(venv|build|dist|node_modules|\.git)/"
            ruff_image = "ghcr.io/astral-sh/ruff:0.15.22"
            ruff_hooks = [
                {
                    "id": "ruff-check",
                    "name": "Ruff Check (all Python modules)",
                    # ruff check --fix applies safe lint fixes (can change code);
                    # keep it at pre-commit so issues are caught before commit.
                    "entry": f"{ruff_image} check --fix",
                    "language": "docker_image",
                    "files": files_regex,
                    "exclude": exclude_re,
                    "pass_filenames": True,
                    "stages": ["pre-commit"],
                },
                {
                    "id": "ruff-format",
                    "name": "Ruff Format (all Python modules)",
                    # ruff format is a pure formatter (no meaning change); pre-push.
                    "entry": f"{ruff_image} format",
                    "language": "docker_image",
                    "files": files_regex,
                    "exclude": exclude_re,
                    "pass_filenames": True,
                    "stages": ["pre-push"],
                },
            ]
            # Generated hooks are emitted before override hooks; prepend ruff so
            # ruff check runs first among the generated python hooks.
            hooks = ruff_hooks + hooks

        # Collapse all per-module mypy hooks into one repo-wide hook (pre-push).
        # mypy uses single-config (CWD-based) discovery and resolves imports
        # against the module's installed deps, so it cannot be a single process
        # across modules — the wrapper groups staged files by module and invokes
        # each module's own <module>/venv/bin/mypy from the module dir (so each
        # module's mypy config still applies). The `files` regex is a union of
        # the mypy module dirs so non-module .py is not type-checked. mypy is
        # slow, so it stays at pre-push; CI still runs the full module-wide mypy
        # via `./gradlew :<module>:lint`. See PFP-5002.
        if mypy_module_dirs:
            m_dirs_alt = "|".join(d.replace(".", r"\.") for d in mypy_module_dirs)
            m_files_regex = f"^({m_dirs_alt})/.*\\.py$"
            modules_arg = ",".join(mypy_module_dirs)
            mypy_hook = {
                "id": "mypy",
                "name": "Mypy (all Python modules)",
                "entry": f"bash .github/scripts/pre-commit/python_staged_mypy.sh --modules {modules_arg}",
                "language": "system",
                "files": m_files_regex,
                "exclude": r"(^|/)(venv|build|dist|node_modules|\.git)/",
                "pass_filenames": True,
                "stages": ["pre-push"],
            }
            hooks = hooks + [mypy_hook]

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

    def _module_has_mypy(self, project: Project) -> bool:
        """Return True if the module's build.gradle runs mypy in a lint task.

        Detects the ``"mypy`` token (a double-quoted mypy command string in the
        Gradle Exec commandLine), which is present in lint/pythonLint tasks that
        invoke mypy and absent from modules that only run ruff.
        """
        gradle_file = os.path.join(project.path, "build.gradle")
        try:
            with open(gradle_file) as f:
                return '"mypy' in f.read()
        except OSError:
            return False

    def _generate_spotless_hook(self, project: Project) -> dict:
        """Generate a spotless hook for Java projects."""
        return {
            "id": f"{project.project_id}-spotless",
            "name": f"{project.path} Spotless Apply",
            "entry": f"./gradlew {project.gradle_path}:spotlessApply -x generateGitPropertiesGlobal",
            "language": "system",
            "files": f"^{project.path}/.*\\.java$",
            "pass_filenames": False,
            # spotlessApply is a self-correcting formatter (slow, module-wide Gradle
            # invocation). Run it at pre-push instead of pre-commit so commits stay
            # fast; see PFP-5002.
            "stages": ["pre-push"],
        }

    def _generate_prettier_hook(self, project: Project) -> dict:
        """Generate a prettier hook for projects."""
        return {
            "id": f"{project.project_id}-{project.taskName}",
            "name": f"{project.taskName}",
            "entry": f"./gradlew {project.gradle_path}:{project.taskName} -x generateGitPropertiesGlobal",
            "language": "system",
            "files": project.filePattern,
            "pass_filenames": False,
            # PrettierWrite is a self-correcting formatter (slow, module-wide Gradle
            # invocation). Run it at pre-push instead of pre-commit so commits stay
            # fast; see PFP-5002.
            "stages": ["pre-push"],
        }


class PrecommitDumper(yaml.Dumper):
    """Custom YAML dumper that maintains proper indentation."""

    def increase_indent(self, flow=False, *args, **kwargs):
        return super().increase_indent(flow=flow, indentless=False)


def write_yaml_with_spaces(file_path: str, data: dict):
    """Write YAML file with extra spacing between hooks and a timestamp header."""
    with open(file_path, "w") as f:
        # Add timestamp header
        header = "# Auto-generated by .github/scripts/pre-commit/generate_pre_commit.py\n"
        f.write(header)
        header = "# Do not edit this file directly. Run the script to regenerate.\n"
        f.write(header)
        header = "# Add additional hooks in .github/scripts/pre-commit/pre-commit-override.yaml\n"
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
    override_file = ".github/scripts/pre-commit/pre-commit-override.yaml"

    # Find projects
    finder = ProjectFinder(root_dir)
    prettier_projects = [
        Project(
            path="datahub-web-react",
            type=ProjectType.PRETTIER,
            taskName="mdPrettierWriteChanged",
            filePattern="^.*\\.md$",
        ),
        Project(
            path="datahub-web-react",
            type=ProjectType.PRETTIER,
            taskName="githubActionsPrettierWriteChanged",
            filePattern="^\\.github/.*\\.(yml|yaml)$"
        ),
    ]
    projects = [*prettier_projects, *finder.find_all_projects()]

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
