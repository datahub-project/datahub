import re
from pathlib import Path

_repo_root = Path(__file__).parent.parent
assert (_repo_root / ".git").exists(), "Unable to find git repo root"


def _load_file(path: str, context_dir: Path) -> str:
    if path.startswith("@/"):
        resolved_path = Path(_repo_root / path[2:])
        return resolved_path.read_text()
    else:
        raise ValueError(
            f"Only repo-rooted paths, which have the '@/' prefix, are supported: got {path}"
        )


def update_template(template_file: Path) -> None:
    """
    Update a template file in-place, injecting content from files referenced in inline directives.

    Args:
        template_file: Path to the template file that will be modified
    """
    content = template_file.read_text()

    def handle_multiline(match: re.Match) -> str:
        path = match.group(2)
        replacement = _load_file(path, template_file.parent).strip()
        replacement = replacement.strip() + "\n"
        return f"{match.group(1)}{replacement}{match.group(3)}"

    # Handle multiline inline directives
    content = re.sub(
        r"(# INLINE-BEGIN (.*?))\n.*?\n([ \t]*# INLINE-END \2)",
        handle_multiline,
        content,
        flags=re.DOTALL,
    )

    template_file.write_text(content)


if __name__ == "__main__":
    files = [
        "docker/datahub-ingestion-base/Dockerfile",
        "docker/datahub-ingestion/Dockerfile",
    ]
    for file in files:
        update_template(Path(_repo_root / file))
