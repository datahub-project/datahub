import re
import sys
from pathlib import Path
from typing import Optional

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


def update_template(
    template_file: Path,
    outfile: Optional[Path] = None,
    check_only: bool = False,
) -> None:
    """
    Update a template file in-place, injecting content from files referenced in inline directives.

    Args:
        template_file: Path to the template file that will be modified
    """

    render_mode = bool(outfile)

    subs = 0
    content = template_file.read_text()

    def handle_multiline(match: re.Match) -> str:
        nonlocal subs
        subs += 1

        path = match.group(2)
        replacement = _load_file(path, template_file.parent).strip()
        replacement = replacement.strip() + "\n"

        if render_mode:
            return f"{replacement}"
        else:
            return f"{match.group(1)}{replacement}{match.group(3)}"

    # Handle multiline inline directives
    content = re.sub(
        r"^([ \t]*# INLINE-BEGIN (.*?)\n).*?^([ \t]*# INLINE-END)$",
        handle_multiline,
        content,
        flags=re.DOTALL | re.MULTILINE,
    )

    # if subs == 0:
    #     raise ValueError(f"No templates found in {template_file}")

    output = outfile or template_file
    if check_only:
        if output.read_text() != content:
            print(f"ERROR: {template_file} is out of date")
            sys.exit(1)
    else:
        print(f"Applied {subs} substitutions while processing {template_file}")
        output.write_text(content)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--check":
        check_only = True
    else:
        check_only = False

    update_template(
        Path(_repo_root / "docker/snippets/ingestion_base.template"),
        outfile=Path(_repo_root / "docker/snippets/ingestion_base"),
        check_only=check_only,
    )

    for file in [
        "docker/datahub-ingestion-base/Dockerfile",
        "docker/datahub-ingestion/Dockerfile",
    ]:
        update_template(Path(_repo_root / file), check_only=check_only)
