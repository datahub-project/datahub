import pathlib
import re

SPHINX_ROOT_DIR = pathlib.Path(".")
SPHINX_BUILD_DIR = SPHINX_ROOT_DIR / pathlib.Path("_build/markdown/apidocs")
DOCS_OUTPUT_DIR = pathlib.Path("../docs/python-sdk")


def wrap_section_blocks(content: str, level: int, class_name: str) -> str:
    """Wraps markdown header sections with a custom div class."""
    header_pattern = re.compile(rf"^({'#' * level}) (.+)$", re.MULTILINE)
    blocks = []
    last_index = 0

    matches = list(header_pattern.finditer(content))

    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        section = content[start:end].strip()
        wrapped = f'<div className="{class_name}">\n\n{section}\n\n</div>'
        blocks.append(content[last_index:start])
        blocks.append(wrapped)
        last_index = end

    blocks.append(content[last_index:])
    return "".join(blocks)


def main():
    DOCS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    replacements = [
        ("<function ", "<\\function "),
        ("<id>", "<\\id>"),
        ("<type>", "<\\type>"),
        ("<id1>", "<\\id1>"),
        ("<id2>", "<\\id2>"),
        ("MDXContent.isMDXComponent = true", ""),
        (".md#", ".mdx#"),  # fix broken links
    ]

    for doc in SPHINX_BUILD_DIR.glob("**/*.md"):
        outfile = DOCS_OUTPUT_DIR / doc.relative_to(SPHINX_BUILD_DIR).with_suffix(".mdx")
        outfile.parent.mkdir(parents=True, exist_ok=True)

        with open(doc, "r") as f:
            content = f.read()

        for old, new in replacements:
            content = content.replace(old, new)

        # Step 1: wrap h3 sections
        content = wrap_section_blocks(content, level=3, class_name="h3-block")

        # Step 3: wrap the entire content
        final_content = f"<div className=\"python-sdk\">\n\n{content.strip()}\n\n</div>\n\n"

        with open(outfile, "w") as f:
            f.write(final_content)

        print(f"Generated {outfile}")

if __name__ == "__main__":
    main()
