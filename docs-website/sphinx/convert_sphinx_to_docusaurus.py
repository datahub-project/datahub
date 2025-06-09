import pathlib

SPHINX_ROOT_DIR = pathlib.Path(".")
SPHINX_BUILD_DIR = SPHINX_ROOT_DIR / pathlib.Path("_build/markdown/apidocs")
DOCS_OUTPUT_DIR = pathlib.Path("../docs/python-sdk")


def main():
    DOCS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    replacements = [
            ("<function ", "<\\function "),
            ("<id>", "<\\id>"),
            ("<type>", "<\\type>"),
            ("<id1>", "<\\id1>"),
            ("<id2>", "<\\id2>"),
            ("MDXContent.isMDXComponent = true", ""),
        ]
    
    for doc in SPHINX_BUILD_DIR.glob("**/*.md"):
        outfile = DOCS_OUTPUT_DIR / doc.relative_to(SPHINX_BUILD_DIR)
        outfile.parent.mkdir(parents=True, exist_ok=True)

        with open(doc, "r") as f:
            content = f.read()

        for old, new in replacements:
            content = content.replace(old, new)

        # Wrap the entire content with div (top and bottom)
        final_content = f"<div className=\"python-sdk\">\n\n{content.strip()}\n\n</div>\n"

        with open(outfile, "w") as f:
            f.write(final_content)

        print(f"Generated {outfile}")

if __name__ == "__main__":
    main()
