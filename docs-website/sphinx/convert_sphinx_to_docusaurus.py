import pathlib

SPHINX_ROOT_DIR = pathlib.Path(".")
SPHINX_BUILD_DIR = SPHINX_ROOT_DIR / pathlib.Path("_build/mdx/apidocs")
DOCS_OUTPUT_DIR = pathlib.Path("../docs/python-sdk")


def main():
    DOCS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    replacements = [
        ("<function ", "<\\function "),
        ("<type>", "<\\type> "),
        ("<id>", "<\\id> "),
        ("<id1>", "<\\id1> "),
        ("<id2>", "<\\id2> "),
    ]

    for doc in SPHINX_BUILD_DIR.glob("**/*.mdx"):
        outfile = DOCS_OUTPUT_DIR / doc.relative_to(SPHINX_BUILD_DIR)
        outfile.parent.mkdir(parents=True, exist_ok=True)

        with open(doc, "r") as f:
            content = f.read()

        for old, new in replacements:
            content = content.replace(old, new)

        with open(outfile, "w") as f:
            f.write(content)

        print(f"Generated {outfile}")

if __name__ == "__main__":
    main()
