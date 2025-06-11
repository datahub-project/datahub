import pathlib
import re

# HTML standard tag allowlist (for mdx sanitizer)
HTML_TAGS = {
    "html", "head", "title", "base", "link", "meta", "style", "script", "noscript",
    "body", "section", "nav", "article", "aside", "h1", "h2", "h3", "h4", "h5", "h6",
    "header", "footer", "address", "p", "hr", "pre", "blockquote", "ol", "ul", "li",
    "dl", "dt", "dd", "figure", "figcaption", "div", "main", "a", "em", "strong",
    "small", "s", "cite", "q", "dfn", "abbr", "data", "time", "code", "var", "samp",
    "kbd", "sub", "sup", "i", "b", "u", "mark", "ruby", "rt", "rp", "bdi", "bdo",
    "span", "br", "wbr", "ins", "del", "img", "iframe", "embed", "object", "param",
    "video", "audio", "track", "canvas", "map", "area", "svg", "math",
    "table", "caption", "colgroup", "col", "tbody", "thead", "tfoot", "tr", "td", "th",
    "form", "fieldset", "legend", "label", "button", "select", "datalist",
    "optgroup", "option", "textarea", "output", "progress", "meter", "details",
    "summary", "dialog", "template", "slot", "portal"
}

# heuristic string replacements
replacements = [
    ("<function ", "&lt;function "),
    ("<disabled ", "&lt;disabled "),
    ("MDXContent.isMDXComponent = true", ""),
    (".md#", ".mdx#"),
]

def sanitize_mdx_unsafe_tags(content: str) -> str:
    def replacer(match):
        tag = match.group(1)
        if tag.lower() in HTML_TAGS:
            return f"<{tag}>"
        else:
            return f"&lt;{tag}&gt;"
    return re.sub(r"<([a-zA-Z0-9_-]+)>", replacer, content)

def wrap_section_blocks(content: str, level: int, class_name: str) -> str:
    lines = content.splitlines()
    wrapped_lines = []
    inside_block = False

    header_pattern = re.compile(rf"^\s*({'#' * level})\s+(.+)$")
    higher_header_pattern = re.compile(rf"^\s*({'#' * (level - 1)})\s+(.+)$")

    for line in lines:
        if header_pattern.match(line):
            if inside_block:
                wrapped_lines.append("\n\n</div>\n\n")
            wrapped_lines.append(f'<div className="{class_name}">\n\n')
            wrapped_lines.append(line)
            inside_block = True
        elif higher_header_pattern.match(line):
            if inside_block:
                wrapped_lines.append("\n\n</div>\n\n")
                inside_block = False
            wrapped_lines.append(line)
        else:
            wrapped_lines.append(line)

    if inside_block:
        wrapped_lines.append("\n\n</div>")

    return "\n".join(wrapped_lines)

def extract_title_and_remove(content: str, default_title: str = "Untitled") -> (str, str):
    match = re.search(r"^# (.+)$", content, re.MULTILINE)
    if match:
        title = match.group(1).strip()
        content = re.sub(r"^# .+$\n?", "", content, count=1, flags=re.MULTILINE)
    else:
        title = default_title
    return title, content

def convert_file(doc: pathlib.Path, outfile: pathlib.Path, wrap_blocks: bool = True, is_cli: bool = False) -> None:
    with open(doc, "r") as f:
        content = f.read()

    for old, new in replacements:
        content = content.replace(old, new)

    content = sanitize_mdx_unsafe_tags(content)

    if is_cli:
        content = process_cli_content(content)
    elif wrap_blocks:
        content = wrap_section_blocks(content, level=3, class_name="h3-block")

    title, content = extract_title_and_remove(content, default_title=doc.stem)

    frontmatter = f"---\ntitle: {title}\n---\n\n"
    final_content = f'{frontmatter}<div className="sphinx-api-docs">\n\n{content.strip()}\n\n</div>\n\n'

    outfile.parent.mkdir(parents=True, exist_ok=True)
    with open(outfile, "w") as f:
        f.write(final_content)
    print(f"Generated {outfile}")

def process_sphinx_build_dir(SPHINX_BUILD_DIR, DOCS_OUTPUT_DIR):
    DOCS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for doc in SPHINX_BUILD_DIR.glob("**/*.md"):
        wrap_blocks = True
        is_cli = False

        if "cli" in str(doc):
            wrap_blocks = False
            is_cli = True

        outfile = DOCS_OUTPUT_DIR / doc.relative_to(SPHINX_BUILD_DIR).with_suffix(".mdx")
        convert_file(doc, outfile, wrap_blocks=wrap_blocks, is_cli=is_cli)

# CLI content cleaner
def process_cli_content(content: str) -> str:
    lines = content.splitlines()
    processed = []
    for line in lines:
        if line.startswith("### -"):
            # Turn option headers into bold lines
            processed.append(f"* {line[4:].strip()}\n")
        elif re.match(r"^### Options$", line):
            processed.append("#### Options\n")  # demote heading for better hierarchy
        elif re.match(r"^### Arguments$", line):
            processed.append("#### Arguments\n")
        else:
            processed.append(line)
    return "\n".join(processed)

def main():
    SPHINX_ROOT_DIR = pathlib.Path(".")
    build_output_pairs = [
        (SPHINX_ROOT_DIR / pathlib.Path("_build/markdown/apidocs"), pathlib.Path("../docs/python-sdk")),
        (SPHINX_ROOT_DIR / pathlib.Path("_build/markdown/clidocs"), pathlib.Path("../docs/cli-reference")),
    ]

    for SPHINX_BUILD_DIR, DOCS_OUTPUT_DIR in build_output_pairs:
        process_sphinx_build_dir(SPHINX_BUILD_DIR, DOCS_OUTPUT_DIR)

if __name__ == "__main__":
    main()
