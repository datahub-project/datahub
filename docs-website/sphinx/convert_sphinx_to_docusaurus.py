import pathlib
import re

SPHINX_ROOT_DIR = pathlib.Path(".")
SPHINX_BUILD_DIR = SPHINX_ROOT_DIR / pathlib.Path("_build/markdown")
DOCS_OUTPUT_DIR = pathlib.Path("../docs/python-sdk")

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

# docusaurus-style slug generator
def slugify(text: str) -> str:
    slug = text.lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug)
    return slug

def generate_anchor(slug: str) -> str:
    return f'<a href="#{slug}" class="hash-link" aria-label="Direct link to {slug}" title="Direct link to {slug}"></a>'

# argument parsing
def parse_args(arg_str: str) -> str:
    args = []
    for arg in arg_str.split(","):
        arg = arg.strip()
        if not arg:
            continue

        match = re.match(r"([\w]+)\s*:\s*([^=]+)\s*=\s*(.+)", arg)
        if match:
            name, typ, default = match.groups()
            args.append(f'<span class="arg-name">{name}</span>: <span class="arg-type">{typ}</span> = <span class="arg-default">{default}</span>')
            continue

        match = re.match(r"([\w]+)\s*=\s*(.+)", arg)
        if match:
            name, default = match.groups()
            args.append(f'<span class="arg-name">{name}</span> = <span class="arg-default">{default}</span>')
            continue

        match = re.match(r"([\w]+)\s*:\s*(.+)", arg)
        if match:
            name, typ = match.groups()
            args.append(f'<span class="arg-name">{name}</span>: <span class="arg-type">{typ}</span>')
            continue

        args.append(f'<span class="arg-name">{arg}</span>')

    return ", ".join(args)

# universal heading parser
def trim_heading_text(text: str) -> (str, str):
    sidebar_text = text

    # Match both class & function form universally
    match = re.match(r"(?:\*class\*\s+)?([\w\.]+)\.([\w]+)\((.*)\)", text)
    if match:
        owner, name, arg_str = match.groups()
        sidebar_text = name
        parsed_args = parse_args(arg_str)

        styled_text = ""
        if "*class*" in text:
            styled_text += '<span class="class-text">class</span> '

        styled_text += (
            f'<span class="class-owner">{owner}.</span>'
            f'<span class="class-name">{name}</span>({parsed_args})'
        )
        return sidebar_text, styled_text

    # fallback — no owner
    match = re.match(r"([\w]+)\((.*)\)", text)
    if match:
        name, arg_str = match.groups()
        sidebar_text = name
        parsed_args = parse_args(arg_str)
        styled_text = f'<span class="class-name">{name}</span>({parsed_args})'
        return sidebar_text, styled_text

    return sidebar_text, text

def wrap_section_blocks(content: str, level: int, class_name: str) -> str:
    lines = content.splitlines()
    wrapped_lines = []
    inside_block = False

    header_pattern = re.compile(rf"^\s*({'#' * level})\s+(.+)$")
    higher_header_pattern = re.compile(rf"^\s*({'#' * (level - 1)})\s+(.+)$")

    for line in lines:
        match = header_pattern.match(line)
        if match:
            if inside_block:
                wrapped_lines.append("\n\n</div>\n\n")

            heading_text = match.group(2)
            sidebar_text, styled_heading_text = trim_heading_text(heading_text)
            slug = slugify(sidebar_text)

            wrapped_lines.append(f'### <span className="visually-hidden">{sidebar_text}</span> {{#{slug}}}\n')
            wrapped_lines.append(f'<div className="{class_name}">\n')
            wrapped_lines.append(f'<div className="section-heading">{styled_heading_text}{generate_anchor(slug)}</div>\n')
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

def convert_file(doc: pathlib.Path, outfile: pathlib.Path) -> None:
    with open(doc, "r") as f:
        content = f.read()

    for old, new in replacements:
        content = content.replace(old, new)

    content = sanitize_mdx_unsafe_tags(content)
    content = wrap_section_blocks(content, level=3, class_name="h3-block")

    title, content = extract_title_and_remove(content, default_title=doc.stem)

    frontmatter = f"---\ntitle: {title}\n---\n\n"
    final_content = f'{frontmatter}<div className="sphinx-api-docs">\n\n{content.strip()}\n\n</div>\n\n'

    outfile.parent.mkdir(parents=True, exist_ok=True)
    with open(outfile, "w") as f:
        f.write(final_content)
    print(f"Generated {outfile}")

def main():
    DOCS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for doc in SPHINX_BUILD_DIR.glob("**/*.md"):
        if doc.stem == "index":
            continue
        outfile = DOCS_OUTPUT_DIR / doc.relative_to(SPHINX_BUILD_DIR / "apidocs").with_suffix(".mdx")
        convert_file(doc, outfile)

if __name__ == "__main__":
    main()
