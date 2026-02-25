import pathlib
import re

SPHINX_ROOT_DIR = pathlib.Path(".")
SPHINX_BUILD_DIR = SPHINX_ROOT_DIR / "_build/markdown"
DOCS_OUTPUT_DIR = pathlib.Path("../docs/python-sdk")

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

REPLACEMENTS = [
    ("<function ", "&lt;function "),
    ("<disabled ", "&lt;disabled "),
    ("MDXContent.isMDXComponent = true", ""),
    (".md#", ".mdx#"),
]

# ---- CLEAN HTML DANGEROUS TAGS ----
def sanitize_mdx_unsafe_tags(content: str) -> str:
    return re.sub(
        r"<([a-zA-Z0-9_-]+)>",
        lambda m: f"<{m.group(1)}>" if m.group(1).lower() in HTML_TAGS else f"&lt;{m.group(1)}&gt;",
        content
    )

# ---- REPAIR BROKEN MARKDOWN BOLD ----
def repair_broken_emphasis(content: str) -> str:
    content = re.sub(r'\[\*\*([^\*]+)\*\s+\*', r'[**\1**', content)
    content = re.sub(r'\*\*([^\*]+)\*\s+\*\]', r'**\1**]', content)
    content = re.sub(r'\*\*([^\*]+)\*\s*,\s+\*\s*([^\*]+)\s*\*', r'**\1**, **\2**', content)
    content = re.sub(r'\*\s*\*([^\*]+)\*\s+\*\]', r'**\1**]', content)
    return content

# ---- ONLY USED INSIDE SECTION HEADINGS ----
def convert_md_link_to_html(arg_str: str) -> str:
    # convert markdown links inside argument types into plain text fallback
    return re.sub(
        r'\[([^\]]+)\]\([^)]+\)',
        r'<code>\1</code>',
        arg_str
    )


# ---- ARGUMENT PARSER ----
def parse_args(arg_str: str) -> str:
    if not arg_str.strip():
        return ""

    parts = []
    for arg in arg_str.split(","):
        arg = arg.strip().replace("\\", "")
        if arg == "*":
            parts.append("*")
            continue

        for pattern, template in [
            (r"([\w_]+)\s*:\s*([^=]+)\s*=\s*(.+)", r'<span class="arg-name">\1</span>: <span class="arg-type">\2</span> = <span class="arg-default">\3</span>'),
            (r"([\w_]+)\s*=\s*(.+)", r'<span class="arg-name">\1</span> = <span class="arg-default">\2</span>'),
            (r"([\w_]+)\s*:\s*(.+)", r'<span class="arg-name">\1</span>: <span class="arg-type">\2</span>')
        ]:
            m = re.match(pattern, arg)
            if m:
                parts.append(m.expand(template))
                break
        else:
            parts.append(f'<span class="arg-name">{arg}</span>')

    parsed = ", ".join(parts)
    parsed = convert_md_link_to_html(parsed)
    return parsed

# ---- HEADING PARSER ----
def parse_heading(text: str):
    match = re.match(r"(?:\*class\*\s+)?([\w\.]+)\.([\w]+)(?:\((.*)\))?", text)
    if match:
        owner, name, args = match.groups()
        parsed_args = parse_args(args or "")
        prefix = '<span class="class-text">class</span> ' if "*class*" in text else ""
        heading = f'{prefix}<span class="class-owner">{owner}.</span><span class="class-name">{name}</span>'
        heading += f"({parsed_args})" if parsed_args else "()"
        slug = f"{owner}.{name}"
        return name, heading, slug

    match = re.match(r"([\w]+)(?:\((.*)\))?", text)
    if match:
        name, args = match.groups()
        parsed_args = parse_args(args or "")
        heading = f'<span class="class-name">{name}</span>'
        heading += f"({parsed_args})" if parsed_args else "()"
        return name, heading, name

    return text, text, text

# ---- SECTION WRAPPER ----
def wrap_section_blocks(content: str, class_name: str) -> str:
    lines = content.splitlines()
    out = []
    inside = False

    for line in lines:
        m = re.match(r"^### (.+)$", line)
        if m:
            if inside:
                out.append("\n\n</div>\n\n")

            name, heading, slug = parse_heading(m.group(1))
            out.append(f'\n\n### <span className="visually-hidden">{name}</span> {{#{slug}}}\n\n')
            out.append(f'<div className="{class_name}">\n')
            out.append(f'<div className="section-heading">{heading}<a href="#{slug}" class="hash-link"></a></div>\n')
            inside = True
        else:
            out.append(line)

    if inside:
        out.append("\n\n</div>\n\n")

    return "\n".join(out)

# ---- PARAMETER DASH FIX ----
def fix_parameter_dash(content: str) -> str:
    return re.sub(r'(\*\s+\*\*[\w]+?\*\*\s+\([^\)]*\))\s+–\s*(?=\n|\r|\Z)', r'\1', content)

# ---- FILE CONVERTER ----
def convert_file(doc: pathlib.Path, outfile: pathlib.Path):
    content = doc.read_text()

    for old, new in REPLACEMENTS:
        content = content.replace(old, new)

    content = sanitize_mdx_unsafe_tags(content)
    content = repair_broken_emphasis(content)
    content = wrap_section_blocks(content, "h3-block")
    content = fix_parameter_dash(content)

    title_match = re.search(r"^# (.+)$", content, re.MULTILINE)
    title = title_match.group(1).strip() if title_match else doc.stem
    content = re.sub(r"^# .+\n?", "", content, count=1, flags=re.MULTILINE)

    final = f"---\ntitle: {title}\n---\n<div className=\"sphinx-api-docs\">\n{content.strip()}\n</div>\n"

    outfile.parent.mkdir(parents=True, exist_ok=True)
    outfile.write_text(final)
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
