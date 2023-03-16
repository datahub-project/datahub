import pathlib
import json
from bs4 import BeautifulSoup


SPHINX_ROOT_DIR = pathlib.Path(".")
SPHINX_BUILD_DIR = SPHINX_ROOT_DIR / pathlib.Path("_build/html/_apidocs")
DOCS_OUTPUT_DIR = pathlib.Path("../docs/python-sdk")


def html_to_mdx(html: str) -> str:
    # Because the HTML uses `class` and has `{}` in it, it isn't valid
    # MDX. As such, we use React's dangerouslySetInnerHTML.
    return f"""

<div dangerouslySetInnerHTML={{{{__html: {json.dumps(html)}}}}}></div>

"""


def convert_html_to_md(html_file: pathlib.Path) -> str:
    html = html_file.read_text()
    soup = BeautifulSoup(html, "html.parser")

    body = soup.find("main").find("div", {"class": "bd-article-container"})
    article = body.find("article")

    # Remove all the "permalink to this heading" links.
    for link in article.find_all("a", {"class": "headerlink"}):
        link.decompose()

    # Extract title from the h1.
    title_element = article.find("h1")
    title = title_element.text
    title_element.decompose()

    # TODO - generate nicer slugs for these pages
    md_meta = f"""---
title: {title}
---\n\n"""

    styles = """
import { useMemo } from 'react';
import './basic.css';

\n"""

    return md_meta + styles + html_to_mdx(str(article))


def main():
    DOCS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for doc in SPHINX_BUILD_DIR.glob("**/*.html"):
        md = convert_html_to_md(doc)

        outfile = DOCS_OUTPUT_DIR / doc.relative_to(SPHINX_BUILD_DIR).with_suffix(".md")
        outfile.parent.mkdir(parents=True, exist_ok=True)
        outfile.write_text(md)

        print(f"Generated {outfile}")

    # css_files = [
    #     (SPHINX_BUILD_DIR.parent / "_static/basic.css"),
    #     (SPHINX_BUILD_DIR.parent / "_static/file.png"),
    #     (SPHINX_BUILD_DIR.parent / "_static/styles/pydata-sphinx-theme.css"),
    # ]
    # for css in css_files:
    #     outfile = DOCS_OUTPUT_DIR.joinpath(css.name)
    #     outfile.write_bytes(css.read_bytes())
    #     print(f"Copied {css} to {outfile}")

    css = SPHINX_ROOT_DIR / "basic.css"
    outfile = DOCS_OUTPUT_DIR.joinpath(css.name)
    outfile.write_bytes(css.read_bytes())
    print(f"Copied {css} to {outfile}")


if __name__ == "__main__":
    main()
