import pathlib
import json
from bs4 import BeautifulSoup


SPHINX_BUILD_DIR = pathlib.Path("_build/html/_apidocs")
DOCS_OUTPUT_DIR = pathlib.Path("../docs/python-sdk")


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

    # Because the HTML uses `class` and has `{}` in it, it isn't valid
    # MDX. As such, we use React's dangerouslySetInnerHTML.
    mdx_wrapped = (
        """
import { useMemo } from 'react';
\n"""
        + f"""
<div dangerouslySetInnerHTML={{{{__html: {json.dumps(str(article))}}}}}></div>
"""
    )

    return md_meta + mdx_wrapped


def main():
    DOCS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for doc in SPHINX_BUILD_DIR.glob("**/*.html"):
        md = convert_html_to_md(doc)

        outfile = DOCS_OUTPUT_DIR / doc.relative_to(SPHINX_BUILD_DIR).with_suffix(".md")
        outfile.parent.mkdir(parents=True, exist_ok=True)
        outfile.write_text(md)

        print(f"Generated {outfile}")


if __name__ == "__main__":
    main()
