import pathlib
import json
from bs4 import BeautifulSoup


SPHINX_ROOT_DIR = pathlib.Path(".")
SPHINX_BUILD_DIR = SPHINX_ROOT_DIR / pathlib.Path("_build/html/apidocs")
DOCS_OUTPUT_DIR = pathlib.Path("../docs/python-sdk")


def html_to_mdx(html: str) -> str:
    # Because the HTML uses `class` and has `{}` in it, it isn't valid
    # MDX. As such, we use React's dangerouslySetInnerHTML.
    return f"""

<div dangerouslySetInnerHTML={{{{__html: {json.dumps(html)}}}}}></div>

"""


def bs4_to_mdx(soup: BeautifulSoup) -> str:
    # TODO: Eventually we should do something smarter here to
    # generate something that's closer to real Markdown. This would
    # be helpful, for example, for enabling Docusaurus to generate
    # a table of contents for the page.
    return html_to_mdx(str(soup))


def convert_html_to_md(html_file: pathlib.Path) -> str:
    html = html_file.read_text()
    soup = BeautifulSoup(html, "html.parser")

    body = soup.find("main").find("div", {"class": "bd-article-container"})
    article = body.find("article")

    # Remove all the "permalink to this heading" links.
    for link in article.find_all("a", {"class": "headerlink"}):
        link.decompose()

    # Remove the trailing " – " from arguments that are missing
    # a description.
    for item in article.select("dl.field-list dd p"):
        # Note - that's U+2013, not a normal hyphen.
        if str(item).endswith(" – </p>"):
            parent = item.parent
            # print("orig item", item)
            new_item = BeautifulSoup(str(item)[:-7] + "</p>", "html.parser")
            # print("new-item", str(new_item))
            parent.p.replace_with(new_item)
            # print("item post replace", parent)

    # Extract title from the h1.
    title_element = article.find("h1")
    title = title_element.text
    title_element.decompose()

    # TODO - generate nicer slugs for these pages
    md_meta = f"""---
title: {title}
---\n\n"""

    return md_meta + bs4_to_mdx(article)


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
