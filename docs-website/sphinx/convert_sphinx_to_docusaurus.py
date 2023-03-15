import pathlib
from bs4 import BeautifulSoup


SPHINX_BUILD_DIR = pathlib.Path("_build/sphinx/html/_apidocs")
DOCS_OUTPUT_DIR = pathlib.Path("../docs")


def convert_html_to_md(html_file: pathlib.Path):
    with open(html_file, "r") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")

    # body = soup.find("main")

    # TODO


def main():
    for doc in SPHINX_BUILD_DIR.glob("**.html"):
        pass


if __name__ == "__main__":
    main()
