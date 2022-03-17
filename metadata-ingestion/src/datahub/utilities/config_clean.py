import re


def remove_suffix(inp: str, suffix: str, remove_all: bool = False) -> str:
    while suffix and inp.endswith(suffix):
        inp = inp[: -len(suffix)]
        if not remove_all:
            break
    return inp


def remove_trailing_slashes(url: str) -> str:
    return remove_suffix(url, "/", remove_all=True)


def remove_protocol(url: str) -> str:
    pattern = re.compile(r"https?://")

    return pattern.sub(
        "",
        url,
    )
