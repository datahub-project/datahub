import re


def remove_trailing_slashes(url: str) -> str:
    while url.endswith("/"):
        url = url[:-1]
    return url


def remove_protocol(url: str) -> str:
    pattern = re.compile(r"https?://")

    return pattern.sub(
        "",
        url,
    )
