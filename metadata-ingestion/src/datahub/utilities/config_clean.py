def remove_trailing_slashes(url: str) -> str:
    while url.endswith("/"):
        url = url[:-1]
    return url
