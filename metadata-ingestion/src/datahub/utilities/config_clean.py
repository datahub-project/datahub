def remove_url_suffix(url: str):
    if url.endswith("/"):
        return url[:-1]
    return url
