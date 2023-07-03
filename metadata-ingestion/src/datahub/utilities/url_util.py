import re


def remove_port_from_url(base_url: str) -> str:
    m = re.match("^(.*):([0-9]+)$", base_url)
    if m is not None:
        base_url = m[1]
    return base_url
