import hashlib


def generate_hash(text: str) -> str:
    # Once we move to Python 3.9+, we can set `usedforsecurity=False`.
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
