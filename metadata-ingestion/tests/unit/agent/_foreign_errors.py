"""Stands in for ingestion code a probe reuses: it quotes its input on failure.

Lives in its own module so the framework sees the raise as foreign, exactly as
it sees source_connectors.py or glue.py.
"""

SENTINEL = "PLANTED-foreign-secret"


def parse_url(url: str) -> None:
    raise ValueError(f"Missing database name in JDBC URL: {url}")


def connect() -> None:
    class TransportError(Exception):
        pass

    raise TransportError(
        f"token endpoint said: {{'error': 'invalid', 'sub': '{SENTINEL}'}}"
    )


def fetch() -> None:
    raise RuntimeError(f"fetcher gave up on https://user:{SENTINEL}@host/api")


class ProgrammingError(Exception):
    """Named like a DB-API error, so classification cannot lean on the name."""


def query() -> None:
    raise ProgrammingError(f"syntax error near '{SENTINEL}'")


def read_file() -> None:
    raise PermissionError(f"cannot read /home/{SENTINEL}/.netrc")


def lookup() -> None:
    raise KeyError(f"no key {SENTINEL}")
