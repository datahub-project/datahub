"""Stands in for ingestion code a probe reuses: it quotes its input on failure.

Every message carries SENTINEL, which no probe output may ever contain.
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


def close() -> None:
    raise TypeError(f"session close got {SENTINEL}")


def parse_name(name: str) -> None:
    raise ValueError(f"cannot parse name {SENTINEL}: {name}")


class Unprintable(Exception):
    """A driver error whose rendering itself fails, quoting what it held."""

    def __str__(self) -> str:
        raise RuntimeError(f"cannot render {SENTINEL}")

    __repr__ = __str__


def unprintable() -> None:
    raise Unprintable()


def exit_process() -> None:
    """A library that gives up by exiting the process with its reason."""
    raise SystemExit(f"fatal: cannot reach https://user:{SENTINEL}@host")


class Abort(BaseException):
    """A library's own control-flow exception, outside the Exception tree."""


def abort() -> None:
    raise Abort(f"aborted while holding {SENTINEL}")
