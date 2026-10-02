"""Driver and SDK errors that carry a short machine code next to their text.

Shaped like the real ones (psycopg2, snowflake-connector, pyodbc, PyMySQL,
SQLAlchemy, requests, google-api-core, azure-core, botocore) without importing
them. Lives in its own module so the framework sees every raise as foreign.
"""

from typing import Dict, NoReturn

import requests
import sqlalchemy.exc

SENTINEL = "PLANTED-coded-secret"


class PgError(Exception):
    def __init__(self, message: str, pgcode: object) -> None:
        super().__init__(message)
        self.pgcode = pgcode


class SnowflakeProgrammingError(Exception):
    def __init__(self, message: str, errno: object, sqlstate: object) -> None:
        super().__init__(message)
        self.errno = errno
        self.sqlstate = sqlstate


class OdbcProgrammingError(Exception):
    """pyodbc's errors carry (sqlstate, message) as their args."""


OdbcProgrammingError.__module__ = "pyodbc"


class MySqlProgrammingError(Exception):
    """PyMySQL's errors carry (errno, message) as their args."""


MySqlProgrammingError.__module__ = "pymysql.err"


class PermissionDenied(Exception):
    """google.api_core's exceptions declare their HTTP status on the class."""

    code = 403


PermissionDenied.__module__ = "google.api_core.exceptions"


class HttpResponseError(Exception):
    def __init__(self, message: str, status_code: object) -> None:
        super().__init__(message)
        self.status_code = status_code


class ClientError(Exception):
    def __init__(self, message: str, response: Dict[str, object]) -> None:
        super().__init__(message)
        self.response = response


class _RaisingCode(Exception):
    """A code attribute whose getter raises, quoting what it held."""

    @property
    def sqlstate(self) -> str:
        raise RuntimeError(f"cannot read {SENTINEL}")


class _BrokenGetattr(Exception):
    def __getattr__(self, name: str) -> str:
        raise RuntimeError(f"no {name} in {SENTINEL}")


class _SneakyStr(str):
    def __str__(self) -> str:
        return SENTINEL

    def __format__(self, spec: str) -> str:
        return SENTINEL


def pg(pgcode: object = "42P01") -> NoReturn:
    raise PgError(f'relation "{SENTINEL}" does not exist', pgcode)


def snowflake(errno: object = 2003, sqlstate: object = "42S02") -> NoReturn:
    raise SnowflakeProgrammingError(
        f"Object '{SENTINEL}' does not exist or not authorized", errno, sqlstate
    )


def odbc() -> NoReturn:
    raise OdbcProgrammingError("42S02", f"Invalid object name '{SENTINEL}'")


def mysql() -> NoReturn:
    raise MySqlProgrammingError(1146, f"Table '{SENTINEL}' doesn't exist")


def sqlalchemy_wrapping_pg() -> NoReturn:
    raise sqlalchemy.exc.ProgrammingError(
        f"SELECT * FROM {SENTINEL}",
        {"p": SENTINEL},
        PgError(f'relation "{SENTINEL}" does not exist', "42P01"),
    )


def http(status: int = 403) -> NoReturn:
    response = requests.Response()
    response.status_code = status
    response.url = f"https://host/api?token={SENTINEL}"
    raise requests.HTTPError(f"{status} for url {response.url}", response=response)


def google() -> NoReturn:
    raise PermissionDenied(f"caller {SENTINEL} lacks bigquery.tables.list")


def azure(status_code: object = 404) -> NoReturn:
    raise HttpResponseError(f"container {SENTINEL} not found", status_code)


def aws(code: object = "AccessDenied") -> NoReturn:
    raise ClientError(
        f"An error occurred ({code}) for arn:aws:iam::{SENTINEL}",
        {"Error": {"Code": code, "Message": SENTINEL}},
    )


def chained_from_pg() -> NoReturn:
    try:
        pg()
    except PgError as exc:
        raise RuntimeError(f"query failed: {exc}") from exc


def raising_code() -> NoReturn:
    raise _RaisingCode(SENTINEL)


def broken_getattr() -> NoReturn:
    raise _BrokenGetattr(SENTINEL)


def sneaky_str() -> NoReturn:
    raise PgError(SENTINEL, _SneakyStr("42P01"))


class _CauseProperty(Exception):
    """An exception whose chain link is a property that raises its text."""


def _raising_cause(self: BaseException) -> BaseException:
    raise RuntimeError(f"cause {SENTINEL}")


# Assigned after the class body: a property over BaseException's writable
# __cause__ is what the test needs, and what a class body cannot declare
# without mypy refusing the override.
setattr(_CauseProperty, "__cause__", property(_raising_cause))  # noqa: B010


class _HostileGetattribute(Exception):
    """Every chain and code attribute read through __getattribute__ raises."""

    def __getattribute__(self, name: str) -> object:
        if name in ("__cause__", "__context__", "args", "errno", "orig"):
            raise RuntimeError(f"no {name} in {SENTINEL}")
        return super().__getattribute__(name)


def cause_property() -> NoReturn:
    raise _CauseProperty(SENTINEL)


def hostile_getattribute() -> NoReturn:
    raise _HostileGetattribute(SENTINEL)
