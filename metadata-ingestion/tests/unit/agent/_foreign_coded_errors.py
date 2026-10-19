"""Driver and SDK errors that carry a short machine code next to their text.

Shaped like the real ones (psycopg2, snowflake-connector, pyodbc, PyMySQL,
mysqlclient, SQLAlchemy, requests, azure-core, botocore) without importing the
drivers. Every message quotes SENTINEL, which no label may ever contain.
"""

import errno as errno_codes
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


class MySqldbProgrammingError(Exception):
    """mysqlclient's errors carry (errno, message) as their args."""


MySqldbProgrammingError.__module__ = "MySQLdb._exceptions"


class HttpResponseError(Exception):
    def __init__(self, message: str, status_code: object) -> None:
        super().__init__(message)
        self.status_code = status_code


class StatusError(Exception):
    def __init__(self, message: str, status: object) -> None:
        super().__init__(message)
        self.status = status


class ClientError(Exception):
    def __init__(self, message: str, response: Dict[str, object]) -> None:
        super().__init__(message)
        self.response = response


ClientError.__module__ = "botocore.exceptions"


class FakeGoogleError(Exception):
    """Named and placed like a google-api-core error, but not one."""

    code = 403


FakeGoogleError.__module__ = "google.api_core.exceptions"


class VendorError(Exception):
    """An error whose code only its own provider knows how to read."""

    def __init__(self, message: str, vendor_code: object, status_code: object) -> None:
        super().__init__(message)
        self.vendor_code = vendor_code
        self.status_code = status_code


class SneakyStr(str):
    """A str whose rendering is not its characters."""

    def __str__(self) -> str:
        return SENTINEL

    def __format__(self, spec: str) -> str:
        return SENTINEL


class _RaisingCode(Exception):
    """A code attribute whose getter raises, quoting what it held."""

    @property
    def sqlstate(self) -> str:
        raise RuntimeError(f"cannot read {SENTINEL}")


class _BrokenGetattr(Exception):
    def __getattr__(self, name: str) -> str:
        raise RuntimeError(f"no {name} in {SENTINEL}")


class _HostileGetattribute(Exception):
    """Every chain and code attribute read through __getattribute__ raises."""

    def __getattribute__(self, name: str) -> object:
        if name in ("__cause__", "__context__", "args", "errno", "orig", "status"):
            raise RuntimeError(f"no {name} in {SENTINEL}")
        return super().__getattribute__(name)


class _CauseProperty(Exception):
    """An exception whose chain link is a property that raises its text."""


def _raising_cause(self: BaseException) -> BaseException:
    raise RuntimeError(f"cause {SENTINEL}")


# Assigned after the class body: a property over BaseException's writable
# __cause__ is what the test needs, and what a class body cannot declare
# without mypy refusing the override.
setattr(_CauseProperty, "__cause__", property(_raising_cause))  # noqa: B010


def pg(pgcode: object = "42P01") -> NoReturn:
    raise PgError(f'relation "{SENTINEL}" does not exist', pgcode)


def snowflake(errno: object = 2003, sqlstate: object = "42S02") -> NoReturn:
    raise SnowflakeProgrammingError(
        f"Object '{SENTINEL}' does not exist or not authorized", errno, sqlstate
    )


def odbc(sqlstate: object = "42S02") -> NoReturn:
    raise OdbcProgrammingError(sqlstate, f"Invalid object name '{SENTINEL}'")


def mysql(errno: object = 1146) -> NoReturn:
    raise MySqlProgrammingError(errno, f"Table '{SENTINEL}' doesn't exist")


def mysqldb() -> NoReturn:
    raise MySqldbProgrammingError(1146, f"Table '{SENTINEL}' doesn't exist")


def sqlalchemy_wrapping_pg() -> NoReturn:
    raise sqlalchemy.exc.ProgrammingError(
        f"SELECT * FROM {SENTINEL}",
        {"p": SENTINEL},
        PgError(f'relation "{SENTINEL}" does not exist', "42P01"),
    )


def sqlalchemy_wrapping_mysql() -> NoReturn:
    raise sqlalchemy.exc.ProgrammingError(
        f"SELECT * FROM {SENTINEL}",
        {"p": SENTINEL},
        MySqlProgrammingError(1146, f"Table '{SENTINEL}' doesn't exist"),
    )


def sqlalchemy_wrapping_sqlstate() -> NoReturn:
    raise sqlalchemy.exc.ProgrammingError(
        f"SELECT * FROM {SENTINEL}",
        {"p": SENTINEL},
        SnowflakeProgrammingError(f"Object '{SENTINEL}' does not exist", None, "42S02"),
    )


def sqlalchemy_wrapping_status() -> NoReturn:
    raise sqlalchemy.exc.OperationalError(
        f"SELECT * FROM {SENTINEL}",
        {"p": SENTINEL},
        HttpResponseError(f"gateway {SENTINEL} unavailable", 503),
    )


def http(status: int = 403) -> NoReturn:
    response = requests.Response()
    response.status_code = status
    response.url = f"https://host/api?token={SENTINEL}"
    raise requests.HTTPError(f"{status} for url {response.url}", response=response)


def azure(status_code: object = 404) -> NoReturn:
    raise HttpResponseError(f"container {SENTINEL} not found", status_code)


def status(code: object = 429) -> NoReturn:
    raise StatusError(f"slow down, {SENTINEL}", code)


def refused() -> NoReturn:
    raise ConnectionRefusedError(
        errno_codes.ECONNREFUSED, f"Connection refused by {SENTINEL}"
    )


def aws(code: object = "AccessDenied") -> NoReturn:
    raise ClientError(
        f"An error occurred ({code}) for arn:aws:iam::{SENTINEL}",
        {"Error": {"Code": code, "Message": SENTINEL}},
    )


def fake_google() -> NoReturn:
    raise FakeGoogleError(f"caller {SENTINEL} lacks bigquery.tables.list")


def vendor(
    vendor_code: object = "AccessDenied", status_code: object = None
) -> NoReturn:
    raise VendorError(f"table {SENTINEL} does not exist", vendor_code, status_code)


def chained_from_azure() -> NoReturn:
    try:
        azure()
    except HttpResponseError as exc:
        raise RuntimeError(f"listing failed: {exc}") from exc


def chained_from_vendor() -> NoReturn:
    try:
        vendor()
    except VendorError as exc:
        raise RuntimeError(f"query failed: {exc}") from exc


def raising_code() -> NoReturn:
    raise _RaisingCode(SENTINEL)


def broken_getattr() -> NoReturn:
    raise _BrokenGetattr(SENTINEL)


def hostile_getattribute() -> NoReturn:
    raise _HostileGetattribute(SENTINEL)


def cause_property() -> NoReturn:
    raise _CauseProperty(SENTINEL)


def connection_error_while_handling_429() -> NoReturn:
    """A 429 handled, then an unrelated failure in the handler: the 429 is
    context, not the cause, and labelling the ConnectionError with it would
    send the caller to back off from a rate limit it did not hit."""
    try:
        azure(status_code=429)
    except HttpResponseError:
        raise ConnectionError(SENTINEL)  # noqa: B904


def suppressed_429() -> NoReturn:
    try:
        azure(status_code=429)
    except HttpResponseError:
        raise ConnectionError(SENTINEL) from None
