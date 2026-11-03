from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def _noop_snowflake_connect_listener():
    # The fivetran Snowflake reader registers an @event.listens_for(engine, "connect")
    # listener to issue USE DATABASE on each pooled connection. These tests mock
    # create_engine() with a MagicMock, which SQLAlchemy's event system can't register
    # a "connect" listener on (InvalidRequestError). The listener is irrelevant when the
    # engine is mocked (query execution is stubbed directly), so make registration a
    # no-op passthrough for the duration of these tests.
    with mock.patch(
        "datahub.ingestion.source.fivetran.fivetran_log_db_reader.event.listens_for",
        lambda *args, **kwargs: lambda fn: fn,
    ):
        yield
