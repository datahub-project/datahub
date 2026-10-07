"""What a DB-API driver's connect() would receive from an engine, caught at
SQLAlchemy's do_connect event: after the URL's query and connect_args are
merged, before a socket opens. How a test compares the probe's engine with
ingestion's without a server."""

from typing import Any, Dict

import pytest
import sqlalchemy


class _Captured(Exception):
    pass


def driver_connect_kwargs(url: str, engine_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    engine = sqlalchemy.create_engine(url, **engine_kwargs)
    seen: Dict[str, Any] = {}

    @sqlalchemy.event.listens_for(engine, "do_connect")
    def _capture(
        dialect: Any, conn_rec: Any, cargs: Any, cparams: Dict[str, Any]
    ) -> None:
        seen.update(cparams)
        raise _Captured()

    try:
        with pytest.raises(_Captured):
            engine.connect()
    finally:
        engine.dispose()
    return seen
