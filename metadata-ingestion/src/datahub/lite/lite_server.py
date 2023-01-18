import logging
from typing import Dict, Iterable, List, Optional, Union

from fastapi import Depends, FastAPI, Query
from fastapi.responses import RedirectResponse

from datahub.lite.lite_local import (
    Browseable,
    DataHubLiteLocal,
    Searchable,
    SearchFlavor,
)
from datahub.metadata.schema_classes import SystemMetadataClass, _Aspect

app = FastAPI()
logger = logging.getLogger(__name__)


@app.get("/")
def redirect_to_docs():
    assert app.docs_url
    return RedirectResponse(app.docs_url)


@app.get("/ping")  # type: ignore
def ping() -> dict:  # type: ignore
    return {"ping": "pong"}


def lite() -> DataHubLiteLocal:
    from datahub.cli.lite_cli import _get_datahub_lite

    lite = _get_datahub_lite()
    return lite


@app.get("/entities")  # type: ignore
def entities_list(lite: DataHubLiteLocal = Depends(lite)) -> Iterable[str]:  # type: ignore
    # TODO add some filtering capabilities
    return lite.list_ids()


@app.get("/entities/{id}")  # type: ignore
def entities_get(  # type: ignore
    id: str,
    aspects: Optional[List[str]] = Query(None),
    lite: DataHubLiteLocal = Depends(lite),
) -> Optional[
    Dict[str, Union[str, Dict[str, Union[dict, _Aspect, SystemMetadataClass]]]]
]:
    # Queried as GET /entities/<url-encoded urn>?aspects=aspect1&aspects=aspect2&...
    logger.warning(f"get {id} aspects={aspects}")
    return lite.get(id, aspects=aspects)


@app.get("/browse")  # type: ignore
def browse(  # type: ignore
    path: str = Query("/"),
    catalog: DataHubLiteLocal = Depends(lite),
) -> Iterable[Browseable]:
    # Queried as GET /browse/?path=<url-encoded-path>
    logger.info(f"browse {path}")
    return catalog.ls(path)


@app.get("/search")  # type: ignore
def search(  # type: ignore
    query: str = Query("*"),
    flavor: SearchFlavor = Query(SearchFlavor.FREE_TEXT),
    lite: DataHubLiteLocal = Depends(lite),
) -> Iterable[Searchable]:
    # Queried as GET /search/?query=<url-encoded-query>
    logger.info(f"search {query}")
    return lite.search(query=query, flavor=flavor)


# TODO put command
