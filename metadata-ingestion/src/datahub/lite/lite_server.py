import logging
from typing import Dict, List, Optional, Union

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse

from datahub.lite.lite_local import (
    Browseable,
    DataHubLiteLocal,
    Searchable,
    SearchFlavor,
)

app = FastAPI()
logger = logging.getLogger(__name__)


@app.get("/")
def redirect_to_docs():
    assert app.docs_url
    return RedirectResponse(app.docs_url)


@app.get("/ping")
def ping() -> dict:
    return {"ping": "pong"}


def lite() -> DataHubLiteLocal:
    from datahub.cli.lite_cli import _get_datahub_lite

    lite = _get_datahub_lite()
    return lite


@app.get("/entities")
def entities_list(lite: DataHubLiteLocal = Depends(lite)) -> List[str]:
    # TODO add some filtering capabilities
    return list(lite.list_ids())


@app.get("/entities/{id}")
def entities_get(
    id: str,
    aspects: Optional[List[str]] = Query(None),
    lite: DataHubLiteLocal = Depends(lite),
) -> Dict[str, Union[str, Dict[str, dict]]]:
    # Queried as GET /entities/<url-encoded urn>?aspects=aspect1&aspects=aspect2&...
    logger.warning(f"get {id} aspects={aspects}")
    entities = lite.get(id, aspects=aspects, typed=False)
    if not entities:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entities  # type: ignore


@app.get("/browse")
def browse(
    path: str = Query("/"),
    catalog: DataHubLiteLocal = Depends(lite),
) -> List[Browseable]:
    # Queried as GET /browse/?path=<url-encoded-path>
    logger.info(f"browse {path}")
    return list(catalog.ls(path))


@app.get("/search")
def search(
    query: str = Query("*"),
    flavor: SearchFlavor = Query(SearchFlavor.FREE_TEXT),
    lite: DataHubLiteLocal = Depends(lite),
) -> List[Searchable]:
    # Queried as GET /search/?query=<url-encoded-query>
    logger.info(f"search {query}")
    return list(lite.search(query=query, flavor=flavor))


# TODO put command
