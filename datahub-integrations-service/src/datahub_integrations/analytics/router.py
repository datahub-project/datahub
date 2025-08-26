import os
import time
from datetime import datetime, timedelta
from typing import Any, Iterator, List, Optional

import fastapi
from cachetools import TTLCache, cached
from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel

from datahub_integrations.analytics.engine import (
    AggregationSpec,
    AnalyticsEngine,
    DataFormat,
    Predicate,
    Row,
)
from datahub_integrations.analytics.factory import (
    AnalyticsEngineFactory,
    AnalyticsEngineLocator,
)
from datahub_integrations.app import graph

router = fastapi.APIRouter()
engine_factory = AnalyticsEngineFactory(graph)


# TTL for S3 URI resolution cache
DATASET_S3_URI_RESOLUTION_TTL_SECONDS_ENV_VAR = (
    "INTEGRATION_SERVICE_DATASET_S3_URI_RESOLUTION_TTL_SECONDS"
)
DATASET_S3_URI_RESOLUTION_TTL_SECONDS_VALUE: int = int(
    os.getenv(DATASET_S3_URI_RESOLUTION_TTL_SECONDS_ENV_VAR) or "60"
)


def cached_with_ttl(maxsize: int, ttl_seconds: int) -> Any:
    def decorator(func):  # type: ignore
        cache = {}  # type: ignore

        def cached_func(*args, **kwargs):  # type: ignore
            key = (args, tuple(sorted(kwargs.items())))
            if key in cache:
                result, timestamp = cache[key]
                if datetime.now() - timestamp <= timedelta(seconds=ttl_seconds):
                    logger.debug(
                        f"TTL Cache hit with key {key}. Time delta was: {datetime.now() - timestamp}"
                    )
                    return result
                else:
                    logger.debug(f"TTL Cache is stale, removing key {key}")
                    del cache[key]
            else:
                logger.debug(f"TTL Cache miss with key {key}")
            result = func(*args, **kwargs)
            cache[key] = (result, datetime.now())
            if len(cache) > maxsize:
                logger.debug("TTL Cache is full, removing oldest key")
                oldest_key = min(cache, key=lambda k: cache[k][1])
                del cache[oldest_key]
            return result

        return cached_func

    return decorator


@cached(cache=TTLCache(maxsize=100, ttl=DATASET_S3_URI_RESOLUTION_TTL_SECONDS_VALUE))  # type: ignore
def extract_physical_location_or_throw(entity_urn: str) -> AnalyticsEngineLocator:  # type: ignore
    """Extract the physical location from the entity URN."""
    # if the entity urn looks like a URI then we assume this is a physical
    # location and just return it
    logger.debug(f"Resolving location for entity urn: {entity_urn}")
    if "://" in entity_urn:
        return AnalyticsEngineLocator(physical_location=entity_urn, connection_urn=None)
    # validate that entity urn is a dataset urn
    if not entity_urn.startswith("urn:li:dataset:"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Entity URN must be a dataset URN",
        )
    # get the dataset properties
    logger.debug(f"Getting dataset properties for entity urn: {entity_urn}")
    dataset_properties = graph.get_dataset_properties(entity_urn)
    logger.debug(f"Dataset properties = {dataset_properties}")
    # if dataset is not found, return 404
    if dataset_properties is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )
    # get the physical location of the dataset
    # by extracting it from the dataset properties map
    physical_location = (
        dataset_properties.customProperties.get("physical_uri")
        or dataset_properties.externalUrl
    )
    # if physical location is not found, return 404
    if physical_location is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Physical location not found",
        )

    # get the connection id of the dataset if present
    connection_urn = dataset_properties.customProperties.get("connection_urn")

    # get the last modified time of the dataset
    last_modified = (
        dataset_properties.lastModified.time
        if dataset_properties.lastModified
        else None
    )
    return AnalyticsEngineLocator(
        physical_location=physical_location,
        connection_urn=connection_urn,
        last_updated_millis=last_modified,
    )


class Field(BaseModel):
    name: str
    type: str
    description: Optional[str] = None


@router.get("/schema")
def schema(entity_urn: str) -> List[Field]:
    """Return the structure of this dataset."""

    physical_location = extract_physical_location_or_throw(entity_urn)
    # parse the physical location to determine which AnalyticsEngine to use
    # and which parameters to use for the data
    engine, parameters = engine_factory.get_engine_with_params(physical_location)

    return [Field(name=k, type=v) for k, v in engine.get_schema(parameters).items()]


@router.get("/preview")
def preview(
    entity_urn: str, limit: int = 100, format: DataFormat = DataFormat.JSON
) -> StreamingResponse:
    """Return a preview of this dataset."""

    physical_location = extract_physical_location_or_throw(entity_urn)
    # parse the physical location to determine which AnalyticsEngine to use
    # and which parameters to use for the preview
    engine, parameters = engine_factory.get_engine_with_params(physical_location)

    return StreamingResponse(
        preview_stream(engine, parameters, limit, format=format),
        media_type="text/event-stream",
    )


def stream_of_rows(iterator: Iterator[Row]) -> Iterator[str]:
    yield "["
    first = True
    for row in iterator:
        prefix = ",\n" if not first else ""
        yield prefix + row.json()
        first = False
    yield "]"


def preview_stream(
    engine: AnalyticsEngine, parameters: dict, limit: int, format: DataFormat
) -> Iterator[str]:
    # get the preview from the AnalyticsEngine
    preview: Iterator[Row] = engine.get_preview(parameters, limit=limit, format=format)
    # return the preview
    yield from stream_of_rows(preview)


@router.get("/data")
def data(entity_urn: str, format: DataFormat = DataFormat.JSON) -> StreamingResponse:
    """Return the data of this dataset."""

    physical_location = extract_physical_location_or_throw(entity_urn)
    # parse the physical location to determine which AnalyticsEngine to use
    # and which parameters to use for the data
    engine, parameters = engine_factory.get_engine_with_params(physical_location)
    logger.info(f"Parameters: {parameters}")

    return StreamingResponse(
        data_stream(engine, parameters, format, None, None, None),
        media_type="text/json-stream",
    )


@router.post("/query")
def query(
    entity_urn: str,
    format: Optional[str] = None,
    project: Optional[list[str]] = None,
    filter: Optional[Predicate] = None,
    aggregation: Optional[AggregationSpec] = None,
    sql_query_fragment: Optional[str] = None,
) -> StreamingResponse:
    """Return the data of this dataset."""
    # parse the physical location to determine which AnalyticsEngine to use
    # and which parameters to use for the data
    locator = extract_physical_location_or_throw(entity_urn)
    logger.debug(f"Locator: {locator}")
    engine, parameters = engine_factory.get_engine_with_params(locator)
    project = project or []
    data_format = (
        DataFormat(format)
        if format in DataFormat._value2member_map_
        else DataFormat.JSON
    )

    return StreamingResponse(
        data_stream(
            engine,
            parameters,
            data_format,
            project,
            filter,
            aggregation,
            sql_query_fragment,
        ),
        media_type="text/event-stream",
    )


def data_stream(
    engine: AnalyticsEngine,
    parameters: dict,
    format: DataFormat,
    project: Optional[list[str]],
    filter: Optional[Predicate],
    aggregation: Optional[AggregationSpec],
    sql_query_fragment: Optional[str] = None,
) -> Iterator[str]:
    # start timer

    start_time = time.time()
    # get the data from the AnalyticsEngine
    data = engine.get_data(
        parameters,
        format=format,
        project=project,
        filter=filter,
        aggregation=aggregation,
        sql_query_fragment=sql_query_fragment,
    )
    yield from stream_of_rows(data)
    # end timer
    end_time = time.time()
    logger.info(f"Query took {end_time - start_time} seconds")
