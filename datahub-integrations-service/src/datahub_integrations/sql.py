import datahub.emitter.mce_builder as builder
import fastapi
from datahub.sql_parsing.sqlglot_utils import try_format_query
from fastapi.responses import JSONResponse

from datahub_integrations.app import graph

router = fastapi.APIRouter()


@router.post("/format")
def format_sql(sql: str, platform: str) -> str:
    return try_format_query(sql, platform=platform)


@router.post("/get_cll")
def get_cll(
    sql: str,
    platform: str,
    platform_instance: str | None = None,
    env: str = builder.DEFAULT_ENV,
) -> JSONResponse:
    res = graph.parse_sql_lineage(
        sql,
        platform=platform,
        platform_instance=platform_instance,
        env=env,
    )
    return JSONResponse(content=res.json(indent=2))
