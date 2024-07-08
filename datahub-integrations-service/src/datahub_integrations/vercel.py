import aiohttp
import fastapi
import fastapi.responses
from loguru import logger

external_router = fastapi.APIRouter()


@external_router.get("/vercel/{subdomain}/{path:path}")
async def acryl_datahub_whl(subdomain: str, path: str) -> fastapi.responses.Response:
    # e.g. docs-website-py39pwtsy-acryldata for https://docs-website-py39pwtsy-acryldata.vercel.app/

    vercel_url = f"https://{subdomain}.vercel.app/{path}"
    logger.debug(f"Making request to {vercel_url}")

    async with aiohttp.ClientSession() as session:
        async with session.get(vercel_url) as response:
            return fastapi.responses.Response(
                content=await response.read(),
                media_type="application/octet-stream",
            )
