import pathlib
import tempfile
from subprocess import CalledProcessError

import aiohttp
import datahub as datahub_version
import fastapi
import fastapi.responses
from fastapi import BackgroundTasks, status
from loguru import logger

import datahub_integrations as integrations_version
from datahub_integrations.app import ROOT_DIR, STATIC_ASSETS_DIR
from datahub_integrations.dispatch.runner import SubprocessRunner

external_router = fastapi.APIRouter()


async def _serve_external_wheel(url: str) -> fastapi.responses.Response:
    logger.debug(f"Making request to {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return fastapi.responses.Response(
                content=await response.read(),
                media_type="application/octet-stream",
            )


@external_router.get("/vercel/{subdomain}/{path:path}")
async def vercel_wheel(subdomain: str, path: str) -> fastapi.responses.Response:
    # e.g. docs-website-py39pwtsy-acryldata for https://docs-website-py39pwtsy-acryldata.vercel.app/
    vercel_url = f"https://{subdomain}.vercel.app/{path}"
    return await _serve_external_wheel(vercel_url)


@external_router.get("/cf-pages/{subdomain}/{path:path}")
async def cf_pages_wheel(subdomain: str, path: str) -> fastapi.responses.Response:
    # e.g. dc0584b7.datahub-wheels for https://dc0584b7.datahub-wheels.pages.dev/
    wheel_url = f"https://{subdomain}.pages.dev/{path}"
    return await _serve_external_wheel(wheel_url)


def _get_expected_integrations_filename() -> str:
    package_name = integrations_version.__package_name__.replace("-", "_")
    version = integrations_version.__version__
    return f"{package_name}-{version}-py3-none-any.whl"


def _get_expected_metadata_ingestion_filename() -> str:
    package_name = datahub_version.__package_name__.replace("-", "_")
    version = datahub_version.__version__
    return f"{package_name}-{version}-py3-none-any.whl"


async def _build_dynamic_wheel(
    package: str,
    background_tasks: BackgroundTasks,
    expected_filename: str,
    src_dir: pathlib.Path,
) -> pathlib.Path:
    # If we have a prebuilt wheel in the static assets directory, use that.
    static_dir_location = STATIC_ASSETS_DIR / "prebuilt-dist" / expected_filename
    if static_dir_location.exists():
        return static_dir_location

    # Otherwise, build it on-the-fly.
    temp_dir_context = tempfile.TemporaryDirectory(prefix="wheel-build-")
    build_dir = pathlib.Path(temp_dir_context.__enter__())
    background_tasks.add_task(temp_dir_context.cleanup)

    runner = SubprocessRunner()
    try:
        logger.debug(f"Building {package} wheel in {build_dir}")
        await runner.execute(
            [
                "python",
                "-m",
                "build",
                "--wheel",
                str(src_dir),
                "--outdir",
                f"{build_dir}/dist",
            ],
            cwd=build_dir,
        )
    except CalledProcessError as e:
        logger.debug(
            f"Failed to build {package} wheel:\n{runner.logs.get_logs()}",
            exc_info=True,
        )
        raise fastapi.HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to build {package} wheel",
        ) from e

    # logger.debug(f"Built {package} in {build_dir}\n{runner.logs.get_logs()}")

    # We assume it builds directly into the local dist directory, with
    # the filename and version matching what we're running with.
    return pathlib.Path(build_dir) / "dist" / expected_filename


LIVE_WHEEL_SETUP = {
    "datahub": (
        ROOT_DIR / "../metadata-ingestion",
        _get_expected_metadata_ingestion_filename(),
    ),
    "integrations": (
        ROOT_DIR,
        _get_expected_integrations_filename(),
    ),
}


@external_router.get("/dist/{package}/{path:path}")
async def dynamic_build_wheel(
    package: str,
    path: str,
    background_tasks: BackgroundTasks,
) -> fastapi.responses.FileResponse:
    if package not in LIVE_WHEEL_SETUP:
        raise fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Package not found: {package}",
        )

    src_dir, expected_filename = LIVE_WHEEL_SETUP[package]

    if path != expected_filename:
        raise fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File not found: {path}. Expected path {expected_filename}",
        )

    filename = await _build_dynamic_wheel(
        package,
        background_tasks,
        expected_filename,
        src_dir,
    )

    return fastapi.responses.FileResponse(filename)
