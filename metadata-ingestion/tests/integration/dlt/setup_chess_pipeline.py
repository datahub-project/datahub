"""
Creates a real dlt Chess.com pipeline in a given pipelines_dir for integration testing.

Uses MagnusCarlsen + Hikaru, Jan-Feb 2024 — deterministic and reproducible.
Requires:
    pip install "dlt[postgres]"
    dlt init chess postgres   (run once, installs chess verified source)

Called by the chess_postgres pytest fixture in test_dlt.py.
"""

from __future__ import annotations

import os

import dlt
from chess import source  # type: ignore[import]  # installed via dlt init


def setup_chess_pipeline(
    pipelines_dir: str,
    host: str = "localhost",
    port: int = 5433,
    database: str = "chess",
    username: str = "dlt",
    password: str = "dlt",
) -> str:
    """
    Run the Chess.com dlt pipeline into local Postgres.

    Returns the pipelines_dir so callers can point DltSourceConfig at it.
    """
    # Set credentials via environment — dlt reads these automatically
    os.environ["DESTINATION__POSTGRES__CREDENTIALS__HOST"] = host
    os.environ["DESTINATION__POSTGRES__CREDENTIALS__PORT"] = str(port)
    os.environ["DESTINATION__POSTGRES__CREDENTIALS__DATABASE"] = database
    os.environ["DESTINATION__POSTGRES__CREDENTIALS__USERNAME"] = username
    os.environ["DESTINATION__POSTGRES__CREDENTIALS__PASSWORD"] = password

    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline",
        destination="postgres",
        dataset_name="chess_data",
        pipelines_dir=pipelines_dir,
    )

    data = source(
        players=["MagnusCarlsen", "Hikaru"], start_month="2024/01", end_month="2024/02"
    )

    info = pipeline.run(
        data.with_resources(
            "players_profiles",
            "players_games",
            "players_archives",
            "players_online_status",
        )
    )
    assert not info.has_failed_jobs, f"dlt pipeline had failed jobs: {info}"
    return pipelines_dir
