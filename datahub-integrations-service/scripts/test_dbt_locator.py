import pathlib
from typing import Optional

import click
from loguru import logger

from datahub_integrations.dbt.dbt_utils import (
    AdvancedDbtProject,
    DbtFileLocator,
    DbtProject,
)


@click.command()
@click.option(
    "--dbt-project-dir", type=click.Path(exists=True, file_okay=False), required=True
)
@click.option("--dbt-unique-id", type=str, required=True)
@click.option("--original-file-path", type=str)
def main(
    dbt_project_dir: str, dbt_unique_id: str, original_file_path: Optional[str]
) -> None:
    proj: DbtProject
    if original_file_path is None:
        tmp_dir = pathlib.Path("/tmp/dbt-test-locator")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        proj = AdvancedDbtProject(
            dbt_dir=pathlib.Path(dbt_project_dir),
            base_temp_dir=tmp_dir,
        )
        original_file_path = proj.get_original_file_path(dbt_unique_id)
        logger.info(f"Original file location: {original_file_path}")
    else:
        proj = DbtProject(dbt_dir=pathlib.Path(dbt_project_dir))

    locator = DbtFileLocator(proj)
    with locator.get_dbt_yml_config(
        dbt_unique_id=dbt_unique_id, original_file_path=original_file_path
    ) as node:
        logger.info(f"Node: {node}")


if __name__ == "__main__":
    main()
