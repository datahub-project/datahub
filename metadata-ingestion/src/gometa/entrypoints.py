import click

from gometa.configuration.yaml import YamlConfigurationMechanism
from gometa.configuration.toml import TomlConfigurationMechanism
from gometa.ingestion.run.pipeline import Pipeline, PipelineConfig

BASE_LOGGING_FORMAT = "%(message)s"
#CONNECTION_STRING_FORMAT_REGEX = re.compile(f"^{HOST_REGEX}(:{PATH_REGEX})?$")
DEFAULT_CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
EXECUTION_CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"], ignore_unknown_options=True, allow_interspersed_args=False
)

import pathlib

@click.command(context_settings=DEFAULT_CONTEXT_SETTINGS)
@click.option("-c", "--config", help="Config file in .toml or .yaml format", required=True)
def gometa_ingest(config: str):
    """Main command for ingesting metadata into DataHub"""

    config_file = pathlib.Path(config)
    if config_file.suffix == ".yaml":
      config_mech = YamlConfigurationMechanism()
    elif config_file.suffix == ".toml":
      config_mech = TomlConfigurationMechanism()
    else:
      click.serr("Cannot process this file type")

    pipeline_config = config_mech.load_config(PipelineConfig, config_file)
    pipeline = Pipeline().configure(pipeline_config).run()


