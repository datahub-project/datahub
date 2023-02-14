from datahub.configuration.common import ConfigModel


class DuckDBLiteConfig(ConfigModel):
    file: str
    read_only: bool = False
    options: dict = {}
