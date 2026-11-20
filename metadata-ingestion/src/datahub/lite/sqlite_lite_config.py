from datahub.configuration.common import ConfigModel


class SqliteLiteConfig(ConfigModel):
    file: str
    read_only: bool = False
    # Applied as `PRAGMA <key> = <value>` on connect, e.g. {"journal_mode": "WAL"}.
    options: dict = {}
