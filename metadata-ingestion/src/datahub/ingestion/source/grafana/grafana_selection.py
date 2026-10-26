"""Which Grafana folders and dashboards ingestion selects, as pure functions of
the recipe.

Shared by GrafanaSource (ingestion) and GrafanaSourceConfig's probe hooks
(`probe filter`, which has no connection), so neither restates the rule.
"""

from typing import Protocol

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

FOLDER_PATTERN = "folder_pattern"
DASHBOARD_PATTERN = "dashboard_pattern"
BASIC_MODE = "basic_mode"


class GrafanaFilterConfig(Protocol):
    # A Protocol so this module never imports grafana_config, which imports it.
    @property
    def basic_mode(self) -> bool: ...

    @property
    def folder_pattern(self) -> AllowDenyPattern: ...

    @property
    def dashboard_pattern(self) -> AllowDenyPattern: ...


def folder_verdict(config: GrafanaFilterConfig, title: str) -> Verdict:
    """A folder is emitted only in enhanced mode, and only when folder_pattern
    keeps its title. basic_mode is not an `Enables` switch: True means skip."""
    if config.basic_mode:
        return Verdict.exclude(BASIC_MODE)
    if not config.folder_pattern.allowed(title):
        return Verdict.exclude(FOLDER_PATTERN)
    return Verdict.include()


def dashboard_verdict(config: GrafanaFilterConfig, title: str) -> Verdict:
    """A dashboard is judged on its title alone, in both modes. Its folder is
    not consulted: a dashboard inside a folder folder_pattern drops is still
    ingested."""
    if not config.dashboard_pattern.allowed(title):
        return Verdict.exclude(DASHBOARD_PATTERN)
    return Verdict.include()
