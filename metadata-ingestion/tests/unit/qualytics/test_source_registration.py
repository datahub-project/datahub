"""The connector is useless if DataHub's recipe loader cannot resolve it.

This verifies the entry point declared in pyproject.toml actually resolves through
DataHub's registry -- i.e. that `type: qualytics` in a recipe works. It is a packaging
check, not a getter check: it fails for real reasons (renamed class, moved module,
broken entry point) that nothing else in the suite would catch.
"""

from datahub.ingestion.source.qualytics.constants import PLATFORM
from datahub.ingestion.source.qualytics.source import QualyticsSource
from datahub.ingestion.source.source_registry import source_registry


def test_recipe_type_resolves_to_the_source_class() -> None:
    assert source_registry.get(PLATFORM) is QualyticsSource
