"""
Recipe library for planning tasks.

Recipes provide guidance to the planner LLM on how to approach common task patterns.
Each recipe is defined in its own file for easier maintenance.
"""

from datahub_integrations.chat.planner.recipes.broad_data_discovery import (
    RECIPE_XML as BROAD_DATA_DISCOVERY,
)
from datahub_integrations.chat.planner.recipes.classification_tier_discovery import (
    RECIPE_XML as CLASSIFICATION_TIER_DISCOVERY,
)
from datahub_integrations.chat.planner.recipes.lineage_impact_analysis import (
    RECIPE_XML as LINEAGE_IMPACT_ANALYSIS,
)
from datahub_integrations.chat.planner.recipes.metric_data_discovery import (
    RECIPE_XML as METRIC_DATA_DISCOVERY,
)
from datahub_integrations.chat.planner.recipes.pii_curated_entities import (
    RECIPE_XML as PII_CURATED_ENTITIES,
)
from datahub_integrations.chat.planner.recipes.sql_generation import (
    RECIPE_XML as SQL_GENERATION,
)


def get_recipe_guidance() -> str:
    """
    Get recipe guidance as XML for the planner system prompt.

    Returns recipes in XML format that can be included in a separate system message.
    This allows programmatic construction while keeping the main prompt clean.
    """
    return f"""<recipes>
{PII_CURATED_ENTITIES}

{LINEAGE_IMPACT_ANALYSIS}

{CLASSIFICATION_TIER_DISCOVERY}

{BROAD_DATA_DISCOVERY}

{METRIC_DATA_DISCOVERY}

{SQL_GENERATION}
</recipes>"""
