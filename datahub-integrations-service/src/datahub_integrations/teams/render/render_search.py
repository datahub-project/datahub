import logging
from typing import Any, Dict
from urllib.parse import quote

from datahub_integrations.app import DATAHUB_FRONTEND_URL
from datahub_integrations.teams.context import SearchContext
from datahub_integrations.teams.render.render_entity import render_entity_card

logger = logging.getLogger(__name__)


def _validate_adaptive_card(card: Dict[str, Any]) -> bool:
    """Validate that an adaptive card has the required structure for Teams."""
    if not card or not isinstance(card, dict):
        return False

    # Check required top-level fields
    if card.get("contentType") != "application/vnd.microsoft.card.adaptive":
        logger.error(f"Invalid contentType: {card.get('contentType')}")
        return False

    content = card.get("content")
    if not content or not isinstance(content, dict):
        logger.error("Missing or invalid content field")
        return False

    # Check required content fields
    if content.get("type") != "AdaptiveCard":
        logger.error(f"Invalid content type: {content.get('type')}")
        return False

    if not content.get("version"):
        logger.error("Missing version field")
        return False

    body = content.get("body")
    if not body or not isinstance(body, list):
        logger.error("Missing or invalid body field")
        return False

    # Check that body items have required type field
    for i, item in enumerate(body):
        if not isinstance(item, dict) or not item.get("type"):
            logger.error(f"Body item {i} missing type field: {item}")
            return False

    return True


def render_search_teams(
    search_results: Dict[str, Any], context: SearchContext
) -> Dict[str, Any]:
    """Render search results for Teams."""

    total_count = search_results.get("total", 0)
    results = search_results.get("searchResults", [])

    if total_count == 0:
        return {"type": "message", "text": f"No results found for '{context.query}'"}

    # Create results count header card
    header_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.2",
            "body": [
                {
                    "type": "TextBlock",
                    "text": f"Found {total_count} results",
                    "size": "Medium",
                    "weight": "Bolder",
                    "color": "Default",
                }
            ],
        },
    }

    # Start with header card
    attachments = [header_card]

    # Add individual results using the same entity card rendering
    for result in results[:3]:  # Show top 3 results
        if not result or not isinstance(result, dict):
            continue

        entity = result.get("entity")
        if not entity or not isinstance(entity, dict):
            continue

        # Render each entity using the consistent entity card function
        try:
            logger.debug(f"Rendering entity card for: {entity.get('urn', 'unknown')}")
            entity_card = render_entity_card(entity)
            if entity_card:
                # Validate the card structure before adding it
                if _validate_adaptive_card(entity_card):
                    attachments.append(entity_card)
                    logger.debug(
                        f"Successfully added card for {entity.get('urn', 'unknown')}"
                    )
                else:
                    logger.error(
                        f"Invalid adaptive card structure for entity {entity.get('urn', 'unknown')}"
                    )
            else:
                logger.warning(
                    f"render_entity_card returned empty result for {entity.get('urn', 'unknown')}"
                )
        except Exception as e:
            # Log the error but continue with other results
            logger.error(
                f"Error rendering entity card for entity {entity.get('urn', 'unknown')}: {e}",
                exc_info=True,
            )
            continue

    # Add pagination footer card if more results exist
    if total_count > 3:
        # Create search URL with encoded query
        search_url = f"{DATAHUB_FRONTEND_URL}/search?page=1&query={quote(context.query)}&unionType=0"

        pagination_card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"... and {total_count - 3} more results",
                        "size": "Small",
                        "color": "Accent",
                    }
                ],
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "View All Results",
                        "url": search_url,
                    }
                ],
            },
        }
        attachments.append(pagination_card)

    return {"type": "message", "attachments": attachments}
