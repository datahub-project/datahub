"""
Adaptive Cards generation for Microsoft Teams.
"""

from typing import Any, List, Optional

from loguru import logger


def create_teams_message_card(
    title: str,
    message: str,
    suggestions: Optional[List[str]] = None,
    is_error: bool = False,
) -> dict:
    """
    Create a Microsoft Teams Adaptive Card for a message.

    Args:
        title: The title of the card
        message: The main message content (supports markdown)
        suggestions: Optional list of follow-up suggestions
        is_error: Whether this is an error message

    Returns:
        A dictionary representing an Adaptive Card
    """

    # Choose colors based on message type - not used currently but preserved for future use
    # accent_color = (
    #     "#D13212" if is_error else "#1890FF"
    # )  # Red for errors, blue for normal

    card: dict[str, Any] = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": title,
                    "weight": "Bolder",
                    "size": "Medium",
                    "color": "Accent" if not is_error else "Attention",
                },
                {
                    "type": "TextBlock",
                    "text": message,
                    "wrap": True,
                    "spacing": "Medium",
                },
            ],
        },
    }

    # Add suggestions as action buttons if provided
    if suggestions and len(suggestions) > 0:
        actions = []
        for _i, suggestion in enumerate(suggestions[:3]):  # Limit to 3 suggestions
            actions.append(
                {
                    "type": "Action.Submit",
                    "title": suggestion,
                    "data": {"action": "followup_question", "question": suggestion},
                }
            )

        if actions:
            card["content"]["body"].append(
                {
                    "type": "TextBlock",
                    "text": "Follow-up questions:",
                    "weight": "Bolder",
                    "size": "Small",
                    "spacing": "Medium",
                }
            )
            card["content"]["actions"] = actions

    return card


def create_teams_search_results_card(
    query: str, results: List[dict], total_count: int
) -> dict:
    """
    Create a Microsoft Teams Adaptive Card for search results.

    Args:
        query: The search query
        results: List of search result entities
        total_count: Total number of results found

    Returns:
        A dictionary representing an Adaptive Card
    """

    card: dict[str, Any] = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": f'Search Results for "{query}"',
                    "weight": "Bolder",
                    "size": "Medium",
                    "color": "Accent",
                },
                {
                    "type": "TextBlock",
                    "text": f"Found {total_count} result(s)",
                    "size": "Small",
                    "color": "Default",
                    "spacing": "Small",
                },
            ],
        },
    }

    assert isinstance(card["content"]["body"], list), "Body must be a list"
    # Add individual search results
    for result in results[:5]:  # Limit to 5 results to avoid card being too long
        entity = result.get("entity", {})
        # urn = entity.get("urn", "")  # Not used currently but may be needed for future features
        entity_type = entity.get("type", "Unknown")
        properties = entity.get("properties", {})
        name = properties.get("name", "Unnamed")
        description = properties.get("description", "No description available")

        # Add result item
        result_container = {
            "type": "Container",
            "spacing": "Medium",
            "style": "emphasis",
            "items": [
                {
                    "type": "TextBlock",
                    "text": f"**{name}** ({entity_type})",
                    "weight": "Bolder",
                    "size": "Small",
                },
                {
                    "type": "TextBlock",
                    "text": description[:200]
                    + ("..." if len(description) > 200 else ""),
                    "wrap": True,
                    "size": "Small",
                    "spacing": "Small",
                },
            ],
        }

        card["content"]["body"].append(result_container)

    if total_count > 5:
        card["content"]["body"].append(
            {
                "type": "TextBlock",
                "text": f"... and {total_count - 5} more results",
                "size": "Small",
                "color": "Default",
                "spacing": "Medium",
            }
        )

    return card


def create_teams_entity_card(entity: dict) -> dict:
    """
    Create a Microsoft Teams Adaptive Card for an entity.

    Args:
        entity: The entity data from DataHub

    Returns:
        A dictionary representing an Adaptive Card
    """

    urn = entity.get("urn", "")
    entity_type = entity.get("type", "Unknown")
    properties = entity.get("properties", {})
    name = properties.get("name", "Unnamed")
    description = properties.get("description", "No description available")

    card: dict[str, Any] = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": f"{name}",
                    "weight": "Bolder",
                    "size": "Large",
                    "color": "Accent",
                },
                {
                    "type": "TextBlock",
                    "text": f"Type: {entity_type}",
                    "size": "Small",
                    "color": "Default",
                    "spacing": "Small",
                },
                {
                    "type": "TextBlock",
                    "text": description,
                    "wrap": True,
                    "spacing": "Medium",
                },
            ],
        },
    }

    # Add URN at the bottom in small text
    card["content"]["body"].append(
        {
            "type": "TextBlock",
            "text": f"URN: `{urn}`",
            "size": "Small",
            "color": "Default",
            "spacing": "Medium",
        }
    )

    return card


def create_teams_help_card() -> dict:
    """
    Create a Microsoft Teams Adaptive Card for help information.

    Returns:
        A dictionary representing an Adaptive Card
    """

    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "DataHub Bot Help",
                    "weight": "Bolder",
                    "size": "Large",
                    "color": "Accent",
                },
                {
                    "type": "TextBlock",
                    "text": "Here are the available commands:",
                    "spacing": "Medium",
                },
                {
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "**Search**: `/datahub search <query>` or `@DataHub search <query>`",
                            "wrap": True,
                        },
                        {
                            "type": "TextBlock",
                            "text": "Find datasets, tables, and other entities in DataHub",
                            "size": "Small",
                            "color": "Default",
                            "spacing": "Small",
                        },
                    ],
                },
                {
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "**Get Entity**: `/datahub get <entity_name>` or `@DataHub get <entity_name>`",
                            "wrap": True,
                        },
                        {
                            "type": "TextBlock",
                            "text": "Get detailed information about a specific entity",
                            "size": "Small",
                            "color": "Default",
                            "spacing": "Small",
                        },
                    ],
                },
                {
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "**Ask AI**: `/datahub ask <question>` or `@DataHub ask <question>`",
                            "wrap": True,
                        },
                        {
                            "type": "TextBlock",
                            "text": "Ask DataHub AI questions about your data and metadata",
                            "size": "Small",
                            "color": "Default",
                            "spacing": "Small",
                        },
                    ],
                },
                {
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "**Help**: `/datahub help` or `@DataHub help`",
                            "wrap": True,
                        },
                        {
                            "type": "TextBlock",
                            "text": "Show this help message",
                            "size": "Small",
                            "color": "Default",
                            "spacing": "Small",
                        },
                    ],
                },
                {
                    "type": "TextBlock",
                    "text": '💡 **Tip**: You can also mention @DataHub with natural language like "DataHub search pet profiles" or ask questions directly!',
                    "wrap": True,
                    "size": "Small",
                    "color": "Default",
                    "spacing": "Large",
                },
            ],
        },
    }


def create_teams_suggestions_card(suggestions: List[str]) -> dict:
    """
    Create a Microsoft Teams Adaptive Card with just suggestion buttons.

    This is used to add clickable follow-up suggestions without duplicating
    the main response content.

    Args:
        suggestions: List of follow-up suggestions

    Returns:
        A dictionary representing an Adaptive Card with action buttons
    """
    if not suggestions:
        return {}

    # Create action buttons for suggestions (limit to 5 for readability)
    actions = []
    for suggestion in suggestions[:5]:
        actions.append(
            {
                "type": "Action.Submit",
                "title": suggestion,
                "data": {"action": "followup_question", "question": suggestion},
            }
        )

    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "💡 **Follow-up questions:**",
                    "weight": "Bolder",
                    "size": "Small",
                    "color": "Accent",
                }
            ],
            "actions": actions,
        },
    }


def create_teams_sources_card(entities: List[dict]) -> dict:
    """
    Create a Microsoft Teams Adaptive Card displaying entity sources referenced in AI responses.

    This card shows the key entities that were referenced in the AI response,
    providing users with quick access to view more details about these entities.
    Now includes rich metadata in a compact mini-card format with collapsible functionality.

    Args:
        entities: List of entity dictionaries with enhanced metadata

    Returns:
        A dictionary representing an Adaptive Card with entity sources
    """
    if not entities:
        logger.info("No entities to create sources card for")
        return {}

    logger.info(f"Creating sources card for {len(entities)} entities: {entities}")

    # Limit to 5 entities for readability
    display_entities = entities[:5]
    entity_count = len(display_entities)

    # Create entity containers that can be toggled
    entity_containers = []
    for i, entity in enumerate(display_entities):
        logger.info(f"Creating mini card {i + 1}/{entity_count} for entity: {entity}")
        mini_card = _create_entity_mini_card(entity)
        logger.info(f"Generated mini card: {mini_card}")

        # Add unique ID for toggle visibility
        mini_card["id"] = f"entity_{i}"
        mini_card["isVisible"] = False  # Initially hidden

        entity_containers.append(mini_card)

    card: dict[str, Any] = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "Container",
                    "items": [
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": f"📚 **Sources ({entity_count})**",
                                            "weight": "Bolder",
                                            "size": "Small",
                                            "color": "Accent",
                                        }
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "auto",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "▼",
                                            "id": "toggle_icon",
                                            "size": "Small",
                                            "color": "Accent",
                                        }
                                    ],
                                },
                            ],
                            "selectAction": {
                                "type": "Action.ToggleVisibility",
                                "targetElements": [
                                    f"entity_{i}" for i in range(entity_count)
                                ]
                                + ["toggle_icon"],
                            },
                        }
                    ],
                }
            ]
            + entity_containers,
        },
    }

    logger.info(f"Final sources card: {card}")
    return card


def _create_entity_mini_card(entity: dict) -> dict:
    """
    Create a compact mini card for an entity with rich metadata on 1-2 lines.

    Format:
    Line 1: [✅] **EntityName** (Type) • Platform • 👤 Owner
    Line 2: 📊 Stats • Description...
    """
    name = entity.get("name", "Unknown")
    entity_type = entity.get("type", "Unknown")
    url = entity.get("url", "")
    description = entity.get("description", "")
    platform = entity.get("platform")
    owner = entity.get("owner")
    certified = entity.get("certified", False)
    stats = entity.get("stats")

    # Build line 1: [✅] **EntityName** (Type) • Platform • 👤 Owner
    line1_parts = []

    # Add certification indicator and name
    name_text = f"✅ **{name}**" if certified else f"**{name}**"
    line1_parts.append(name_text)

    # Add type in parentheses
    line1_parts.append(f"({entity_type})")

    # Add platform if available
    if platform:
        line1_parts.append(f"• {platform}")

    # Add owner if available
    if owner:
        line1_parts.append(f"• 👤 {owner}")

    line1_text = " ".join(line1_parts)

    # Build line 2: 📊 Stats • Description...
    line2_parts = []

    # Add stats if available
    if stats:
        line2_parts.append(f"📊 {stats}")

    # Add description if available and we have space
    if (
        description and len(line1_text) < 80
    ):  # Only add description if line1 isn't too long
        max_desc_length = 80 - len(" • ".join(line2_parts)) if line2_parts else 100
        if max_desc_length > 20:  # Only show description if we have reasonable space
            truncated_desc = description[:max_desc_length]
            if len(description) > max_desc_length:
                truncated_desc += "..."
            line2_parts.append(truncated_desc)

    # Create the mini card container
    items = [
        {
            "type": "TextBlock",
            "text": line1_text,
            "size": "Small",
            "color": "Default",
            "wrap": True,
        }
    ]

    # Add line 2 if we have content for it
    if line2_parts:
        line2_text = " • ".join(line2_parts)
        items.append(
            {
                "type": "TextBlock",
                "text": line2_text,
                "size": "Small",
                "color": "Default",
                "spacing": "None",
                "wrap": True,
            }
        )

    entity_container: dict[str, Any] = {
        "type": "Container",
        "spacing": "Small",
        "style": "emphasis",
        "items": items,
    }

    # Make the container clickable if URL is available
    if url:
        entity_container["selectAction"] = {
            "type": "Action.OpenUrl",
            "url": url,
        }

    return entity_container
