import logging
from typing import Iterable, List, Optional

from datahub_integrations.slack.render.render_search import get_final_platform_url
from datahub_integrations.slack.utils.entity_extract import (
    ExtractedEntity,
    extract_name,
)
from datahub_integrations.slack.utils.numbers import abbreviate_number, format_number
from datahub_integrations.slack.utils.string import truncate
from datahub_integrations.slack.utils.time import (
    get_last_updated_copy,
    show_last_updated,
)

logger = logging.getLogger(__name__)


MAX_DESCRIPTION_CHAR_COUNT = 100


def render_entity_preview(raw_entity: dict, include_link: bool = False) -> dict:
    return {
        "blocks": list(render_entity_preview_blocks(raw_entity, include_link)),
    }


def render_entity_preview_blocks(
    raw_entity: dict, include_link: bool
) -> Iterable[dict]:
    entity = ExtractedEntity(raw_entity)

    yield {
        "type": "section" if include_link else "header",
        "text": (
            {
                "type": "mrkdwn",
                "text": f"<{entity.profile_url}|*{entity.name}*>",
            }
            if include_link
            else {
                "type": "plain_text",
                "text": entity.name,
            }
        ),
    }

    if entity.platform:
        yield {
            "type": "context",
            "elements": [
                {
                    "type": "image",
                    "image_url": entity.platform_url,
                    "alt_text": "",
                },
                {"type": "mrkdwn", "text": f"{entity.platform} {entity.type}"},
            ],
        }

    if entity.description:
        yield {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{entity.description[:MAX_DESCRIPTION_CHAR_COUNT]}"
                    if entity.description is not None
                    else "No description"
                ),
            },
        }

    facts = []
    if entity.domain:
        facts.append(
            {
                "type": "mrkdwn",
                "text": f"*Domain*: {entity.domain}",
            }
        )

    if entity.owners:
        # TODO: Replace these with mentions? We'd need to be careful not to spam people though.
        facts.append(
            {
                "type": "mrkdwn",
                "text": f"*Owners*: {', '.join(entity.owners)}",
            }
        )

    if entity.glossary_terms:
        facts.append(
            {
                "type": "mrkdwn",
                "text": f"*Terms*: {', '.join(entity.glossary_terms)}",
            }
        )

    if facts:
        yield {"type": "divider"}
        yield {
            "type": "section",
            "fields": facts,
        }


def render_entity_full(entity: ExtractedEntity) -> dict:
    blocks: List[dict] = []

    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<{entity.profile_url}|*{entity.name}*>{' ⚠️' if entity.is_unhealthy else ''} {' ❗' if entity.is_deprecated else ''}",
            },
        }
    )

    # Entity Header
    if entity.platform_url or entity.type or entity.domain or entity.path_entities:
        elements = []
        if entity.platform_url:
            elements.append(
                {
                    "type": "image",
                    "alt_text": entity.platform,
                    "image_url": get_final_platform_url(entity.platform_url),
                }
            )
        if entity.path_entities or entity.type:
            elements.append(
                {
                    "type": "mrkdwn",
                    "text": f"{' | '.join([entity.type] + [extract_name(e) for e in entity.path_entities or []])}",
                }
            )
        if entity.domain:
            elements.append(
                {
                    "type": "mrkdwn",
                    "text": f"Domain: *{entity.domain}*",
                }
            )
        if len(elements) > 0:
            blocks.append(
                {
                    "type": "context",
                    "elements": elements,
                }
            )

    # Add spacer, then divider.
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": " \n\n",
            },
        },
    )

    blocks.append(
        {"type": "divider"},
    )

    if entity.health is not None and len(entity.health) > 0:
        health_messages = []
        for health_item in entity.health:
            health_message = get_entity_health_message(health_item)
            if health_message is not None:
                health_messages.append(health_message)

        if len(health_messages) > 0:
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Health* \n{health_message}",
                        }
                    ],
                }
            )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*Description* \n{truncate(entity.description, 2000) if entity.description else 'None'}\n\n",
                },
            ],
        }
    )

    # The Rich Context
    lines = []

    if entity.owners:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Owners*\n {', '.join(entity.owners)}",
            }
        )
    if entity.glossary_terms:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Glossary Terms*\n {', '.join(entity.glossary_terms)}",
            }
        )
    if entity.tags:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Tags*\n {', '.join(entity.tags)}",
            }
        )
    if entity.row_count is not None:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Rows*\n {format_number(entity.row_count)}",
            }
        )
    if entity.column_count is not None:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Columns*\n {format_number(entity.column_count)}",
            }
        )
    if entity.usage_level:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Usage Level*\n `{entity.usage_level}`",
            }
        )
    if entity.last_updated_time and show_last_updated(entity.last_ingested_time):
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Last Updated*\n {get_last_updated_copy(entity.last_updated_time)}",
            }
        )
    if entity.asset_count is not None:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Asset Count*\n {abbreviate_number(entity.asset_count)}",
            }
        )
    if entity.downstream_count is not None:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Downstreams*\n {abbreviate_number(entity.downstream_count)}",
            }
        )
    if entity.upstream_count is not None:
        lines.append(
            {
                "type": "mrkdwn",
                "text": f"*Upstreams*\n {abbreviate_number(entity.upstream_count)}",
            }
        )

    for line in lines:
        blocks.append(
            {
                "type": "context",
                "elements": [line],
            }
        )

    # Actions Section
    actions = []
    if entity.external_url and entity.platform is not None:
        actions.append(
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": f"View in {entity.platform}",
                },
                "url": entity.external_url,
                "action_id": "external_redirect",
            }
        )

    if actions:
        blocks.append(
            {
                "type": "actions",
                "elements": actions,
            }
        )

    return {
        "blocks": blocks,
    }


def render_entity_modal(entity: ExtractedEntity) -> dict:
    return {
        "type": "modal",
        "title": {
            "type": "plain_text",
            "text": "Asset Details",
        },
        **render_entity_full(entity),
    }


def get_entity_health_message(health: dict) -> Optional[str]:
    health_type = health["type"]
    health_status = health["status"]
    health_message = health["message"]
    if health_status == "FAIL" and health_type == "INCIDENTS":
        return f"- :warning: {health_message}\n"
    elif health_status == "FAIL" and health_type == "ASSERTIONS":
        return f"- :x: {health_message}\n"
    elif health_status == "PASS" and health_type == "ASSERTIONS":
        return f"- :white_check_mark: {health_message}\n"
    return None
