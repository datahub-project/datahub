import dataclasses
import json
import logging
from typing import Dict, Iterable, Optional

from datahub_integrations.app import DATAHUB_FRONTEND_URL
from datahub_integrations.slack.context import SearchContext
from datahub_integrations.slack.render.constants import ACRYL_COLORS
from datahub_integrations.slack.render.render_filter import (
    get_recommended_facet,
    render_applied_filters,
    render_filter_action,
)
from datahub_integrations.slack.utils.entity_extract import (
    ExtractedEntity,
    extract_name,
)
from datahub_integrations.slack.utils.numbers import abbreviate_number
from datahub_integrations.slack.utils.string import pluralize, truncate
from datahub_integrations.slack.utils.time import (
    get_last_updated_copy,
    show_last_updated,
)

logger = logging.getLogger(__name__)


def render_search(
    data: dict,
    context: SearchContext,
    subscriptions: Optional[Dict[str, Optional[str]]],
) -> dict:
    start = data["start"]
    count = data["count"]
    total = data["total"]
    end = min(start + count, total)

    headline = (
        f"*Showing {start + 1}-{end} of {total} assets matching `{context.query}`*"
        if total > 0
        else f"*No assets found matching query `{context.query}`*"
    )
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": headline,
            },
        },
        *render_applied_filters(context),
        *render_filter_action(get_recommended_facet(data["facets"], context), context),
    ]

    attachments = [
        {
            "blocks": list(
                render_search_entity(ExtractedEntity(entity["entity"]), subscriptions)
            ),
            "color": ACRYL_COLORS[i],
        }
        for i, entity in enumerate(data["searchResults"])
    ]

    pagination_elements = list(_render_pagination_elements(context, total, end))
    if pagination_elements:
        attachments.append(
            {
                "blocks": [
                    {
                        "type": "actions",
                        "elements": pagination_elements,
                    }
                ]
            }
        )

    return {
        "text": f"DataHub search results for `{context.query}`",
        "blocks": blocks,
        "attachments": attachments,
    }


def _render_pagination_elements(
    context: SearchContext, total: int, end: int
) -> Iterable[dict]:
    if context.page > 0:
        yield {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "Previous",
            },
            "action_id": "search_prev",
            "value": json.dumps(
                {**dataclasses.asdict(context), "page": context.page - 1}
            ),
        }

    if end < total:
        yield {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "Next",
            },
            "action_id": "search_next",
            "value": json.dumps(
                {**dataclasses.asdict(context), "page": context.page + 1}
            ),
        }

    if total > 0:
        yield {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "View All",
            },
            "action_id": "external_redirect",
            "url": f"{DATAHUB_FRONTEND_URL}/search?query={context.query}{_build_filter_string(context)}",
        }


def render_search_entity(
    entity: ExtractedEntity, subscriptions: Optional[Dict[str, Optional[str]]]
) -> Iterable[dict]:
    if entity.entity_type in [
        "DATASET",
        "CHART",
        "DASHBOARD",
        "CONTAINER",
        "DATA_JOB",
        "DATA_FLOW",
        "DOMAIN",
        "DATA_PRODUCT",
        "GLOSSARY_TERM",
    ]:
        yield from render_base(
            entity, subscriptions is not None, (subscriptions or {}).get(entity.urn)
        )
    else:
        logger.warning(f"Unsupported entity type: {entity.entity_type}")


def get_final_platform_url(platform_url: str) -> str:
    if platform_url.startswith("http"):
        return platform_url
    # Relative URL.
    return f"https://{DATAHUB_FRONTEND_URL}{platform_url}"


def render_base(
    entity: ExtractedEntity,
    show_subscription_action: bool,
    subscription_urn: Optional[str],
) -> Iterable[dict]:
    # Health Badge
    health_status_badge = ""
    deprecated_badge = " ❗" if entity.is_deprecated else ""
    is_unhealthy = entity.is_unhealthy
    is_passing_assertions = entity.is_passing_assertions
    if is_unhealthy:
        # If incidents or failing assertions, health will be in warn.
        health_status_badge = " ⚠️"
    if is_passing_assertions:
        # If Assertions are passing, health will be passing.
        health_status_badge = " :white_check_mark: "
    yield {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"<{entity.profile_url}|*{entity.name}*>{health_status_badge} {deprecated_badge}",
        },
    }

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
            yield {
                "type": "context",
                "elements": elements,
            }

    if entity.description:
        yield {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": truncate(entity.description, 80),
                },
            ],
        }

    # Bottom Context Row Section
    elements = []

    if entity.owners:
        elements.append(_render_count("owner", len(entity.owners)))
    if entity.glossary_terms:
        elements.append(_render_count("term", len(entity.glossary_terms)))
    if entity.tags:
        elements.append(_render_count("tag", len(entity.tags)))
    if entity.row_count is not None:
        elements.append(_render_count("row", entity.row_count))
    if entity.usage_level:
        elements.append(
            {
                "type": "mrkdwn",
                "text": f"*{entity.usage_level}* usage",
            }
        )
    if entity.last_updated_time and show_last_updated(entity.last_ingested_time):
        elements.append(
            {
                "type": "mrkdwn",
                "text": f"Updated *{get_last_updated_copy(entity.last_updated_time)}*",
            }
        )
    if entity.asset_count is not None:
        elements.append(_render_count("asset", entity.asset_count))
    if entity.downstream_count is not None:
        elements.append(_render_count("downstream", entity.downstream_count))

    if elements:
        yield {
            "type": "context",
            "elements": elements,
        }

    # Actions Section
    actions = [
        {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "Post",
            },
            "action_id": "post_search_result",
            "style": "primary",
            "value": json.dumps(
                {
                    "urn": entity.urn,
                    "entity_type": entity.entity_type,
                    "name": entity.name,
                }
            ),
        },
        {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "More Details",
            },
            "action_id": "view_details",
            "value": entity.urn,
        },
    ]
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

    if show_subscription_action and subscription_urn:
        actions.append(
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "🔕 Unsubscribe",
                },
                "action_id": "unsubscribe",
                "value": subscription_urn,
            }
        )
    elif show_subscription_action:
        actions.append(
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "🔔 Subscribe",
                },
                "action_id": "subscribe",
                "value": entity.urn,
            }
        )

    yield {
        "type": "actions",
        "elements": actions,
    }


def _build_filter_string(context: SearchContext) -> str:
    if not context.filters or len(context.filters) == 0:
        return ""
    # Context.filters is facet field -> (value, display_name)
    return "&" + "&".join(
        f"filter_{key}={value[0]}" for key, value in context.filters.items()
    )


def _render_count(title: str, count: int) -> dict:
    return {
        "type": "mrkdwn",
        "text": f"*{abbreviate_number(count)}* {pluralize(title, count)}",
    }
