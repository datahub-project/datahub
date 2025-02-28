import dataclasses
import json
from collections import defaultdict
from enum import Enum
from typing import Iterable, Optional

from datahub_integrations.slack.context import SearchContext

SEPARATOR = "␞"
MAX_RECOMMENDED_FILTERS = 6


class Facet(Enum):
    PLATFORM = "platform"
    ENTITY_TYPE = "_entityType"
    COMBINED_TYPE = f"_entityType{SEPARATOR}typeNames"
    SUBTYPE = "typeNames"

    def enabled(self) -> bool:
        return self != Facet.SUBTYPE

    def render(self) -> str:
        if self == Facet.PLATFORM:
            return "Platform"
        elif self == Facet.ENTITY_TYPE:
            return "Type"
        elif self == Facet.COMBINED_TYPE:
            return "Type"
        elif self == Facet.SUBTYPE:
            return "Type"

    def render_agg(self, agg: dict) -> str:
        if self == Facet.PLATFORM:
            try:
                return agg["entity"]["properties"]["displayName"]
            except (TypeError, KeyError):
                return agg["value"].title()
        elif self == Facet.ENTITY_TYPE:
            return agg["value"].title().replace("_", " ")
        elif self == Facet.COMBINED_TYPE:
            return agg["value"].split(SEPARATOR)[1].title()
        elif self == Facet.SUBTYPE:
            return agg["value"].title()


def get_graphql_filters(context: SearchContext) -> Iterable[dict]:
    """Create filters for GraphQL search query."""

    for field, (value, _) in context.filters.items():
        if field == Facet.COMBINED_TYPE.value:
            entity_type, subtype = value.split(SEPARATOR)
            yield {"field": Facet.ENTITY_TYPE.value, "value": entity_type}
            yield {"field": Facet.SUBTYPE.value, "value": subtype}
        else:
            yield {"field": field, "value": value}


def get_recommended_facet(facets: dict, context: SearchContext) -> Optional[dict]:
    """Returns single facet for which to generate filter buttons."""

    facets_map = defaultdict(
        dict,
        {
            facet["field"]: facet
            for facet in facets
            if facet["field"] in [f.value for f in Facet if f.enabled()]
        },
    )

    if Facet.PLATFORM.value not in context.filters:
        return facets_map.get(Facet.PLATFORM.value)
    elif {Facet.COMBINED_TYPE.value, Facet.ENTITY_TYPE.value}.isdisjoint(
        context.filters
    ):
        subtype_aggregations = [
            agg
            for agg in facets_map[Facet.COMBINED_TYPE.value].get("aggregations", [])
            if SEPARATOR in agg["value"]
        ]
        if subtype_aggregations:
            return {
                **facets_map[Facet.COMBINED_TYPE.value],
                "aggregations": subtype_aggregations,
            }
        else:
            return facets_map.get(Facet.ENTITY_TYPE.value)

    return None


def render_applied_filters(context: SearchContext) -> Iterable[dict]:
    if not context.filters:
        return

    yield {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "  ".join(
                f"`{Facet(name).render()} = {value}`"
                for name, (_, value) in context.filters.items()
            ),
        },
        "accessory": {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "Clear Filters",
            },
            "action_id": "search_clear_filters",
            "value": json.dumps(
                {**dataclasses.asdict(context), "page": 0, "filters": {}}
            ),
        },
    }


def render_filter_action(
    raw_facet: Optional[dict], context: SearchContext
) -> Iterable[dict]:
    if not raw_facet:
        return

    facet = Facet(raw_facet["field"])
    yield {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": f"{facet.render_agg(agg)} ({agg['count']})",
                },
                "action_id": f"search_{facet.value}_{agg['value']}",
                "value": json.dumps(
                    {
                        **dataclasses.asdict(context),
                        "page": 0,
                        # Only single value allowed per facet
                        "filters": {
                            **context.filters,
                            facet.value: (agg["value"], facet.render_agg(agg)),
                        },
                    }
                ),
            }
            for agg in raw_facet["aggregations"][:MAX_RECOMMENDED_FILTERS]
            if (agg.get("count") or 0) > 0
        ],
    }
