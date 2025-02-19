import json
from typing import Any, Dict, List, Optional

from datahub_integrations.notifications.constants import (
    INCIDENT_ADVANCED_ACTIONS_ENABLED,
)
from datahub_integrations.notifications.sinks.slack.template_utils import (
    get_initial_stage_option,
    get_stage_select_options,
)
from datahub_integrations.slack.context import IncidentContext


def render_resolve_incident(
    incident_urn: str,
    response_url: str,
    current_stage: Optional[str],
    context: IncidentContext,
) -> dict:
    blocks: List[Dict[str, Any]] = [
        {
            "type": "input",
            "block_id": "incident_message_input",
            "element": {
                "type": "plain_text_input",
                "multiline": True,
                "action_id": "incident_message_input_action",
            },
            "label": {
                "type": "plain_text",
                "text": "Note",
            },
        }
    ]

    if INCIDENT_ADVANCED_ACTIONS_ENABLED == "true":
        initial_stage_option = (
            get_initial_stage_option(current_stage, context) if current_stage else None
        )
        blocks.append(
            {
                "type": "input",
                "block_id": "incident_stage_select",
                "label": {
                    "type": "plain_text",
                    "text": "Stage",
                },
                "element": {
                    "action_id": "select_incident_stage",
                    "type": "static_select",
                    "placeholder": {"type": "plain_text", "text": "Select stage"},
                    "options": get_stage_select_options(context),
                    **(
                        {"initial_option": initial_stage_option}
                        if initial_stage_option
                        else {}
                    ),
                },
            }
        )

    return {
        "type": "modal",
        "callback_id": "resolve_incident",
        "private_metadata": json.dumps(
            {"urn": incident_urn, "response_url": response_url}
        ),
        "title": {
            "type": "plain_text",
            "text": "Resolve Incident",
        },
        "submit": {
            "type": "plain_text",
            "text": "Resolve Incident",
        },
        "close": {
            "type": "plain_text",
            "text": "Cancel",
        },
        "blocks": blocks,
    }
