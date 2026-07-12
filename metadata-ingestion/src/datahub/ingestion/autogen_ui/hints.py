from enum import Enum
from typing import Dict, List, Optional, Union

# Keys used inside a pydantic Field's json_schema_extra to carry UI hints.
# Deliberately small; mirrors Airbyte's proven vocabulary so semantics are not invented.
UI_SECTION = "ui_section"
UI_ORDER = "ui_order"
UI_WIDGET = "ui_widget"
UI_ALWAYS_SHOW = "ui_always_show"
UI_HIDDEN = "ui_hidden"


class UISection(str, Enum):
    # The canonical, framework-owned section order. Connectors tag a field into a
    # section; they can never reorder the sections themselves. This is what makes
    # "Connection is first everywhere" a structural guarantee rather than a convention.
    CONNECTION = "connection"
    SCOPE = "scope"
    ENRICHMENT = "enrichment"
    ADVANCED = "advanced"


# Fixed render order + which sections start expanded.
SECTION_ORDER: List[UISection] = [
    UISection.CONNECTION,
    UISection.SCOPE,
    UISection.ENRICHMENT,
    UISection.ADVANCED,
]

SECTION_TITLES: Dict[UISection, str] = {
    UISection.CONNECTION: "Connection",
    UISection.SCOPE: "Scope & Filters",
    UISection.ENRICHMENT: "Enrichment",
    UISection.ADVANCED: "Advanced",
}

SECTION_EXPANDED_BY_DEFAULT: Dict[UISection, bool] = {
    UISection.CONNECTION: True,
    UISection.SCOPE: True,
    UISection.ENRICHMENT: False,
    UISection.ADVANCED: False,
}


def ui(
    *,
    section: Optional[UISection] = None,
    order: Optional[int] = None,
    widget: Optional[str] = None,
    always_show: Optional[bool] = None,
    hidden: Optional[bool] = None,
) -> Dict[str, Union[str, int, bool]]:
    """Build a json_schema_extra payload for a pydantic Field.

    Usage:
        warehouse: Optional[str] = Field(
            default=None,
            description="Snowflake warehouse.",
            json_schema_extra=ui(section=UISection.CONNECTION, order=40),
        )

    Only set what you need to override; everything else is inferred from the
    schema (type, SecretStr, AllowDenyPattern, required, name conventions).
    """
    extra: Dict[str, Union[str, int, bool]] = {}
    if section is not None:
        extra[UI_SECTION] = section.value
    if order is not None:
        extra[UI_ORDER] = order
    if widget is not None:
        extra[UI_WIDGET] = widget
    if always_show is not None:
        extra[UI_ALWAYS_SHOW] = always_show
    if hidden is not None:
        extra[UI_HIDDEN] = hidden
    return extra
