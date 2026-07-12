from enum import Enum
from typing import Any, Dict, List, Optional, Union

# Keys used inside a pydantic Field's json_schema_extra to carry UI hints.
# Deliberately small; mirrors Airbyte's proven vocabulary so semantics are not invented.
UI_SECTION = "ui_section"
UI_ORDER = "ui_order"
UI_WIDGET = "ui_widget"
UI_ALWAYS_SHOW = "ui_always_show"
UI_HIDDEN = "ui_hidden"
UI_PLACEHOLDER = "ui_placeholder"
UI_DEPENDS_ON = "ui_depends_on"
UI_ENABLED_WHEN = "ui_enabled_when"
UI_OPTIONS = "ui_options"
UI_LABEL = "ui_label"
UI_ICON = "ui_icon"


class UISection(str, Enum):
    # The canonical, framework-owned section order. Connectors tag a field into a
    # section; they can never reorder the sections themselves. This is what makes
    # "Connection is first everywhere" a structural guarantee rather than a
    # convention. Fields are grouped by feature area (lineage, usage, profiling,
    # ...) so related settings sit together instead of in one flat "enrichment" list.
    CONNECTION = "connection"
    SCOPE = "scope"
    LINEAGE = "lineage"
    USAGE = "usage"
    PROFILING = "profiling"
    CLASSIFICATION = "classification"
    STATEFUL = "stateful"
    ADVANCED = "advanced"


# Fixed render order + which sections start expanded.
SECTION_ORDER: List[UISection] = [
    UISection.CONNECTION,
    UISection.SCOPE,
    UISection.LINEAGE,
    UISection.USAGE,
    UISection.PROFILING,
    UISection.CLASSIFICATION,
    UISection.STATEFUL,
    UISection.ADVANCED,
]

SECTION_TITLES: Dict[UISection, str] = {
    UISection.CONNECTION: "Connection",
    UISection.SCOPE: "Scope & Filters",
    UISection.LINEAGE: "Lineage",
    UISection.USAGE: "Usage & Queries",
    UISection.PROFILING: "Profiling",
    UISection.CLASSIFICATION: "Classification & Tags",
    UISection.STATEFUL: "Stateful Ingestion",
    UISection.ADVANCED: "Advanced",
}

# Framework-owned icon name per section (a stable, connector-agnostic token the
# frontend maps to its own icon set) so every connector renders the same icon for
# the same section.
SECTION_ICONS: Dict[UISection, str] = {
    UISection.CONNECTION: "link",
    UISection.SCOPE: "filter",
    UISection.LINEAGE: "flow",
    UISection.USAGE: "chart",
    UISection.PROFILING: "gauge",
    UISection.CLASSIFICATION: "tag",
    UISection.STATEFUL: "refresh",
    UISection.ADVANCED: "gear",
}

# Only Connection and Scope open by default; feature sections start collapsed so
# the form is not overwhelming.
SECTION_EXPANDED_BY_DEFAULT: Dict[UISection, bool] = {
    UISection.CONNECTION: True,
    UISection.SCOPE: True,
    UISection.LINEAGE: False,
    UISection.USAGE: False,
    UISection.PROFILING: False,
    UISection.CLASSIFICATION: False,
    UISection.STATEFUL: False,
    UISection.ADVANCED: False,
}


def ui(
    *,
    section: Optional[UISection] = None,
    order: Optional[int] = None,
    widget: Optional[str] = None,
    always_show: Optional[bool] = None,
    hidden: Optional[bool] = None,
    placeholder: Optional[str] = None,
    depends_on: Optional[str] = None,
    enabled_when: Optional[Union[bool, str]] = None,
    options: Optional[List[str]] = None,
    label: Optional[str] = None,
    icon: Optional[str] = None,
) -> Dict[str, Any]:  # pydantic Field(json_schema_extra=) expects a JsonDict
    """Build a json_schema_extra payload for a pydantic Field.

    Usage:
        warehouse: Optional[str] = Field(
            default=None,
            description="Snowflake warehouse.",
            json_schema_extra=ui(section=UISection.CONNECTION, order=40),
        )

    depends_on/enabled_when express a conditional: the field is only enabled when
    the named sibling field equals enabled_when (default True), e.g.
        view_pattern: ... = Field(
            json_schema_extra=ui(depends_on="include_views", enabled_when=True),
        )

    Only set what you need to override; everything else is inferred from the
    schema (type, SecretStr, AllowDenyPattern, required, name conventions,
    examples->placeholder, and include_X<->X_pattern dependencies).
    """
    extra: Dict[str, Any] = {}
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
    if placeholder is not None:
        extra[UI_PLACEHOLDER] = placeholder
    if depends_on is not None:
        extra[UI_DEPENDS_ON] = depends_on
        extra[UI_ENABLED_WHEN] = True if enabled_when is None else enabled_when
    if options is not None:
        extra[UI_OPTIONS] = options
    if label is not None:
        extra[UI_LABEL] = label
    if icon is not None:
        extra[UI_ICON] = icon
    return extra
