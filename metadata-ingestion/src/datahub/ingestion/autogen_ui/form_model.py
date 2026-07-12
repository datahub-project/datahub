from typing import List, Optional

import pydantic


class FormFieldOption(pydantic.BaseModel):
    label: str
    value: str


class FormField(pydantic.BaseModel):
    # A single rendered input in the generated form.
    name: str
    label: str
    # Dotted path into the recipe object, e.g. "source.config.username".
    # Derived from the schema path, never hand-written -> the #1 drift source is gone.
    field_path: str
    widget: str
    description: Optional[str] = None
    required: bool = False
    secret: bool = False
    deprecated: bool = False
    default: Optional[object] = None
    placeholder: Optional[str] = None
    options: Optional[List[FormFieldOption]] = None
    # Shown inline vs. collapsed. Derived from section + always_show/required/secret.
    always_show: bool = False
    # Conditional enable: this field is only enabled when `depends_on` (another
    # field's name) equals `enabled_when`. e.g. view_pattern depends_on include_views.
    depends_on: Optional[str] = None
    enabled_when: Optional[object] = None
    # When set (widget == "group"), this field is a nested sub-model rendered as a
    # collapsible group of its own fields (e.g. an OAuth credential block).
    group_fields: Optional[List["FormField"]] = None


class FormSection(pydantic.BaseModel):
    key: str
    title: str
    expanded: bool
    fields: List[FormField]


class ConnectorForm(pydantic.BaseModel):
    connector: str
    display_name: str
    # Number of raw config properties, before collapsing. Useful to show how much
    # the auto-collapse tames an "overwhelming" config.
    total_properties: int
    sections: List[FormSection]
