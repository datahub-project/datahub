import re
from typing import Dict, List, Optional, Tuple, Type

import pydantic

from datahub.ingestion.autogen_ui.form_model import (
    ConnectorForm,
    FormField,
    FormFieldOption,
    FormSection,
)
from datahub.ingestion.autogen_ui.hints import (
    SECTION_EXPANDED_BY_DEFAULT,
    SECTION_ORDER,
    SECTION_TITLES,
    UI_ALWAYS_SHOW,
    UI_HIDDEN,
    UI_ORDER,
    UI_SECTION,
    UI_WIDGET,
    UISection,
)

RECIPE_PREFIX = "source.config"

# Above this many total properties, non-Connection sections start collapsed so
# the heaviest connectors do not overwhelm on open.
_LARGE_CONFIG_THRESHOLD = 40
_LARGE_SECTION_THRESHOLD = 8

# Sub-config containers that are always demoted to Advanced, regardless of name.
_ADVANCED_CONTAINERS = {
    "profiling",
    "stateful_ingestion",
    "classification",
    "profile_pattern",
}

# Field names that belong in Connection when they are not otherwise classified.
# Kept deliberately tight so a 100-property config does not dump 40 fields here.
_CONNECTION_NAMES = {
    "host",
    "host_port",
    "port",
    "account_id",
    "account",
    "url",
    "uri",
    "host_url",
    "workspace_url",
    "server",
    "base_url",
    "connect_uri",
    "scheme",
    "username",
    "user",
    "password",
    "token",
    "private_key",
    "private_key_password",
    "role",
    "warehouse",
    "region",
    "project_id",
    "project",
    "tenant_id",
    "client_id",
    "client_secret",
    "api_key",
    "authentication_type",
    "credential",
    "credentials",
    "connection",
    "database",
    "catalog",
    "warehouse_id",
}

# Nested sub-model containers whose child fields should be flattened into the
# Connection section (e.g. Kafka's `connection`, BigQuery's `credential`) instead
# of being rendered as one opaque object.
_CONNECTION_CONTAINERS = {"connection", "credential", "credentials"}

# Scope selectors (in addition to any AllowDenyPattern / *_pattern field).
# Note: plain `database`/`catalog` are connection targets (which DB to connect to),
# not filters -- they live in _CONNECTION_NAMES. The filter equivalents are the
# `*_pattern` fields, caught separately.
_SCOPE_NAMES = {
    "project_ids",
    "projects",
    "include_tables",
    "include_views",
}

_ENRICHMENT_RE = re.compile(
    r"^(include|extract|ingest|emit|enable)_.*|.*(lineage|usage|ownership|_tags|_owners)$"
)
_PATTERN_NAME_RE = re.compile(r"_pattern$")

# Default intra-section ordering for common connection fields so the host/endpoint
# leads and credentials follow -- the natural reading order operators expect,
# without requiring a per-connector ui_order hint. Lower sorts first.
_DEFAULT_ORDER = {
    "host": 10,
    "host_port": 10,
    "account_id": 10,
    "account": 10,
    "url": 10,
    "uri": 10,
    "host_url": 10,
    "workspace_url": 10,
    "server": 10,
    "base_url": 10,
    "connect_uri": 10,
    "database": 20,
    "catalog": 20,
    "project_id": 20,
    "project": 20,
    "warehouse": 30,
    "region": 30,
    "role": 32,
    "scheme": 34,
    "tenant_id": 36,
    "authentication_type": 38,
    "username": 50,
    "user": 50,
    "client_id": 52,
    "password": 54,
    "client_secret": 54,
    "token": 56,
    "api_key": 56,
    "private_key": 58,
    "private_key_password": 59,
}
_DEFAULT_ORDER_FALLBACK = 100
_DEFAULT_ORDER_DEMOTED = 10_000
_DEMOTED_NAMES = {"env", "options", "platform_instance"}


def _resolve(schema: Dict, defs: Dict) -> Tuple[Dict, Optional[str]]:
    # Resolve a $ref and unwrap Optional (anyOf: [T, null]) down to the core schema.
    # Returns (core_schema, ref_name) where ref_name is the $defs key when the field
    # points at a sub-model (used to detect AllowDenyPattern / advanced containers).
    ref_name: Optional[str] = None
    node = schema
    if "$ref" in node:
        ref_name = node["$ref"].split("/")[-1]
        node = {
            **defs.get(ref_name, {}),
            **{k: v for k, v in node.items() if k != "$ref"},
        }
    any_of = node.get("anyOf")
    if any_of:
        non_null = [b for b in any_of if b.get("type") != "null"]
        if len(non_null) == 1:
            branch = non_null[0]
            if "$ref" in branch:
                ref_name = branch["$ref"].split("/")[-1]
                branch = defs.get(ref_name, {})
            node = {**branch, **{k: v for k, v in node.items() if k not in ("anyOf",)}}
    return node, ref_name


def _is_secret(prop_schema: Dict) -> bool:
    if prop_schema.get("format") == "password":
        return True
    for branch in prop_schema.get("anyOf", []):
        if branch.get("format") == "password":
            return True
    return False


def _contains_secret(core: Dict) -> bool:
    # True if this (already-resolved) nested-model schema has a direct property
    # that is a secret (format:password). One level deep, which covers credential
    # sub-configs like AwsConnectionConfig. (Deeper nesting is a known follow-up.)
    return any(_is_secret(prop) for prop in (core.get("properties") or {}).values())


def _infer_widget(name: str, core: Dict, ref_name: Optional[str], secret: bool) -> str:
    if secret:
        return "password"
    if ref_name == "AllowDenyPattern":
        return "allow_deny"
    core_type = core.get("type")
    if core.get("enum"):
        return "select"
    if core_type == "boolean":
        return "toggle"
    if core_type == "array":
        return "list"
    if core_type == "object":
        return "keyvalue"
    if core_type == "integer" or core_type == "number":
        return "number"
    return "text"


def _classify(
    name: str,
    core: Dict,
    ref_name: Optional[str],
    required: bool,
    secret: bool,
    explicit: Optional[str],
) -> UISection:
    if explicit:
        return UISection(explicit)
    if name in _ADVANCED_CONTAINERS or ref_name in {"GEProfilingConfig"}:
        return UISection.ADVANCED
    if (
        ref_name == "AllowDenyPattern"
        or _PATTERN_NAME_RE.search(name)
        or name in _SCOPE_NAMES
    ):
        return UISection.SCOPE
    if core.get("type") == "boolean" and _ENRICHMENT_RE.match(name):
        return UISection.ENRICHMENT
    if required or secret or name in _CONNECTION_NAMES:
        return UISection.CONNECTION
    return UISection.ADVANCED


def _label(name: str, prop: Dict) -> str:
    # Use the property's own title when pydantic set one (scalar fields). For $ref
    # fields pydantic puts no property-level title, so humanize the field name
    # rather than leaking the referenced model's title (e.g. "AllowDenyPattern").
    return prop.get("title") or name.replace("_", " ").title()


def _options(core: Dict) -> Optional[List[FormFieldOption]]:
    enum = core.get("enum")
    if not enum:
        return None
    return [FormFieldOption(label=str(v), value=str(v)) for v in enum]


def _placeholder(core: Dict) -> Optional[str]:
    # Only string examples make sensible input placeholders. Some configs supply
    # a dict/list example (e.g. for an object field), which must not become a
    # placeholder string.
    for example in core.get("examples") or []:
        if isinstance(example, str):
            return example
    return None


def _build_field(
    name: str,
    prop: Dict,
    defs: Dict,
    required: bool,
    field_path: str,
) -> Tuple[FormField, Optional[str]]:
    core, ref_name = _resolve(prop, defs)
    secret = _is_secret(prop)
    widget = prop.get(UI_WIDGET) or _infer_widget(name, core, ref_name, secret)
    always_show = bool(prop.get(UI_ALWAYS_SHOW)) or required or secret

    field = FormField(
        name=name,
        label=_label(name, prop),
        field_path=field_path,
        widget=widget,
        description=prop.get("description") or core.get("description"),
        required=required,
        secret=secret,
        deprecated=bool(prop.get("deprecated")),
        default=prop.get("default"),
        placeholder=_placeholder(core),
        options=_options(core),
        always_show=always_show,
    )
    return field, ref_name


def build_form(
    connector: str, display_name: str, config_class: Type[pydantic.BaseModel]
) -> ConnectorForm:
    schema = config_class.model_json_schema()
    defs = schema.get("$defs", {})
    properties: Dict[str, Dict] = schema.get("properties", {})
    required_names = set(schema.get("required", []))

    buckets: Dict[UISection, List[Tuple[int, int, FormField]]] = {
        s: [] for s in SECTION_ORDER
    }

    def _place(
        section: UISection, ui_order: int, decl_index: int, field: FormField
    ) -> None:
        buckets[section].append((ui_order, decl_index, field))

    for decl_index, (name, prop) in enumerate(properties.items()):
        if prop.get(UI_HIDDEN):
            continue

        core, ref_name = _resolve(prop, defs)
        secret = _is_secret(prop)
        required = name in required_names
        explicit_section = prop.get(UI_SECTION)
        section = _classify(name, core, ref_name, required, secret, explicit_section)

        # Flatten connection/credential sub-models one level so their fields
        # (e.g. Kafka bootstrap servers) surface in Connection instead of an
        # opaque object blob.
        child_props = core.get("properties")
        if child_props and (name in _CONNECTION_CONTAINERS or _contains_secret(core)):
            child_required = set(core.get("required", []))
            container_required = name in required_names
            for child_index, (child_name, child_prop) in enumerate(child_props.items()):
                if child_prop.get(UI_HIDDEN):
                    continue
                child_field, _ = _build_field(
                    child_name,
                    child_prop,
                    defs,
                    child_name in child_required and container_required,
                    f"{RECIPE_PREFIX}.{name}.{child_name}",
                )
                _place(
                    UISection.CONNECTION,
                    50,
                    decl_index * 100 + child_index,
                    child_field,
                )
            continue

        field, _ = _build_field(name, prop, defs, required, f"{RECIPE_PREFIX}.{name}")

        # Ordering: explicit ui_order wins; then the common-field default order;
        # env/options/platform_instance sink to the bottom of their section.
        ui_order = prop.get(UI_ORDER)
        if ui_order is None:
            if name in _DEMOTED_NAMES:
                ui_order = _DEFAULT_ORDER_DEMOTED
            else:
                ui_order = _DEFAULT_ORDER.get(name, _DEFAULT_ORDER_FALLBACK)
        _place(section, ui_order, decl_index, field)

    sections: List[FormSection] = []
    for s in SECTION_ORDER:
        entries = sorted(buckets[s], key=lambda t: (t[0], t[1]))
        fields = [f for _, _, f in entries]
        if not fields:
            continue

        if s == UISection.CONNECTION:
            expanded = True
        elif (
            len(properties) > _LARGE_CONFIG_THRESHOLD
            or len(fields) > _LARGE_SECTION_THRESHOLD
        ):
            expanded = False
        else:
            expanded = SECTION_EXPANDED_BY_DEFAULT[s]

        sections.append(
            FormSection(
                key=s.value,
                title=SECTION_TITLES[s],
                expanded=expanded,
                fields=fields,
            )
        )

    return ConnectorForm(
        connector=connector,
        display_name=display_name,
        total_properties=len(properties),
        sections=sections,
    )
