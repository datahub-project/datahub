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
    UI_DEPENDS_ON,
    UI_ENABLED_WHEN,
    UI_HIDDEN,
    UI_ORDER,
    UI_PLACEHOLDER,
    UI_SECTION,
    UI_WIDGET,
    UISection,
)

RECIPE_PREFIX = "source.config"

# Above this many total properties, non-Connection sections start collapsed so
# the heaviest connectors do not overwhelm on open.
_LARGE_CONFIG_THRESHOLD = 40
_LARGE_SECTION_THRESHOLD = 8

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

# Feature-area routing. Checked in priority order in _classify so a field lands
# in the section for the feature it configures (all lineage together, etc.).
_STATEFUL_RE = re.compile(r"stateful")  # stateful_ingestion, enable_stateful_*
_LINEAGE_RE = re.compile(r"lineage")
_USAGE_RE = re.compile(r"usage|operational_stats|top_n_queries|query|queries")
_TAG_CLASS_RE = re.compile(r"classification|tag|structured_propert|owner")
# `include_`/`ingest_` toggles that survive the feature checks are "what to
# ingest" -> Scope (object types, schema detail). `enable_`/`extract_` are NOT
# included here (they're usually feature flags handled above or advanced tuning).
_SCOPE_INCLUDE_RE = re.compile(r"^(include|ingest)_")
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


def _field_declarers(config_class: Type[pydantic.BaseModel]) -> Dict[str, str]:
    # Map each field name to the name of the most-base class in the MRO that
    # declares it. DataHub configs are composed of feature mixins/bases
    # (SnowflakeConnectionConfig, BaseUsageConfig, ClassificationSourceConfigMixin,
    # ...), so the declaring class IS the logical group -- more robust than guessing
    # from field names. model_json_schema() flattens this away, so we read it from
    # the class directly. __mro__ is leaf-first; overwriting leaves the most-base
    # (the mixin that introduced the field) as the final value.
    declarers: Dict[str, str] = {}
    for klass in config_class.__mro__:
        for fname in vars(klass).get("__annotations__", {}):
            declarers[fname] = klass.__name__
    return declarers


def _class_to_section(class_name: Optional[str]) -> Optional[UISection]:
    # Derive a section from the declaring class's name. Order matters: Stateful
    # wins over Profiling/Lineage/Usage (StatefulProfilingConfigMixin is a stateful
    # toggle, not profiling config).
    if not class_name:
        return None
    if "Stateful" in class_name:
        return UISection.STATEFUL
    if "Connection" in class_name:
        return UISection.CONNECTION
    if "Filter" in class_name:
        return UISection.SCOPE
    if "Classification" in class_name:
        return UISection.CLASSIFICATION
    if "Lineage" in class_name:
        return UISection.LINEAGE
    if "Usage" in class_name:
        return UISection.USAGE
    if "Profiling" in class_name:
        return UISection.PROFILING
    return None


def _classify(
    name: str,
    core: Dict,
    ref_name: Optional[str],
    required: bool,
    secret: bool,
    explicit: Optional[str],
    mixin_section: Optional[UISection],
) -> UISection:
    if explicit:
        return UISection(explicit)
    # Universal mixin fields that should never be promoted out of Advanced.
    if name in ("env", "options", "platform_instance"):
        return UISection.ADVANCED
    # Structure first: the declaring mixin/base is the most reliable group signal.
    if mixin_section is not None:
        return mixin_section
    # Fallback: name heuristics for fields declared directly on the leaf connector
    # config (no feature mixin to key off).
    # Profiling and stateful are dedicated feature sections (sub-configs + flags).
    if name in ("profiling", "profile_pattern") or ref_name == "GEProfilingConfig":
        return UISection.PROFILING
    if _STATEFUL_RE.search(name):
        return UISection.STATEFUL
    # Classification & tags (incl. tag/structured-property filters and ownership).
    if _TAG_CLASS_RE.search(name):
        return UISection.CLASSIFICATION
    if _LINEAGE_RE.search(name):
        return UISection.LINEAGE
    if _USAGE_RE.search(name):
        return UISection.USAGE
    # What/how much to ingest: allow/deny patterns, scope selectors, include_ toggles.
    if (
        ref_name == "AllowDenyPattern"
        or _PATTERN_NAME_RE.search(name)
        or name in _SCOPE_NAMES
        or _SCOPE_INCLUDE_RE.match(name)
    ):
        return UISection.SCOPE
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


def _placeholder(prop: Dict, core: Dict) -> Optional[str]:
    # Explicit ui(placeholder=...) hint wins; else the first string example.
    # Non-string examples (dict/list for object fields) must not become a
    # placeholder string.
    hint = prop.get(UI_PLACEHOLDER)
    if isinstance(hint, str):
        return hint
    for example in core.get("examples") or []:
        if isinstance(example, str):
            return example
    return None


def _auto_dependency(name: str, property_names: set) -> Optional[Tuple[str, bool]]:
    # Convention: a `X_pattern` filter is only meaningful when the sibling
    # `include_X` / `include_Xs` toggle is on, so auto-link them (e.g.
    # view_pattern -> include_views) without a per-connector hint.
    if not name.endswith("_pattern"):
        return None
    base = name[: -len("_pattern")]
    for candidate in (f"include_{base}", f"include_{base}s"):
        if candidate in property_names:
            return candidate, True
    return None


def _build_field(
    name: str,
    prop: Dict,
    defs: Dict,
    required: bool,
    field_path: str,
    property_names: Optional[set] = None,
) -> Tuple[FormField, Optional[str]]:
    core, ref_name = _resolve(prop, defs)
    secret = _is_secret(prop)
    widget = prop.get(UI_WIDGET) or _infer_widget(name, core, ref_name, secret)
    always_show = bool(prop.get(UI_ALWAYS_SHOW)) or required or secret

    depends_on = prop.get(UI_DEPENDS_ON)
    enabled_when = prop.get(UI_ENABLED_WHEN)
    if depends_on is None and property_names is not None:
        auto = _auto_dependency(name, property_names)
        if auto is not None:
            depends_on, enabled_when = auto

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
        placeholder=_placeholder(prop, core),
        options=_options(core),
        always_show=always_show,
        depends_on=depends_on,
        enabled_when=enabled_when,
    )
    return field, ref_name


def build_form(
    connector: str, display_name: str, config_class: Type[pydantic.BaseModel]
) -> ConnectorForm:
    schema = config_class.model_json_schema()
    defs = schema.get("$defs", {})
    properties: Dict[str, Dict] = schema.get("properties", {})
    required_names = set(schema.get("required", []))
    property_names = set(properties.keys())
    declarers = _field_declarers(config_class)

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
        mixin_section = _class_to_section(declarers.get(name))
        section = _classify(
            name, core, ref_name, required, secret, explicit_section, mixin_section
        )

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

        field, _ = _build_field(
            name, prop, defs, required, f"{RECIPE_PREFIX}.{name}", property_names
        )

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
