# ABOUTME: Declaration API for the environment variables the CLI and Python SDK read.
# ABOUTME: Specs registered here are rendered into the public environment variable reference docs.

from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Type, TypeVar

_UNSET = object()


class EnvVarGroup(Enum):
    """Sections of the generated reference page, rendered in definition order."""

    CONNECTION = "Connection and authentication"
    OAUTH = "Environment-configured OAuth"
    SINK = "Default ingestion sink"
    REST_SINK = "REST sink tuning"
    EMITTER = "REST emitter tuning"
    REPORTING = "Ingestion reporting"
    LOGGING = "Logging and debugging"
    SQL = "SQL parsing and lineage"
    CONNECTORS = "Connector-specific"
    RECORDING = "Recording and replay"
    EXECUTOR = "Ingestion executor"
    TELEMETRY = "Telemetry"
    CLOUD = "Cloud runtime detection"
    DOCKER = "Docker and local development"
    MISC = "Other"

    @property
    def order(self) -> int:
        return list(type(self)).index(self)


class EnvVarTier(Enum):
    BASIC = "basic"
    """Variables most users will reach for: where DataHub is, how to authenticate, how noisy to be."""

    ADVANCED = "advanced"
    """Tuning and escape hatches. Safe to set, but the defaults suit nearly everyone."""

    EXPERIMENTAL = "experimental"
    """Subject to change or removal without a deprecation cycle. Do not depend on these."""

    @property
    def order(self) -> int:
        return list(type(self)).index(self)

    @property
    def display_name(self) -> str:
        return self.value.title()


class EnvVarAudience(Enum):
    USER = "user"
    """You set this."""

    AUTO_DETECTED = "auto-detected"
    """DataHub or the surrounding platform sets this; the SDK only reads it. Setting it by hand is
    usually a mistake, and in some cases (quickstart port mappings) actively breaks things."""

    INTERNAL = "internal"
    """Test and development only. Never rendered on the reference page."""


@dataclasses.dataclass(frozen=True)
class EnvVarSpec:
    """One environment variable, as the docs should describe it.

    ``default`` holds the real Python default the reading code falls back to, so that
    ``tests/unit/configuration/test_env_var_registry.py`` can call the accessor with a cleared
    environment and catch the spec drifting away from the ``os.getenv`` call it describes.
    """

    name: str
    type: Type[object]
    description: str
    group: EnvVarGroup
    default: object = None
    tier: EnvVarTier = EnvVarTier.ADVANCED
    audience: EnvVarAudience = EnvVarAudience.USER
    secret: bool = False
    risky: bool = False
    """Changing this has a consequence a user would not expect from the name alone --
    duplicate entities, dropped lineage, credentials in logs. Rendered with a warning mark,
    and the description must say what actually goes wrong."""
    choices: Optional[Tuple[str, ...]] = None
    type_label: Optional[str] = None
    recipe_field: Optional[str] = None
    since: Optional[str] = None
    deprecated: bool = False
    deprecated_in: Optional[str] = None
    replaced_by: Optional[str] = None
    hidden: bool = False

    @property
    def display_type(self) -> str:
        if self.type_label:
            return self.type_label
        return {str: "string", int: "integer", bool: "boolean", float: "number"}.get(
            self.type, self.type.__name__
        )

    @property
    def is_documented(self) -> bool:
        return not self.hidden and self.audience is not EnvVarAudience.INTERNAL


def summarize_docstring(doc: Optional[str]) -> str:
    """A whole docstring collapsed onto one line.

    Every paragraph is kept, not just the opening one. Caveats are usually written below the
    summary, so taking the first paragraph alone is how a warning gets silently lost on the
    way to the docs -- which is exactly what happened to the ``incremental_lineage`` note on
    ``DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH``. A long cell is the lesser problem.
    """
    if not doc:
        return ""
    return " ".join(line.strip() for line in doc.strip().splitlines() if line.strip())


_REGISTRY: Dict[str, EnvVarSpec] = {}
_ACCESSORS: Dict[str, Callable[[], object]] = {}

F = TypeVar("F", bound=Callable[..., object])


def register_env_var(spec: EnvVarSpec) -> EnvVarSpec:
    """Declare a variable without decorating an accessor.

    For module-level reads that happen at import time, and for
    ``DATAHUB_SQLGLOT_DISABLE_C``, whose read site runs from a ``.pth`` file before this
    module is importable.
    """
    existing = _REGISTRY.get(spec.name)
    if existing is not None and existing != spec:
        raise ValueError(
            f"Environment variable {spec.name} is declared twice with different metadata. "
            f"Declare it once and import the accessor, or make the two declarations identical.\n"
            f"  existing: {existing}\n"
            f"  new:      {spec}"
        )
    _REGISTRY[spec.name] = spec
    return spec


def env_var(
    *,
    name: str,
    type: Type[object],
    group: EnvVarGroup,
    description: Optional[str] = None,
    default: object = _UNSET,
    tier: EnvVarTier = EnvVarTier.ADVANCED,
    audience: EnvVarAudience = EnvVarAudience.USER,
    secret: bool = False,
    risky: bool = False,
    choices: Optional[Sequence[str]] = None,
    type_label: Optional[str] = None,
    recipe_field: Optional[str] = None,
    since: Optional[str] = None,
    deprecated: bool = False,
    deprecated_in: Optional[str] = None,
    replaced_by: Optional[str] = None,
    hidden: bool = False,
) -> Callable[[F], F]:
    """Declare the environment variable an accessor function reads.

    Applies anywhere -- the decorator goes to the code that does the read, rather than
    forcing the read into one module. ``datahub.configuration.env_vars`` remains the
    conventional home for variables with no better one.

    ``description`` defaults to the accessor's own docstring, so the explanation lives in
    one place and cannot drift from the code it describes.
    """

    def wrapper(func: F) -> F:
        resolved_description = description or summarize_docstring(func.__doc__)
        if not resolved_description:
            raise ValueError(
                f"Environment variable {name} needs a description: give {func.__name__} "
                f"a docstring, or pass description= explicitly."
            )
        spec = EnvVarSpec(
            name=name,
            type=type,
            description=resolved_description,
            group=group,
            default=None if default is _UNSET else default,
            tier=tier,
            audience=audience,
            secret=secret,
            risky=risky,
            choices=tuple(choices) if choices is not None else None,
            type_label=type_label,
            recipe_field=recipe_field,
            since=since,
            deprecated=deprecated,
            deprecated_in=deprecated_in,
            replaced_by=replaced_by,
            hidden=hidden,
        )
        register_env_var(spec)
        _ACCESSORS[name] = func
        return func

    return wrapper


def declare_env_var(
    *,
    name: str,
    type: Type[object],
    description: str,
    group: EnvVarGroup,
    default: object = None,
    tier: EnvVarTier = EnvVarTier.ADVANCED,
    audience: EnvVarAudience = EnvVarAudience.USER,
    secret: bool = False,
    risky: bool = False,
    choices: Optional[Sequence[str]] = None,
    type_label: Optional[str] = None,
    recipe_field: Optional[str] = None,
    since: Optional[str] = None,
    deprecated: bool = False,
    deprecated_in: Optional[str] = None,
    replaced_by: Optional[str] = None,
    hidden: bool = False,
) -> EnvVarSpec:
    """Declare a variable that is read inline rather than through an accessor function.

    Call this at module level, next to the read, so the declaration and the
    ``os.getenv`` it describes move together.
    """
    return register_env_var(
        EnvVarSpec(
            name=name,
            type=type,
            description=description,
            group=group,
            default=default,
            tier=tier,
            audience=audience,
            secret=secret,
            risky=risky,
            choices=tuple(choices) if choices is not None else None,
            type_label=type_label,
            recipe_field=recipe_field,
            since=since,
            deprecated=deprecated,
            deprecated_in=deprecated_in,
            replaced_by=replaced_by,
            hidden=hidden,
        )
    )


def get_registry(*, include_undocumented: bool = False) -> List[EnvVarSpec]:
    """All declared variables, ordered as the reference page renders them."""
    specs = [
        spec
        for spec in _REGISTRY.values()
        if include_undocumented or spec.is_documented
    ]
    return sorted(
        specs, key=lambda spec: (spec.group.order, spec.tier.order, spec.name)
    )


def get_accessor(name: str) -> Optional[Callable[[], object]]:
    """The decorated reader for a variable, when it was declared via the decorator."""
    return _ACCESSORS.get(name)
