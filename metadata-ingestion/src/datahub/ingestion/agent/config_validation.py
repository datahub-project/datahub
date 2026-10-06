from typing import Mapping, Type, TypeVar

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent.error_policy import call_config_hook

_ConfigT = TypeVar("_ConfigT", bound=ConfigModel)


def validate_source_config(
    config_cls: Type[_ConfigT], source_type: str, config_dict: Mapping[str, object]
) -> _ConfigT:
    """Build `config_cls` from a recipe the way the source's own create() does.

    Registered names sharing one config class may validate with different
    pydantic contexts; a config states its context through
    `probe_validation_context(source_type=...)`.

    A ValidationError comes back as a ValueError naming each failing field and
    why. pydantic's input_value is never included: its truncated repr of a
    secret matches no registered value, so nothing downstream could mask it.
    A validator's own message is kept, since it is the diagnostic a recipe
    author needs ("either password or private_key must be set"); it may
    quote what it rejected, so callers scrub this text against the recipe's
    secrets and credential shapes, as the CLI does. The hook is called as
    probe_methods.config_hook calls the others; it is read here because
    probe_methods imports this module.
    """
    hook = getattr(config_cls, "probe_validation_context", None)
    context = (
        call_config_hook(
            config_cls, "probe_validation_context", hook, source_type=source_type
        )
        if callable(hook)
        else None
    )
    try:
        if context is None:
            # Omitted, so a model_validate taking no context still works.
            return config_cls.model_validate(config_dict)
        return config_cls.model_validate(config_dict, context=context)
    except pydantic.ValidationError as exc:
        raise ValueError(describe_validation_error(exc)) from None


def describe_validation_error(exc: pydantic.ValidationError) -> str:
    """Each failing field's path, message and error type, never its input."""
    errors = exc.errors(include_input=False, include_url=False)
    lines = [
        f"{len(errors)} validation error{'s' if len(errors) != 1 else ''} "
        f"for {exc.title}"
    ]
    for error in errors:
        loc = ".".join(str(part) for part in error.get("loc", ())) or "(config)"
        lines.append(f"{loc}\n  {error.get('msg', '')} [type={error.get('type', '')}]")
    return "\n".join(lines)
