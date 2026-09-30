from typing import Any, Mapping


def validate_source_config(
    config_cls: Any, source_type: str, config_dict: Mapping[str, object]
) -> Any:
    """Build `config_cls` from a recipe the way the source's own create() does.

    Some sources validate differently depending on the name they were
    registered under: `mssql-odbc` is an alias of `mssql` whose only difference
    is the pydantic context SQLServerSource.create passes, and `uri_args` is
    legal only with it. A bare model_validate rejected every valid ODBC recipe
    in recipe validate, probe run and probe filter alike. A config states its
    context through `probe_validation_context(source_type=...)`; most need none.

    Imports nothing from agent/, so any agent module can use it without a cycle.
    """
    hook = getattr(config_cls, "probe_validation_context", None)
    context = hook(source_type=source_type) if callable(hook) else None
    if context is None:
        # Omitted rather than passed as None: pydantic's default is None, so
        # this is identical, and a config (or test fake) whose model_validate
        # takes no context keeps working.
        return config_cls.model_validate(config_dict)
    return config_cls.model_validate(config_dict, context=context)
