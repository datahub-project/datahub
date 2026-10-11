"""GX version probe that must import before any other great_expectations modules.

``great_expectations.data_asset`` was removed in GX 1.x, so version gating in
``action.py`` has to run before those submodule imports. Listed in
``pyproject.toml`` ``extra-standard-library`` so isort keeps this import above
third-party ``great_expectations`` imports.
"""

from typing import Optional

import packaging.version

_GX_VERSION: Optional[packaging.version.Version] = None
has_name_positional_arg: bool = False

try:
    from great_expectations import __version__ as GX_VERSION  # type: ignore

    _GX_VERSION = packaging.version.parse(GX_VERSION)
    has_name_positional_arg = _GX_VERSION >= packaging.version.Version("0.18.14")
except Exception:
    pass


def require_gx_0x() -> None:
    if _GX_VERSION is not None and _GX_VERSION.major >= 1:
        raise ImportError(
            "datahub_gx_plugin.action.DataHubValidationAction requires "
            "great-expectations<1.0.0. For GX Core 1.x, use "
            "datahub_gx_plugin.action_v1.DataHubValidationAction instead."
        )


def require_gx_1x() -> None:
    if _GX_VERSION is None or _GX_VERSION.major < 1:
        raise ImportError(
            "datahub_gx_plugin.action_v1 requires great-expectations>=1.0.0. "
            "For GX 0.17/0.18, use datahub_gx_plugin.action.DataHubValidationAction."
        )
