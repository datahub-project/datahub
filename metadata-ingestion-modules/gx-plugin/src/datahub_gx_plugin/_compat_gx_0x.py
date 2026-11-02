"""Import side-effect: refuse to load under GX Core 1.x.

Listed in ``pyproject.toml`` ``extra-standard-library`` so isort keeps this
above third-party ``great_expectations`` imports (those still reference the
removed ``data_asset`` package under GX 1.x).
"""

from datahub_gx_plugin._gx_version import has_name_positional_arg, require_gx_0x

require_gx_0x()

__all__ = ["has_name_positional_arg"]
