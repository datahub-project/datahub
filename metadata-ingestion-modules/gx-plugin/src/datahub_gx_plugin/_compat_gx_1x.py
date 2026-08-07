"""Import side-effect: refuse to load under GX 0.x.

Listed in ``pyproject.toml`` ``extra-standard-library`` so isort keeps this
above third-party ``great_expectations`` 1.x imports.
"""

from datahub_gx_plugin._gx_version import require_gx_1x

require_gx_1x()

GX_1X_REQUIRED = True

__all__ = ["GX_1X_REQUIRED"]
