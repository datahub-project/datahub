import sys
from typing import List, Optional

if sys.version_info < (3, 10):
    from importlib_metadata import EntryPoint, entry_points
else:
    from importlib.metadata import EntryPoint, entry_points


_CUSTOM_PACKAGE_GROUP_KEY = "datahub.custom_packages"

_MODELS_KEY = "models"


class CustomPackageException(Exception):
    pass


def _get_all_registered_custom_packages() -> List[EntryPoint]:
    return list(entry_points(group=_CUSTOM_PACKAGE_GROUP_KEY))


def _get_custom_package_for_name(name: str) -> Optional[str]:
    entrypoints = [
        ep for ep in _get_all_registered_custom_packages() if ep.name == name
    ]

    if not entrypoints:
        return None

    if len(entrypoints) > 1:
        all_package_options = [
            entrypoint.dist.name for entrypoint in entrypoints if entrypoint.dist
        ]
        raise CustomPackageException(
            f"Multiple custom packages registered for {name}: cannot pick between {all_package_options}"
        )

    return entrypoints[0].value


def get_custom_models_package() -> Optional[str]:
    return _get_custom_package_for_name(_MODELS_KEY)
