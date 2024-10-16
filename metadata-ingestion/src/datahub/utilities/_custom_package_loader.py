import sys
from typing import List, Optional

if sys.version_info < (3, 10):
    from importlib_metadata import EntryPoint, entry_points
else:
    from importlib.metadata import EntryPoint, entry_points


_CUSTOM_PACKAGE_GROUP_KEY = "datahub.custom_packages"

_MODELS_KEY = "models"
_URNS_KEY = "urns"


class CustomPackageException(Exception):
    pass


def _get_all_registered_custom_packages() -> List[EntryPoint]:
    return list(entry_points(group=_CUSTOM_PACKAGE_GROUP_KEY))


def _get_custom_entrypoint_for_name(name: str) -> Optional[EntryPoint]:
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

    return entrypoints[0]


def _get_custom_package_for_name(name: str) -> Optional[str]:
    entrypoint = _get_custom_entrypoint_for_name(name)
    if entrypoint:
        return entrypoint.value
    return None


def model_version_name() -> str:
    custom_entrypoint = _get_custom_entrypoint_for_name(_MODELS_KEY)
    if not custom_entrypoint:
        return "bundled"

    if not custom_entrypoint.dist:
        return custom_entrypoint.value

    custom_model_package = custom_entrypoint.dist.name
    version = custom_entrypoint.dist.version

    try:
        package = __import__(custom_model_package)

        nice_version_name = getattr(package, "nice_version_name", None)
        if nice_version_name:
            version = nice_version_name()
    except ImportError:
        pass

    return f"{custom_model_package} ({version})"


def get_custom_models_package() -> Optional[str]:
    return _get_custom_package_for_name(_MODELS_KEY)


def get_custom_urns_package() -> Optional[str]:
    return _get_custom_package_for_name(_URNS_KEY)
