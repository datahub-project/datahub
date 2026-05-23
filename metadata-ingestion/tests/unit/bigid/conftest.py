"""Inject the bigid source package into the datahub namespace for unit tests."""
import importlib
import importlib.util
import pathlib
import sys

_BIGID_PKG = (
    pathlib.Path(__file__).parents[3]
    / "src"
    / "datahub"
    / "ingestion"
    / "source"
    / "bigid"
)


def _inject_bigid() -> None:
    import datahub.ingestion.source  # noqa: F401 — ensure parent is loaded

    pkg_name = "datahub.ingestion.source.bigid"
    if pkg_name in sys.modules:
        return

    spec = importlib.util.spec_from_file_location(
        pkg_name,
        _BIGID_PKG / "__init__.py",
        submodule_search_locations=[str(_BIGID_PKG)],
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[pkg_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]


_inject_bigid()
