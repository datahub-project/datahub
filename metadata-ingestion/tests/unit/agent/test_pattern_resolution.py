from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.agent.probe import resolve_pattern_field
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


class _Cfg(ConfigModel):
    schema_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    view_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    topic_patterns: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())
    # A same-named field that is NOT a pattern — must never be resolved to.
    table_pattern: str = "not-an-allow-deny-pattern"


def test_resolves_by_convention():
    assert (
        resolve_pattern_field(_Cfg, DatasetContainerSubTypes.SCHEMA) == "schema_pattern"
    )
    assert resolve_pattern_field(_Cfg, DatasetSubTypes.VIEW) == "view_pattern"


def test_resolves_the_plural_form():
    assert resolve_pattern_field(_Cfg, DatasetSubTypes.TOPIC) == "topic_patterns"


def test_ignores_a_same_named_non_pattern_field():
    # `table_pattern: str` exists but is not an AllowDenyPattern.
    assert resolve_pattern_field(_Cfg, DatasetSubTypes.TABLE) is None


def test_returns_none_when_absent():
    assert resolve_pattern_field(_Cfg, DatasetContainerSubTypes.DATABASE) is None


def test_multiword_kind_collapses_to_underscores():
    class _K(ConfigModel):
        flink_job_pattern: AllowDenyPattern = Field(
            default=AllowDenyPattern.allow_all()
        )

    assert resolve_pattern_field(_K, "Flink Job") == "flink_job_pattern"


def test_every_probe_level_resolves_to_a_real_pattern_field():
    """Drift net: every declared level/source must reach an AllowDenyPattern field,
    explicitly or by convention. Covers all probe-capable connectors, including
    future ones."""
    import importlib
    import pkgutil

    import datahub.ingestion.source as source_pkg
    from datahub.ingestion.agent.models import ProbeLeafKind
    from datahub.ingestion.agent.probe import ClientProbe, _config_class
    from datahub.ingestion.source.source_registry import source_registry

    # source_type -> config class, for every source whose config declares a probe
    configs = {}
    for source_type in source_registry.mapping:
        try:
            config_cls = _config_class(source_type)
        except Exception:
            continue
        if callable(getattr(config_cls, "probe_hierarchy", None)):
            configs[source_type] = config_cls

    assert configs, "no probe-capable sources discovered"

    # module name -> the ClientProbe objects it declares
    probes = {}
    for mod_info in pkgutil.walk_packages(
        source_pkg.__path__, "datahub.ingestion.source."
    ):
        if not mod_info.name.endswith("_probe"):
            continue
        try:
            mod = importlib.import_module(mod_info.name)
        except Exception:
            continue
        for attr in dir(mod):
            obj = getattr(mod, attr, None)
            if isinstance(obj, ClientProbe):
                probes[f"{mod_info.name}.{attr}"] = obj

    assert probes, "no ClientProbe declarations discovered"

    unresolved = []
    for label, probe in probes.items():
        for level in probe._levels:
            entries = (
                [(s.kind, s.pattern_field) for s in level.sources]
                if level.sources
                else [(level.kind, level.pattern_field)]
            )
            for kind, declared in entries:
                if declared is not None or kind == ProbeLeafKind.COLUMN:
                    continue
                # Must resolve against at least one config class that uses this probe.
                if not any(
                    resolve_pattern_field(cfg, kind) is not None
                    for cfg in configs.values()
                ):
                    unresolved.append((label, str(kind)))

    assert not unresolved, (
        "these probe levels declare no pattern_field and no config's conventional "
        f"field matches their kind: {unresolved}"
    )
