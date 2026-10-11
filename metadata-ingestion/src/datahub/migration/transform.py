"""Stage 2 of the migration framework: URN transforms.

A *transform* is a ``Callable[[str], str]`` mapping a source URN to its target
URN. ``platform2instance`` / ``instance2instance`` / an explicit user-supplied
mapping are all just different transforms; a caller that already has target URNs
can skip this stage entirely and build :class:`MigrationPair` objects directly.
"""

from typing import Callable, Dict, Iterable, Iterator, Optional, Tuple

from datahub.emitter.mce_builder import (
    chart_urn_to_key,
    dashboard_urn_to_key,
    dataset_urn_to_key,
    make_chart_urn,
    make_dashboard_urn,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
)
from datahub.metadata.schema_classes import DataPlatformInstanceClass
from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.migration.models import MigrationPair


def replace_instance_prefix(name: str, old_instance: str, new_instance: str) -> str:
    """Replace the old platform instance prefix in an entity name with the new one.

    Entity names with a platform instance are formatted as '{instance}.{name}'.
    Raises ValueError if the name doesn't start with the expected old instance prefix,
    as this indicates a data quality issue (entity doesn't belong to the old instance).
    """
    prefix = f"{old_instance}."
    if name.startswith(prefix):
        return f"{new_instance}.{name[len(prefix) :]}"
    raise ValueError(
        f"Entity name '{name}' does not start with expected instance prefix "
        f"'{old_instance}.'. This entity may not belong to the source instance."
    )


# Entity type → (key_parser, name_extractor, urn_constructor)
_UrnSpec = Tuple[Callable, Callable, Callable]

_ENTITY_URN_SPECS: Dict[str, _UrnSpec] = {
    "dataset": (
        dataset_urn_to_key,
        lambda key: key.name,
        # `name` already carries the new instance prefix (added by
        # _rewrite_name), so platform_instance must be None here — otherwise the
        # instance segment is prepended twice (e.g. new_inst.new_inst.db.table).
        lambda key, name, _inst: make_dataset_urn_with_platform_instance(
            platform=key.platform[len("urn:li:dataPlatform:") :],
            name=name,
            platform_instance=None,
            env=str(key.origin),
        ),
    ),
    "chart": (
        chart_urn_to_key,
        lambda key: key.chartId,
        lambda key, name, _inst: make_chart_urn(platform=key.dashboardTool, name=name),
    ),
    "dashboard": (
        dashboard_urn_to_key,
        lambda key: key.dashboardId,
        lambda key, name, _inst: make_dashboard_urn(
            platform=key.dashboardTool, name=name
        ),
    ),
}


def make_urn_builder(
    entity_type: str,
    new_instance: str,
    old_instance: Optional[str] = None,
) -> Callable[[str], str]:
    """Build a URN rewriter for any supported entity type.

    When *old_instance* is ``None`` (platform-to-instance migration) the
    new instance is simply prepended.  When *old_instance* is given
    (instance-to-instance) the old prefix is replaced.
    """

    def _rewrite_name(name: str) -> str:
        if old_instance is None:
            return f"{new_instance}.{name}"
        return replace_instance_prefix(name, old_instance, new_instance)

    if entity_type == "dataFlow":

        def _dataflow_urn(src_urn: str) -> str:
            parsed = DataFlowUrn.from_string(src_urn)
            return make_data_flow_urn(
                orchestrator=parsed.orchestrator,
                flow_id=_rewrite_name(parsed.flow_id),
                cluster=parsed.cluster,
            )

        return _dataflow_urn

    if entity_type == "dataJob":

        def _datajob_urn(src_urn: str) -> str:
            parsed = DataJobUrn.from_string(src_urn)
            flow = DataFlowUrn.from_string(parsed.flow)
            new_flow_urn = make_data_flow_urn(
                orchestrator=flow.orchestrator,
                flow_id=_rewrite_name(flow.flow_id),
                cluster=flow.cluster,
            )
            return make_data_job_urn_with_flow(new_flow_urn, parsed.job_id)

        return _datajob_urn

    spec = _ENTITY_URN_SPECS.get(entity_type)
    if spec is None:
        raise ValueError(f"Unsupported entity type for URN rewriting: {entity_type}")

    key_parser, name_extractor, urn_constructor = spec

    def _entity_urn(src_urn: str) -> str:
        key = key_parser(src_urn)
        assert key
        new_name = _rewrite_name(name_extractor(key))
        return urn_constructor(key, new_name, new_instance)

    return _entity_urn


def pairs_from_transform(
    urns: Iterable[str],
    transform: Callable[[str], str],
    *,
    data_platform_instance: Optional[DataPlatformInstanceClass] = None,
) -> Iterator[MigrationPair]:
    """Turn source URNs + a transform into :class:`MigrationPair` objects.

    ``data_platform_instance`` (if given) is attached to every pair so the engine
    stamps the target instance on each migrated entity.
    """
    for src in urns:
        yield MigrationPair(
            source_urn=src,
            target_urn=transform(src),
            data_platform_instance=data_platform_instance,
        )


# --- Backward-compatible convenience wrappers ---


def make_p2i_dataset_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("dataset", new_instance=instance)


def make_p2i_chart_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("chart", new_instance=instance)


def make_p2i_dashboard_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("dashboard", new_instance=instance)


def make_p2i_dataflow_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("dataFlow", new_instance=instance)


def make_p2i_datajob_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("dataJob", new_instance=instance)


def make_i2i_dataset_urn(old_instance: str, new_instance: str) -> Callable[[str], str]:
    return make_urn_builder(
        "dataset", new_instance=new_instance, old_instance=old_instance
    )


def make_i2i_chart_urn(old_instance: str, new_instance: str) -> Callable[[str], str]:
    return make_urn_builder(
        "chart", new_instance=new_instance, old_instance=old_instance
    )


def make_i2i_dashboard_urn(
    old_instance: str, new_instance: str
) -> Callable[[str], str]:
    return make_urn_builder(
        "dashboard", new_instance=new_instance, old_instance=old_instance
    )


def make_i2i_dataflow_urn(old_instance: str, new_instance: str) -> Callable[[str], str]:
    return make_urn_builder(
        "dataFlow", new_instance=new_instance, old_instance=old_instance
    )


def make_i2i_datajob_urn(old_instance: str, new_instance: str) -> Callable[[str], str]:
    return make_urn_builder(
        "dataJob", new_instance=new_instance, old_instance=old_instance
    )
