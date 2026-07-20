from typing import Dict, List, Optional, Protocol, Type

from datahub.ingestion.agent.models import (
    ProbeLeafKind,
    ProbeNode,
    ProbeNodeKind,
    ProbeResult,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.source_registry import source_registry


class ProbeAdapter(Protocol):
    def supports(self, source_type: str, config_cls: Type) -> bool: ...

    def hierarchy(self) -> List[ProbeNodeKind]: ...

    def list_children(
        self,
        source_type: str,
        config_dict: Dict[str, object],
        parent_path: List[str],
        limit: int,
    ) -> ProbeResult: ...


class SqlAlchemyProbeAdapter:
    def supports(self, source_type: str, config_cls: Type) -> bool:
        # Duck-typed match: any connector config that can produce a SQLAlchemy URL
        # (the classic SQL family plus Snowflake) can be probed generically.
        return hasattr(config_cls, "get_sql_alchemy_url")

    def hierarchy(self) -> List[ProbeNodeKind]:
        return [
            DatasetContainerSubTypes.SCHEMA,
            DatasetSubTypes.TABLE,
            ProbeLeafKind.COLUMN,
        ]

    def list_children(
        self,
        source_type: str,
        config_dict: Dict[str, object],
        parent_path: List[str],
        limit: int,
    ) -> ProbeResult:
        # Lazy import: sqlalchemy is a heavy dependency only needed by this adapter.
        from sqlalchemy import create_engine, inspect

        source_cls = source_registry.get(source_type)
        # get_config_class is injected by the @config_class decorator at runtime, so it is
        # not declared on the Source base class and mypy cannot see it statically.
        get_config_class = getattr(source_cls, "get_config_class", None)
        if get_config_class is None:
            raise TypeError(f"Source {source_type!r} does not define a config class")
        config = get_config_class().model_validate(config_dict)
        url = config.get_sql_alchemy_url()
        engine = create_engine(url)
        try:
            inspector = inspect(engine)
            nodes: List[ProbeNode] = []
            truncated: bool
            if len(parent_path) == 0:
                names = inspector.get_schema_names()
                for name in names[:limit]:
                    nodes.append(
                        ProbeNode(
                            name,
                            DatasetContainerSubTypes.SCHEMA,
                            name,
                            "schema_pattern",
                        )
                    )
                truncated = len(names) > limit
            elif len(parent_path) == 1:
                schema = parent_path[0]
                tables = inspector.get_table_names(schema=schema)
                views = inspector.get_view_names(schema=schema)
                view_names = set(views)
                # get_table_names() excludes views, so merge the two listings
                # (preserving table order first) to get the full set of children.
                combined = list(tables) + [v for v in views if v not in tables]
                for name in combined[:limit]:
                    is_view = name in view_names
                    kind = DatasetSubTypes.VIEW if is_view else DatasetSubTypes.TABLE
                    pattern = "view_pattern" if is_view else "table_pattern"
                    nodes.append(ProbeNode(name, kind, f"{schema}.{name}", pattern))
                truncated = len(combined) > limit
            else:
                schema, table = parent_path[0], parent_path[1]
                cols = inspector.get_columns(table, schema=schema)
                for col in cols[:limit]:
                    nodes.append(
                        ProbeNode(
                            str(col["name"]),
                            ProbeLeafKind.COLUMN,
                            f"{schema}.{table}.{col['name']}",
                            None,
                        )
                    )
                truncated = len(cols) > limit
            return ProbeResult(
                source_type=str(source_type),
                supported=True,
                parent_path=parent_path,
                nodes=nodes,
                truncated=truncated,
            )
        finally:
            engine.dispose()


_ADAPTERS: List[ProbeAdapter] = [SqlAlchemyProbeAdapter()]


def get_probe_adapter(source_type: str) -> Optional[ProbeAdapter]:
    try:
        source_cls = source_registry.get(source_type)
    except Exception:
        return None
    # get_config_class is injected by the @config_class decorator at runtime, so it is
    # not declared on the Source base class and mypy cannot see it statically.
    get_config_class = getattr(source_cls, "get_config_class", None)
    if get_config_class is None:
        return None
    config_cls = get_config_class()
    for adapter in _ADAPTERS:
        if adapter.supports(source_type, config_cls):
            return adapter
    return None


def probe(
    source_type: str,
    config_dict: Dict[str, object],
    parent_path: List[str],
    limit: int,
) -> ProbeResult:
    adapter = get_probe_adapter(source_type)
    if adapter is None:
        return ProbeResult(
            source_type=source_type,
            supported=False,
            parent_path=parent_path,
            fallback="No live-probe adapter for this source; use test-connection or Layer C.",
        )
    return adapter.list_children(source_type, config_dict, parent_path, limit)
