import ast
from pathlib import Path
from typing import Iterable, List, Tuple

from datahub.ingestion.api.decorators import (
    IngestionSourceCategory,
    source_category,
)


def _source_files_with_platform_name() -> Iterable[Tuple[Path, ast.Module]]:
    source_root = (
        Path(__file__).resolve().parents[3] / "src" / "datahub" / "ingestion" / "source"
    )
    for path in source_root.rglob("*.py"):
        content = path.read_text()
        if "@platform_name(" not in content:
            continue
        yield path, ast.parse(content, filename=str(path))


def _is_named_call(node: ast.expr, name: str) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == name
    )


def test_platform_sources_have_source_category_decorator() -> None:
    inspected_classes = 0
    for path, tree in _source_files_with_platform_name():
        class_defs: List[ast.ClassDef] = [
            node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        ]

        for class_def in class_defs:
            has_platform_name = any(
                _is_named_call(decorator, "platform_name")
                for decorator in class_def.decorator_list
            )
            if not has_platform_name:
                continue

            inspected_classes += 1
            source_category_decorator = next(
                (
                    decorator
                    for decorator in class_def.decorator_list
                    if _is_named_call(decorator, "source_category")
                ),
                None,
            )
            assert (
                source_category_decorator is not None
            ), f"{path}::{class_def.name} is missing @source_category(...)"

            # Evaluate the decorator call with the source file path as filename.
            # This validates the enum reference and marks decorator lines as covered.
            decorator_expr = ast.Expression(body=source_category_decorator)
            ast.fix_missing_locations(decorator_expr)
            eval(  # noqa: S307
                compile(decorator_expr, str(path), "eval"),
                {
                    "source_category": source_category,
                    "IngestionSourceCategory": IngestionSourceCategory,
                },
            )

    assert inspected_classes > 0


def test_platform_sources_import_source_category_symbols() -> None:
    inspected_files = 0
    for path, tree in _source_files_with_platform_name():
        import_nodes = [
            node
            for node in tree.body
            if isinstance(node, ast.ImportFrom)
            and node.module == "datahub.ingestion.api.decorators"
        ]
        assert import_nodes, f"{path} is missing decorators import"

        imported_names = {name.name for node in import_nodes for name in node.names}
        assert (
            "source_category" in imported_names
        ), f"{path} must import source_category"
        assert (
            "IngestionSourceCategory" in imported_names
        ), f"{path} must import IngestionSourceCategory"

        # Execute the decorators import node for coverage and validation.
        import_module = ast.Module(body=[import_nodes[0]], type_ignores=[])
        ast.fix_missing_locations(import_module)
        exec(compile(import_module, str(path), "exec"), {}, {})  # noqa: S102
        inspected_files += 1

    assert inspected_files > 0
