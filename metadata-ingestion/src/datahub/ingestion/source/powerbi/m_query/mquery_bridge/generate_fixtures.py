#!/usr/bin/env python3
"""
Optional dev-only tool for manually inspecting the parsed AST of M_QUERIES entries.

This script is no longer needed for tests (tests generate ASTs at test time now).
It is kept as an optional tool for manual AST inspection during development.

Usage (from the repo root, output defaults to ./ast_fixtures_debug/):
    PYTHONPATH=metadata-ingestion/src python \
        metadata-ingestion/src/datahub/ingestion/source/powerbi/m_query/mquery_bridge/generate_fixtures.py

To override the output directory:
    PYTHONPATH=metadata-ingestion/src python \
        metadata-ingestion/src/datahub/ingestion/source/powerbi/m_query/mquery_bridge/generate_fixtures.py \
        --output /path/to/output/
"""

import argparse
import json
import sys
from pathlib import Path

# generate_fixtures.py is at:
#   metadata-ingestion/src/datahub/ingestion/source/powerbi/m_query/mquery_bridge/
# parents: [0]=mquery_bridge [1]=m_query [2]=powerbi [3]=source [4]=ingestion
#          [5]=datahub [6]=src [7]=metadata-ingestion
TESTS_DIR = Path(__file__).resolve().parents[7] / "tests" / "integration" / "powerbi"
sys.path.insert(0, str(TESTS_DIR))

from test_m_parser import M_QUERIES  # type: ignore  # noqa: E402


def slugify(text: str) -> str:
    """Create a safe filename from the first ~40 chars of an expression."""
    slug = text[:40].replace(" ", "_").replace("\n", "").replace('"', "")
    return "".join(c for c in slug if c.isalnum() or c in "_-")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        required=False,
        type=Path,
        default=Path("./ast_fixtures_debug/"),
    )
    args = parser.parse_args()

    # Import bridge here so PYTHONPATH errors surface clearly.
    from datahub.ingestion.source.powerbi.m_query._bridge import (
        _clear_bridge,
        get_bridge,
    )

    _clear_bridge()
    bridge = get_bridge()

    args.output.mkdir(parents=True, exist_ok=True)

    for i, expression in enumerate(M_QUERIES):
        try:
            node_map = bridge.parse(expression)
            result = {"ok": True, "nodeIdMap": list(node_map.items())}
            status = "ok"
        except Exception as e:
            result = {"ok": False, "error": str(e)}
            status = "ERROR"

        slug = slugify(expression)
        out_path = args.output / f"{i:02d}_{slug}.json"
        out_path.write_text(json.dumps(result, indent=2))
        print(f"[{status}] {out_path.name}")

    _clear_bridge()
    print(f"\nGenerated {len(M_QUERIES)} fixtures in {args.output}")


if __name__ == "__main__":
    main()
