#!/usr/bin/env python3
"""
Generate AST fixture JSON files for test_ast_utils.py.
Run once after building the bridge binary, then commit the output.

Usage:
    python generate_fixtures.py \
        --binary ../binaries/mquery-parser-darwin-arm64 \
        --output ../../../../tests/integration/powerbi/mquery_ast_fixtures/
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Import the M_QUERIES list from the integration test.
# generate_fixtures.py is at:
#   metadata-ingestion/src/datahub/ingestion/source/powerbi/m_query/mquery_bridge/
# parents: [0]=mquery_bridge [1]=m_query [2]=powerbi [3]=source [4]=ingestion
#          [5]=datahub [6]=src [7]=metadata-ingestion
TESTS_DIR = Path(__file__).resolve().parents[7] / "tests" / "integration" / "powerbi"
sys.path.insert(0, str(TESTS_DIR))

from test_m_parser import M_QUERIES  # type: ignore  # noqa: E402


def parse_expression(binary: Path, text: str) -> dict:
    req = json.dumps({"text": text}) + "\n"
    # Write stdout to a temp file to avoid OS pipe buffer limits (64KB) which
    # would truncate large AST responses and produce invalid JSON.
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with open(tmp_path, "wb") as out_fh:
            proc = subprocess.Popen(
                [str(binary)],
                stdin=subprocess.PIPE,
                stdout=out_fh,
                stderr=subprocess.PIPE,
            )
            proc.communicate(input=req.encode(), timeout=30)
        with open(tmp_path) as in_fh:
            return json.loads(in_fh.read())
    finally:
        os.unlink(tmp_path)


def slugify(text: str) -> str:
    """Create a safe filename from the first ~40 chars of an expression."""
    slug = text[:40].replace(" ", "_").replace("\n", "").replace('"', "")
    return "".join(c for c in slug if c.isalnum() or c in "_-")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)

    for i, expression in enumerate(M_QUERIES):
        result = parse_expression(args.binary, expression)
        slug = slugify(expression)
        out_path = args.output / f"{i:02d}_{slug}.json"
        out_path.write_text(json.dumps(result, indent=2))
        status = "ok" if result.get("ok") else "ERROR"
        print(f"[{status}] {out_path.name}")

    print(f"\nGenerated {len(M_QUERIES)} fixtures in {args.output}")


if __name__ == "__main__":
    main()
