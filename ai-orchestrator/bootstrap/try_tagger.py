"""Exercise propose/apply without the chat loop, with timings.

    python bootstrap/try_tagger.py appdb.device_sessions [--apply]
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import pii_tagger  # noqa: E402
from mcp_tools import get_mcp, shutdown_mcp  # noqa: E402

PLATFORM = os.environ.get("TRY_PLATFORM", "mysql")


def urn_for(table: str) -> str:
    if table.startswith("urn:li:"):
        return table
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{table},PROD)"


async def main() -> None:
    table = sys.argv[1] if len(sys.argv) > 1 else "appdb.device_sessions"
    do_apply = "--apply" in sys.argv
    urn = urn_for(table)
    api_key = os.environ["ANTHROPIC_API_KEY"]

    mcp = await get_mcp()
    try:
        start = time.perf_counter()
        proposal = await pii_tagger.propose(mcp, dataset_urn=urn, api_key=api_key)
        propose_ms = (time.perf_counter() - start) * 1000
        print(json.dumps(proposal, indent=2))
        print(f"\npropose: {propose_ms:.0f} ms")

        by_rule = sum(1 for r in proposal["proposed"] if r["decided_by"] == "rule")
        print(f"  {by_rule} of {len(proposal['proposed'])} decided by rules (no model call)")

        if do_apply:
            start = time.perf_counter()
            result = await pii_tagger.apply(dataset_urn=urn)
            apply_ms = (time.perf_counter() - start) * 1000
            print(json.dumps(result, indent=2))
            print(f"\napply: {apply_ms:.0f} ms for {len(result['written'])} column(s)")
    finally:
        await shutdown_mcp()


if __name__ == "__main__":
    asyncio.run(main())
