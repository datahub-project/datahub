#!/usr/bin/env python3
"""
All-pillar agent eval runner for DataHub.

Aggregates evals from every pillar file and runs them together.
To run a single pillar in isolation use its own entry point:

    python3 scripts/dev/tests/test_evals_operate.py   # Pillar 3 — Operate
    python3 scripts/dev/tests/test_evals_query.py     # Pillar 4 — Query

Usage:
    python3 scripts/dev/tests/test_agent_evals.py              # all evals
    python3 scripts/dev/tests/test_agent_evals.py --eval 1,11  # specific evals
    python3 scripts/dev/tests/test_agent_evals.py --runs 3     # repeat N times
    python3 scripts/dev/tests/test_agent_evals.py --json       # JSON output
    python3 scripts/dev/tests/test_agent_evals.py --dry-run    # print prompts only
"""

import sys

from _eval_lib import make_main
from test_evals_operate import DEFINED_EVALS as OPERATE_EVALS
from test_evals_query import DEFINED_EVALS as QUERY_EVALS

ALL_EVALS = OPERATE_EVALS + QUERY_EVALS

main = make_main(
    ALL_EVALS,
    description="All-pillar agent eval runner for DataHub",
    meta_eval_id=5,
)

if __name__ == "__main__":
    sys.exit(main())
