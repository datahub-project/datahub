#!/usr/bin/env python3
"""
Rollback compatibility report between two DataHub releases (N → N-1).

Analyzes PDL schema changes, aspect migration mutators, upgrade steps, and
schema version gaps to produce a per-change risk classification and a
top-level feasibility verdict for rolling back from N to N-1.

Usage:
    python3 .github/scripts/rollback_analysis.py --current v2.3.0rc7-cloud --target v2.2.3-cloud
    python3 .github/scripts/rollback_analysis.py --current HEAD --target v2.2.3-cloud --output report.md --json
"""

from rollback.cli import main

if __name__ == "__main__":
    main()
