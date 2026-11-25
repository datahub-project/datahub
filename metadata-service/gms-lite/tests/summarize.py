"""Summarize completed cases while stress tests are still running."""

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    rows = (
        [json.loads(line) for line in args.results.read_text().splitlines()]
        if args.results.exists()
        else []
    )
    for case in sorted({row["case"] for row in rows}):
        completed = [row for row in rows if row["case"] == case]
        print(
            f"{case}: n={len(completed)}, passed={sum(row['passed'] for row in completed)}, seconds={sum(row['seconds'] for row in completed):.1f}"
        )
        for row in completed:
            if row["error"]:
                print(f"  {row['error']}")
    print(f"Completed: {len(rows)}")


if __name__ == "__main__":
    main()
