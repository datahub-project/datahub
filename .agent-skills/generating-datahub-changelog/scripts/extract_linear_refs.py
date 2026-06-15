#!/usr/bin/env python3
"""
Extract Linear ticket references from GitHub PR JSON data.

Usage:
    # From gh pr list JSON output:
    gh pr list --repo datahub-project/datahub --state merged --json number,body | python extract_linear_refs.py

    # Or with a file:
    python extract_linear_refs.py < prs.json

Output (JSON):
    {
      "15859": ["ING-1372"],
      "15828": ["ING-1282", "OSS-942"],
      "15862": []
    }

Output (text, with --text flag):
    15859: ING-1372
    15828: ING-1282, OSS-942
    15862: (none)
"""

import json
import re
import sys

# Pattern to match Linear ticket references
LINEAR_PATTERN = re.compile(r'\b(ING|OSS|PLAT|CUS|PFP)-\d+\b')


def extract_refs(body: str) -> list[str]:
    """Extract unique Linear refs from text."""
    if not body:
        return []
    matches = LINEAR_PATTERN.findall(body)
    # findall returns the captured group, reconstruct full match
    full_matches = LINEAR_PATTERN.finditer(body)
    refs = sorted(set(match.group(0) for match in full_matches))
    return refs


def main():
    # Check for --text flag
    text_mode = '--text' in sys.argv

    # Read JSON from stdin
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON input - {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print("Error: Expected JSON array of PRs", file=sys.stderr)
        sys.exit(1)

    result = {}
    for pr in data:
        number = str(pr.get("number", "unknown"))
        body = pr.get("body", "")
        refs = extract_refs(body)
        result[number] = refs

    if text_mode:
        for number, refs in sorted(result.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0, reverse=True):
            if refs:
                print(f"{number}: {', '.join(refs)}")
            else:
                print(f"{number}: (none)")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
