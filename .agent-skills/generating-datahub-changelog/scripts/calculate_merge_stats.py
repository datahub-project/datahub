#!/usr/bin/env python3
"""
Calculate PR merge time statistics from GitHub PR JSON data.

Usage:
    # From gh pr list JSON output:
    gh pr list --repo datahub-project/datahub --state merged --json number,createdAt,mergedAt,reviews | python calculate_merge_stats.py

    # Or with a file:
    python calculate_merge_stats.py < prs.json

    # Or with inline JSON:
    echo '[{"number": 123, "createdAt": "2025-01-01T00:00:00Z", "mergedAt": "2025-01-02T00:00:00Z", "reviews": []}]' | python calculate_merge_stats.py

Output:
    Median: X.X days/weeks
    Average: Y.Y days/weeks
    Top Reviewers: @user1 (N), @user2 (N), @user3 (N)
"""

import json
import sys
from datetime import datetime
from collections import Counter


def humanize_hours(hours: float) -> str:
    """Convert hours to human-friendly format per style guide."""
    if hours < 48:
        return f"{hours:.1f} hours"
    elif hours < 336:  # 2 weeks
        return f"{hours/24:.1f} days"
    else:
        return f"{hours/168:.1f} weeks"


def calculate_merge_times(prs: list) -> tuple[float, float]:
    """Calculate median and average merge times in hours."""
    merge_hours = []

    for pr in prs:
        created = pr.get("createdAt")
        merged = pr.get("mergedAt")

        if not created or not merged:
            continue

        c = datetime.fromisoformat(created.replace("Z", "+00:00"))
        m = datetime.fromisoformat(merged.replace("Z", "+00:00"))
        hours = (m - c).total_seconds() / 3600
        merge_hours.append(hours)

    if not merge_hours:
        return 0.0, 0.0

    merge_hours.sort()
    n = len(merge_hours)

    # Calculate median
    if n % 2 == 0:
        median = (merge_hours[n//2 - 1] + merge_hours[n//2]) / 2
    else:
        median = merge_hours[n//2]

    # Calculate average
    avg = sum(merge_hours) / n

    return median, avg


def count_reviewers(prs: list, top_n: int = 3) -> list[tuple[str, int]]:
    """Count approved reviews per reviewer."""
    reviewer_counts = Counter()

    for pr in prs:
        reviews = pr.get("reviews", [])
        for review in reviews:
            if review.get("state") == "APPROVED":
                author = review.get("author", {})
                login = author.get("login") if isinstance(author, dict) else None
                if login:
                    reviewer_counts[login] += 1

    return reviewer_counts.most_common(top_n)


def main():
    # Read JSON from stdin
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON input - {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print("Error: Expected JSON array of PRs", file=sys.stderr)
        sys.exit(1)

    if not data:
        print("No PRs provided")
        sys.exit(0)

    # Calculate merge times
    median, avg = calculate_merge_times(data)

    # Count reviewers
    top_reviewers = count_reviewers(data)

    # Output results
    print(f"Median: {humanize_hours(median)}")
    print(f"Average: {humanize_hours(avg)}")

    if top_reviewers:
        reviewer_str = ", ".join(f"@{login} ({count})" for login, count in top_reviewers)
        print(f"Top Reviewers: {reviewer_str}")
    else:
        print("Top Reviewers: (no approved reviews found)")


if __name__ == "__main__":
    main()
