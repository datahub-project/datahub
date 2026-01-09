#!/usr/bin/env python3
"""
Fetch ChatbotInteraction events from DataHub's OpenSearch index.

This script retrieves chatbot conversation history from a DataHub deployment,
including full conversation history with tool calls and reasoning.

Usage:
    # Fetch last 7 days of events
    python fetch_chatbot_events.py

    # Fetch specific date range
    python fetch_chatbot_events.py --start-date 2024-01-01 --end-date 2024-01-31

    # Fetch events with errors only
    python fetch_chatbot_events.py --errors-only

    # Search for specific terms in message_contents
    python fetch_chatbot_events.py --search "error problem issue"

    # Filter by chatbot source
    python fetch_chatbot_events.py --chatbot slack

    # Local grep through full_history (after fetching)
    python fetch_chatbot_events.py --grep "confidence.*low"

    # Custom output path
    python fetch_chatbot_events.py --output /path/to/output.jsonl

Environment:
    Reads credentials from ~/.datahubenv (YAML format):
        gms:
          server: https://your-instance.acryl.io/gms
          token: <your-token>

    Or from environment variables:
        DATAHUB_GMS_URL=https://your-instance.acryl.io/gms
        DATAHUB_GMS_TOKEN=<your-token>

API Details:
    Endpoint: POST {GMS_URL}/openapi/v2/analytics/datahub_usage_events/_search

    This queries the OpenSearch `datahub_usage_event` index directly, bypassing the
    `/openapi/v1/events/audit/search` endpoint which filters to backend events only.

    Example query sent:
        {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "ChatbotInteraction"}},
                        {"range": {"timestamp": {"gte": <start_ms>, "lte": <end_ms>}}},
                        {"query_string": {"query": "*term1* OR *term2*", "fields": ["message_contents", "response_contents"]}}
                    ],
                    "must_not": [
                        {"term": {"response_error.keyword": ""}}
                    ]
                }
            },
            "sort": [{"timestamp": "desc"}],
            "size": 100,
            "search_after": [<last_sort_values>]  # for pagination
        }

    Searchable fields:
        - message_contents: User's question (wildcard/query_string works)
        - response_contents: Bot's response (wildcard/query_string works)
        - response_error: Error message (exists query works)
        - chatbot: Source filter (term query: slack, teams, datahub_ui)
        - timestamp: Date range (range query, epoch ms)

    Non-searchable fields (too large, use --grep for local filtering):
        - full_history: Complete conversation JSON (20KB-200KB+)
        - reduction_sequence: Context reduction data
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterator, Optional

import requests
import yaml


def load_credentials() -> tuple[str, str]:
    """Load DataHub credentials from ~/.datahubenv or environment variables.

    Returns:
        Tuple of (server_url, token)
    """
    # Try environment variables first
    server = os.environ.get("DATAHUB_GMS_URL")
    token = os.environ.get("DATAHUB_GMS_TOKEN")

    if server and token:
        return server, token

    # Try ~/.datahubenv
    datahubenv_path = Path.home() / ".datahubenv"
    if datahubenv_path.exists():
        with open(datahubenv_path) as f:
            config = yaml.safe_load(f)

        gms_config = config.get("gms", {})
        server = gms_config.get("server")
        token = gms_config.get("token")

        if server and token:
            return server, token

    raise ValueError(
        "DataHub credentials not found. Set DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN "
        "environment variables, or create ~/.datahubenv with gms.server and gms.token"
    )


def build_query(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    errors_only: bool = False,
    search_terms: Optional[str] = None,
    chatbot: Optional[str] = None,
) -> dict[str, Any]:
    """Build OpenSearch query for ChatbotInteraction events.

    Args:
        start_time: Start of time range (default: 7 days ago)
        end_time: End of time range (default: now)
        errors_only: Only return events with response_error set
        search_terms: Space-separated terms to search in message_contents
        chatbot: Filter by chatbot source (slack, teams, datahub_ui)

    Returns:
        OpenSearch query dict
    """
    # Default time range: last 7 days
    if end_time is None:
        end_time = datetime.now()
    if start_time is None:
        start_time = end_time - timedelta(days=7)

    # Convert to epoch milliseconds
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    # Build must clauses
    must_clauses: list[dict[str, Any]] = [
        {"term": {"type": "ChatbotInteraction"}},
        {"range": {"timestamp": {"gte": start_ms, "lte": end_ms}}},
    ]

    # Add chatbot filter
    if chatbot:
        must_clauses.append({"term": {"chatbot": chatbot}})

    # Add search terms (wildcard search on message_contents)
    if search_terms:
        # Convert space-separated terms to OR query with wildcards
        terms = search_terms.split()
        wildcard_patterns = " OR ".join(f"*{term}*" for term in terms)
        must_clauses.append(
            {
                "query_string": {
                    "query": wildcard_patterns,
                    "fields": ["message_contents", "response_contents"],
                }
            }
        )

    # Build must_not clauses
    must_not_clauses: list[dict[str, Any]] = []

    # Handle errors_only filter
    if errors_only:
        must_clauses.append({"exists": {"field": "response_error"}})
        must_not_clauses.append({"term": {"response_error.keyword": ""}})

    query: dict[str, Any] = {
        "query": {
            "bool": {
                "must": must_clauses,
            }
        },
        "sort": [{"timestamp": "desc"}],
        "size": 100,  # Fetch in batches of 100
    }

    if must_not_clauses:
        query["query"]["bool"]["must_not"] = must_not_clauses

    return query


def fetch_events(
    server: str,
    token: str,
    query: dict[str, Any],
) -> Iterator[dict[str, Any]]:
    """Fetch events from OpenSearch with pagination.

    Args:
        server: DataHub GMS server URL
        token: DataHub auth token
        query: OpenSearch query dict

    Yields:
        Individual event documents
    """
    url = f"{server.rstrip('/')}/openapi/v2/analytics/datahub_usage_events/_search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    total_fetched = 0
    search_after = None

    while True:
        # Add search_after for pagination
        current_query = query.copy()
        if search_after:
            current_query["search_after"] = search_after

        response = requests.post(url, headers=headers, json=current_query, timeout=60)
        response.raise_for_status()

        result = response.json()
        hits = result.get("hits", {}).get("hits", [])

        if not hits:
            break

        for hit in hits:
            yield hit["_source"]
            total_fetched += 1

        # Get search_after from last hit for pagination
        search_after = hits[-1].get("sort")

        # Log progress
        total = result.get("hits", {}).get("total", {}).get("value", "?")
        print(f"Fetched {total_fetched}/{total} events...", file=sys.stderr)

        # Safety limit
        if total_fetched >= 10000:
            print("Warning: Reached 10,000 event limit", file=sys.stderr)
            break


def grep_events(
    events: list[dict[str, Any]],
    pattern: str,
) -> list[dict[str, Any]]:
    """Filter events by regex pattern in full_history field.

    Args:
        events: List of event documents
        pattern: Regex pattern to match

    Returns:
        Filtered list of events matching the pattern
    """
    regex = re.compile(pattern, re.IGNORECASE)
    matched = []

    for event in events:
        full_history = event.get("full_history", "")
        message_contents = event.get("message_contents", "")
        response_contents = event.get("response_contents", "")
        response_error = event.get("response_error", "")

        # Search across all relevant text fields
        searchable_text = (
            f"{full_history} {message_contents} {response_contents} {response_error}"
        )

        if regex.search(searchable_text):
            matched.append(event)

    return matched


def format_event_summary(event: dict[str, Any]) -> str:
    """Format a single event for human-readable display.

    Args:
        event: Event document

    Returns:
        Formatted string summary
    """
    timestamp = event.get("timestamp", 0)
    dt = datetime.fromtimestamp(timestamp / 1000) if timestamp else None
    date_str = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "unknown"

    user = (
        event.get("slack_user_name")
        or event.get("teams_user_name")
        or event.get("ui_user_urn")
        or "unknown"
    )
    chatbot = event.get("chatbot", "unknown")
    message = event.get("message_contents", "")[:100]
    error = event.get("response_error")

    lines = [
        f"[{date_str}] {chatbot} - {user}",
        f"  Message: {message}...",
    ]

    if error:
        lines.append(f"  ERROR: {error[:100]}...")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch ChatbotInteraction events from DataHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Time range options
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD format). Default: 7 days ago",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD format). Default: today",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Number of days to look back (default: 7). Ignored if --start-date is set",
    )

    # Filter options
    parser.add_argument(
        "--errors-only",
        action="store_true",
        help="Only fetch events with response_error set",
    )
    parser.add_argument(
        "--search",
        type=str,
        help="Search terms to match in message_contents (space-separated, OR logic)",
    )
    parser.add_argument(
        "--chatbot",
        type=str,
        choices=["slack", "teams", "datahub_ui"],
        help="Filter by chatbot source",
    )

    # Post-processing options
    parser.add_argument(
        "--grep",
        type=str,
        help="Regex pattern to filter events locally (searches full_history, message, response)",
    )

    # Output options
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path. Default: ~/Downloads/chatbot_events_YYYY-MM-DD.jsonl",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["jsonl", "summary"],
        default="jsonl",
        help="Output format: jsonl (full data) or summary (human-readable). Default: jsonl",
    )
    parser.add_argument(
        "--no-full-history",
        action="store_true",
        help="Exclude full_history field from output (reduces file size)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print the OpenSearch query being sent",
    )

    args = parser.parse_args()

    # Parse dates
    end_time = datetime.now()
    if args.end_date:
        end_time = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        )

    if args.start_date:
        start_time = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_time = end_time - timedelta(days=args.days_back)

    # Load credentials
    try:
        server, token = load_credentials()
        print(f"Using DataHub server: {server}", file=sys.stderr)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Build query
    query = build_query(
        start_time=start_time,
        end_time=end_time,
        errors_only=args.errors_only,
        search_terms=args.search,
        chatbot=args.chatbot,
    )

    print(
        f"Fetching events from {start_time.date()} to {end_time.date()}...",
        file=sys.stderr,
    )

    if args.verbose:
        print(f"\nOpenSearch query:", file=sys.stderr)
        print(json.dumps(query, indent=2), file=sys.stderr)
        print(file=sys.stderr)

    # Fetch events
    try:
        events = list(fetch_events(server, token, query))
    except requests.exceptions.RequestException as e:
        print(f"Error fetching events: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Fetched {len(events)} events", file=sys.stderr)

    # Apply local grep filter
    if args.grep:
        events = grep_events(events, args.grep)
        print(f"After grep filter: {len(events)} events", file=sys.stderr)

    # Remove full_history if requested
    if args.no_full_history:
        for event in events:
            event.pop("full_history", None)
            event.pop("reduction_sequence", None)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        date_str = datetime.now().strftime("%Y-%m-%d")
        output_path = Path.home() / "Downloads" / f"chatbot_events_{date_str}.jsonl"

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "summary":
        with open(output_path, "w") as f:
            for event in events:
                f.write(format_event_summary(event))
                f.write("\n\n")
    else:
        with open(output_path, "w") as f:
            for event in events:
                f.write(json.dumps(event))
                f.write("\n")

    print(f"Output written to: {output_path}", file=sys.stderr)
    print(f"File size: {output_path.stat().st_size / 1024:.1f} KB", file=sys.stderr)

    # Print summary stats
    if events:
        error_count = sum(1 for e in events if e.get("response_error"))
        chatbots = {}
        for e in events:
            cb = e.get("chatbot", "unknown")
            chatbots[cb] = chatbots.get(cb, 0) + 1

        print(f"\nSummary:", file=sys.stderr)
        print(f"  Total events: {len(events)}", file=sys.stderr)
        print(f"  Events with errors: {error_count}", file=sys.stderr)
        print(f"  By chatbot: {chatbots}", file=sys.stderr)


if __name__ == "__main__":
    main()
