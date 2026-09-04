#!/usr/bin/env python3
"""
Idempotently create a Linear label matching a release tag, under a label group.

Used by .github/workflows/release-process-cut-tag.yml after a non-prerelease
tag is cut, to add the new release to Linear's release label group so projects
and feature requests can be tagged with an expected ETA.

On error the script exits 1 to make the failure visible in the workflow
UI; the calling job sets continue-on-error: true so a Linear outage still
cannot block the release cut.

Usage:
  python3 create_linear_release_label.py --tag <tag>

Environment variables:
  INGESTION_LINEAR_KEY   Linear API key (required; if missing, script no-ops)
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

from release_variables import LINEAR_RELEASE_GROUP

LINEAR_GRAPHQL_URL = "https://api.linear.app/graphql"
TIMEOUT_SECONDS = 15


def graphql(api_key: str, query: str, variables: dict) -> dict:
    """POST a GraphQL request to Linear; raise on transport or GraphQL errors."""
    body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    req = urllib.request.Request(
        LINEAR_GRAPHQL_URL,
        data=body,
        headers={
            "Authorization": api_key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("errors"):
        raise RuntimeError(f"Linear GraphQL errors: {payload['errors']}")
    return payload["data"]


def find_group_id(api_key: str, group_name: str) -> str | None:
    query = """
        query($name: String!) {
          issueLabels(filter: { name: { eq: $name }, isGroup: { eq: true } }) {
            nodes { id name }
          }
        }
    """
    data = graphql(api_key, query, {"name": group_name})
    nodes = data["issueLabels"]["nodes"]
    return nodes[0]["id"] if nodes else None


def find_existing_label_id(api_key: str, name: str, parent_id: str) -> str | None:
    query = """
        query($name: String!, $parent: ID!) {
          issueLabels(filter: { name: { eq: $name }, parent: { id: { eq: $parent } } }) {
            nodes { id name }
          }
        }
    """
    data = graphql(api_key, query, {"name": name, "parent": parent_id})
    nodes = data["issueLabels"]["nodes"]
    return nodes[0]["id"] if nodes else None


def create_label(api_key: str, name: str, parent_id: str, description: str) -> str:
    query = """
        mutation($name: String!, $parent: String!, $description: String) {
          issueLabelCreate(input: { name: $name, parentId: $parent, description: $description }) {
            success
            issueLabel { id name }
          }
        }
    """
    data = graphql(
        api_key,
        query,
        {"name": name, "parent": parent_id, "description": description},
    )
    result = data["issueLabelCreate"]
    if not result.get("success") or not result.get("issueLabel"):
        raise RuntimeError(f"issueLabelCreate did not succeed: {result}")
    return result["issueLabel"]["id"]


def run(tag: str, group_name: str) -> int:
    api_key = os.environ.get("INGESTION_LINEAR_KEY", "").strip()
    if not api_key:
        print("INGESTION_LINEAR_KEY not set, skipping Linear label creation")
        return 0

    group_id = find_group_id(api_key, group_name)
    if not group_id:
        print(f"Could not find Linear label group '{group_name}'", file=sys.stderr)
        return 1
    print(f"Found '{group_name}' group: {group_id}")

    existing_id = find_existing_label_id(api_key, tag, group_id)
    if existing_id:
        print(f"Label '{tag}' already exists under '{group_name}' (id: {existing_id}), nothing to do")
        return 0

    new_id = create_label(
        api_key,
        name=tag,
        parent_id=group_id,
        description=f"Released in {tag}",
    )
    print(f"Created Linear label '{tag}' (id: {new_id}) under '{group_name}'")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="Release tag, used as the label name")
    parser.add_argument("--group", default=LINEAR_RELEASE_GROUP, help=f"Linear label group name (default: {LINEAR_RELEASE_GROUP!r})")
    args = parser.parse_args()

    try:
        return run(args.tag, args.group)
    except (urllib.error.URLError, RuntimeError, KeyError, json.JSONDecodeError) as exc:
        # Surface the failure (job marked failed) but the workflow's
        # continue-on-error keeps the release cut from being blocked.
        print(f"Linear label creation failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
