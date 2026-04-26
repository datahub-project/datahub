from __future__ import annotations

import importlib.util
import importlib
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import requests

LINEAR_GRAPHQL = "https://api.linear.app/graphql"

# Baked-in: image repo basename (e.g. datahub-gms) → Acryl Linear label id.
DEFAULT_LINEAR_REPO_LABEL_MAP: dict[str, str] = {
    "datahub-actions": "75489fcb-ab53-4087-a764-cd699db9c32a",
    "datahub-executor": "976d804a-217c-421e-a34c-ab8d2c9748d5",
    "datahub-frontend-react": "f643839d-83c3-41a8-bb8e-c28ffb36a643",
    "datahub-gms": "b9f7f5f9-bfce-49bc-befd-089933bfc0d6",
    "datahub-integrations-service": "6c93348f-bf52-4ccb-a01b-7c461871ea57",
    "datahub-mae-consumer": "632fb146-90ea-4683-88b4-624f86f45f61",
    "datahub-mce-consumer": "5b8941e9-0df6-44e6-b4ab-bb8dece4a1e1",
    "datahub-upgrade": "4c4f0d98-4921-4f02-b432-6823c3fdbff7",
}


def dedupe_preserve_order(ids: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in ids:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def unique_repo_basenames_from_occurrences(
    occurrences: list[tuple[str, str, str, dict[str, Any]]],
) -> list[str]:
    basenames: list[str] = []
    seen: set[str] = set()
    for artifact_ref, _, _, _ in occurrences:
        base, _tag = split_image_ref(artifact_ref)
        b = (base or "").strip()
        if b and b not in seen:
            seen.add(b)
            basenames.append(b)
    return basenames


def split_image_ref(target: str) -> tuple[str, str]:
    try:
        module = importlib.import_module("utils.security_scan_utils")
        return module.split_image_ref(target)
    except ModuleNotFoundError:
        module_path = Path(__file__).resolve().parent / "security_scan_utils.py"
        spec = importlib.util.spec_from_file_location("security_scan_utils", module_path)
        if not spec or not spec.loader:
            raise RuntimeError("Unable to load security_scan_utils module")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.split_image_ref(target)


def linear_priority_for_scan_severity(severity_upper: str | None) -> int | None:
    """Map scanner severities to Linear numeric priority."""
    if not severity_upper:
        return None
    s = severity_upper.strip().upper()
    if s == "CRITICAL":
        return 1
    if s == "HIGH":
        return 2
    return None


def linear_due_date_for_scan_severity(severity_upper: str | None) -> str | None:
    """Linear IssueCreateInput.dueDate (UTC YYYY-MM-DD)."""
    if not severity_upper:
        return None
    s = severity_upper.strip().upper()
    if s == "CRITICAL":
        days = 16
    elif s == "HIGH":
        days = 31
    else:
        return None
    day = datetime.now(timezone.utc).date() + timedelta(days=days)
    return day.isoformat()


def graphql(
    api_key: str, query: str, variables: dict[str, Any] | None = None
) -> dict[str, Any]:
    payload: dict[str, Any] = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    try:
        resp = requests.post(
            LINEAR_GRAPHQL,
            json=payload,
            headers={
                "Authorization": api_key,
                "Content-Type": "application/json",
            },
            timeout=120,
        )
        resp.raise_for_status()
        body = resp.json()
    except requests.HTTPError as e:
        err_body = e.response.text if e.response is not None else ""
        code = e.response.status_code if e.response is not None else "?"
        raise RuntimeError(f"Linear HTTP {code}: {err_body}") from e
    if body.get("errors"):
        raise RuntimeError(f"Linear GraphQL errors: {body['errors']}")
    return body.get("data") or {}


def resolve_linear_repo_label_map() -> dict[str, str]:
    return dict(DEFAULT_LINEAR_REPO_LABEL_MAP)


def repo_label_ids_for_occurrences(
    repo_map: dict[str, str],
    occurrences: list[tuple[str, str, str, dict[str, Any]]],
) -> list[str]:
    if not repo_map:
        return []
    out: list[str] = []
    for name in unique_repo_basenames_from_occurrences(occurrences):
        lid = repo_map.get(name)
        if lid:
            out.append(lid)
    return out


def issue_graphql_label_ids(api_key: str, issue_id: str) -> list[str]:
    q = """
query IssueLabelIds($id: String!) {
  issue(id: $id) {
    labels { nodes { id } }
  }
}
"""
    data = graphql(api_key, q, {"id": issue_id})
    issue = data.get("issue")
    if not issue:
        return []
    return [
        str(n["id"])
        for n in (issue.get("labels") or {}).get("nodes") or []
        if n.get("id")
    ]


def issue_update_label_ids(api_key: str, issue_id: str, label_ids: list[str]) -> None:
    m = """
mutation IssueUpdateLabelIds($id: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $id, input: $input) {
    success
  }
}
"""
    u = dedupe_preserve_order(label_ids)
    data = graphql(api_key, m, {"id": issue_id, "input": {"labelIds": u}})
    if not (data.get("issueUpdate") or {}).get("success"):
        raise RuntimeError(f"issueUpdate labelIds failed: {data}")


def random_label_color_hex() -> str:
    return f"#{random.randint(0, 0xFFFFFF):06x}"


def find_team_label_by_name(api_key: str, team_id: str, label_name: str) -> str | None:
    q = """
query TeamIssueLabelByName($teamId: ID!, $name: String!) {
  issueLabels(
    filter: {
      team: { id: { eq: $teamId } }
      name: { eq: $name }
    }
    first: 1
  ) {
    nodes { id name }
  }
}
"""
    data = graphql(api_key, q, {"teamId": team_id, "name": label_name})
    nodes = (data.get("issueLabels") or {}).get("nodes") or []
    if not nodes:
        return None
    found = nodes[0].get("id")
    return str(found) if found else None


def create_team_label(
    api_key: str, team_id: str, label_name: str, color_hex: str
) -> str:
    m = """
mutation IssueLabelCreate($input: IssueLabelCreateInput!) {
  issueLabelCreate(input: $input) {
    success
    issueLabel { id name color }
  }
}
"""
    data = graphql(
        api_key,
        m,
        {
            "input": {
                "teamId": team_id,
                "name": label_name,
                "color": color_hex,
            }
        },
    )
    result = data.get("issueLabelCreate") or {}
    if not result.get("success"):
        raise RuntimeError(f"issueLabelCreate failed: {data}")
    issue_label = result.get("issueLabel") or {}
    lid = issue_label.get("id")
    if not lid:
        raise RuntimeError(f"issueLabelCreate returned no id: {data}")
    return str(lid)


def get_or_create_team_label_id(api_key: str, team_id: str, label_name: str) -> str:
    existing = find_team_label_by_name(api_key, team_id, label_name)
    if existing:
        return existing
    try:
        return create_team_label(api_key, team_id, label_name, random_label_color_hex())
    except RuntimeError as e:
        em = str(e).lower()
        if any(
            x in em for x in ("existing", "already", "duplicate", " unique", "constraint")
        ):
            existing_after_race = find_team_label_by_name(api_key, team_id, label_name)
            if existing_after_race:
                return existing_after_race
        raise


def request_file_upload(
    api_key: str, filename: str, content_type: str, size: int
) -> tuple[str, str, dict[str, str]]:
    m = """
mutation FileUpload($filename: String!, $contentType: String!, $size: Int!) {
  fileUpload(filename: $filename, contentType: $contentType, size: $size) {
    success
    uploadFile {
      uploadUrl
      assetUrl
      headers {
        key
        value
      }
    }
  }
}
"""
    data = graphql(
        api_key,
        m,
        {
            "filename": filename,
            "contentType": content_type,
            "size": int(size),
        },
    )
    result = data.get("fileUpload") or {}
    if not result.get("success"):
        raise RuntimeError(f"fileUpload failed: {data}")
    upload_file = result.get("uploadFile") or {}
    upload_url = str(upload_file.get("uploadUrl") or "").strip()
    asset_url = str(upload_file.get("assetUrl") or "").strip()
    if not upload_url or not asset_url:
        raise RuntimeError(f"fileUpload returned missing URLs: {data}")
    hdrs: dict[str, str] = {}
    for h in upload_file.get("headers") or []:
        key = str(h.get("key") or "").strip()
        val = str(h.get("value") or "").strip()
        if key:
            hdrs[key] = val
    return upload_url, asset_url, hdrs


def upload_file_to_signed_url(
    upload_url: str, upload_headers: dict[str, str], payload: bytes
) -> None:
    resp = requests.put(upload_url, headers=upload_headers, data=payload, timeout=300)
    resp.raise_for_status()


def create_issue_attachment(api_key: str, issue_id: str, title: str, url: str) -> str:
    m = """
mutation AttachmentCreate($input: AttachmentCreateInput!) {
  attachmentCreate(input: $input) {
    success
    attachment {
      id
    }
  }
}
"""
    data = graphql(
        api_key,
        m,
        {
            "input": {
                "issueId": issue_id,
                "title": title,
                "url": url,
            }
        },
    )
    result = data.get("attachmentCreate") or {}
    if not result.get("success"):
        raise RuntimeError(f"attachmentCreate failed: {data}")
    attachment = result.get("attachment") or {}
    attachment_id = str(attachment.get("id") or "").strip()
    if not attachment_id:
        raise RuntimeError(f"attachmentCreate returned no id: {data}")
    return attachment_id


def attach_file_to_issue(api_key: str, issue_id: str, file_path: Path, title: str) -> str:
    payload = file_path.read_bytes()
    content_type = "application/json" if file_path.suffix.lower() == ".json" else "application/octet-stream"
    upload_url, asset_url, upload_headers = request_file_upload(
        api_key=api_key,
        filename=file_path.name,
        content_type=content_type,
        size=len(payload),
    )
    upload_file_to_signed_url(upload_url, upload_headers, payload)
    return create_issue_attachment(api_key, issue_id, title=title, url=asset_url)


def find_issue_by_title(api_key: str, team_id: str, title: str) -> str | None:
    q = """
query IssuesByTitle($teamId: ID!, $title: String!) {
  issues(
    filter: { team: { id: { eq: $teamId } }, title: { eq: $title } }
    first: 5
  ) {
    nodes { id identifier title }
  }
}
"""
    data = graphql(api_key, q, {"teamId": team_id, "title": title})
    nodes = (data.get("issues") or {}).get("nodes") or []
    if not nodes:
        return None
    return str(nodes[0]["id"])


def resolve_issue_create_state_id(api_key: str, team_id: str, explicit_state_id: str) -> str | None:
    if explicit_state_id:
        return explicit_state_id
    q = """
query TeamTriageIssueState($id: String!) {
  team(id: $id) {
    triageEnabled
    triageIssueState {
      id
      name
    }
  }
}
"""
    data = graphql(api_key, q, {"id": team_id})
    team = data.get("team") or {}
    if not team.get("triageEnabled"):
        return None
    triage_st = team.get("triageIssueState") or {}
    tid = triage_st.get("id")
    return str(tid) if tid else None


def create_issue(
    api_key: str,
    team_id: str,
    title: str,
    description: str,
    label_ids: list[str] | None,
    priority: int | None,
    state_id: str | None,
    due_date: str | None,
) -> str:
    m = """
mutation CreateIssue($input: IssueCreateInput!) {
  issueCreate(input: $input) {
    success
    issue { id identifier url }
  }
}
"""
    input_payload: dict[str, Any] = {
        "teamId": team_id,
        "title": title,
        "description": description,
    }
    if label_ids:
        input_payload["labelIds"] = label_ids
    if priority is not None:
        input_payload["priority"] = priority
    if state_id:
        input_payload["stateId"] = state_id
    if due_date:
        input_payload["dueDate"] = due_date
    data = graphql(api_key, m, {"input": input_payload})
    result = data.get("issueCreate") or {}
    if not result.get("success"):
        raise RuntimeError(f"issueCreate failed: {data}")
    issue = result.get("issue") or {}
    return str(issue["id"])


def link_issue_related(api_key: str, issue_id: str, related_issue_id: str) -> None:
    m = """
mutation IssueRelationCreate($input: IssueRelationCreateInput!) {
  issueRelationCreate(input: $input) {
    success
    issueRelation { id type }
  }
}
"""
    data = graphql(
        api_key,
        m,
        {
            "input": {
                "issueId": issue_id,
                "relatedIssueId": related_issue_id,
                "type": "related",
            }
        },
    )
    result = data.get("issueRelationCreate") or {}
    if not result.get("success"):
        raise RuntimeError(f"issueRelationCreate failed: {data}")


def link_issue_related_best_effort(api_key: str, issue_id: str, related_issue_id: str) -> str:
    try:
        link_issue_related(api_key, issue_id, related_issue_id)
    except RuntimeError as e:
        em = str(e).lower()
        if any(x in em for x in ("existing", "already", "duplicate", " unique", "constraint")):
            return ""
        return str(e)
    return ""


def get_marker_comment_id(
    api_key: str, issue_id: str, has_refs_anchor: Callable[[str], bool]
) -> tuple[str | None, str | None]:
    q = """
query IssueComments($issueId: String!) {
  issue(id: $issueId) {
    id
    comments {
      nodes {
        id
        body
      }
    }
  }
}
"""
    data = graphql(api_key, q, {"issueId": issue_id})
    issue = data.get("issue")
    if not issue:
        return None, None
    for node in (issue.get("comments") or {}).get("nodes") or []:
        body = node.get("body") or ""
        if has_refs_anchor(body):
            return str(node["id"]), body
    return None, None


def create_comment(api_key: str, issue_id: str, body: str) -> None:
    m = """
mutation CreateComment($input: CommentCreateInput!) {
  commentCreate(input: $input) {
    success
  }
}
"""
    data = graphql(api_key, m, {"input": {"issueId": issue_id, "body": body}})
    if not (data.get("commentCreate") or {}).get("success"):
        raise RuntimeError(f"commentCreate failed: {data}")


def update_comment(api_key: str, comment_id: str, body: str) -> None:
    m = """
mutation CommentUpdate($id: String!, $input: CommentUpdateInput!) {
  commentUpdate(id: $id, input: $input) {
    success
  }
}
"""
    data = graphql(api_key, m, {"id": comment_id, "input": {"body": body}})
    if not (data.get("commentUpdate") or {}).get("success"):
        raise RuntimeError(f"commentUpdate failed: {data}")
