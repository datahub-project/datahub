from __future__ import annotations

import importlib
import importlib.util
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, NamedTuple

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
    """Map scanner severities to Linear numeric priority (0=none; 1–4 = urgent→low)."""
    if not severity_upper:
        return None
    s = severity_upper.strip().upper()
    if s == "CRITICAL":
        return 1
    if s == "HIGH":
        return 2
    if s == "MEDIUM":
        return 3
    if s == "LOW":
        return 4
    return None


def linear_due_date_for_scan_severity(severity_upper: str | None) -> str | None:
    """Linear IssueCreateInput.dueDate (UTC); today + 15/35/180/360 days for critical/high/medium/low."""
    if not severity_upper:
        return None
    s = severity_upper.strip().upper()
    if s == "CRITICAL":
        days = 15
    elif s == "HIGH":
        days = 35
    elif s == "MEDIUM":
        days = 180
    elif s == "LOW":
        days = 360
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


class IssueDisplay(NamedTuple):
    """Linear issue fields useful for notifications (empty strings if absent)."""

    identifier: str
    url: str
    title: str


_ISSUE_DISPLAY_QUERY = """
query IssueDisplayById($id: String!) {
  issue(id: $id) {
    identifier
    url
    title
  }
}
"""


def get_issue_display(api_key: str, issue_id: str) -> IssueDisplay:
    """Return identifier, URL, and title for a Linear issue id."""
    data = graphql(api_key, _ISSUE_DISPLAY_QUERY, {"id": issue_id})
    issue = data.get("issue") or {}
    return IssueDisplay(
        identifier=str(issue.get("identifier") or ""),
        url=str(issue.get("url") or ""),
        title=str(issue.get("title") or ""),
    )


def get_issue_identifier_url(api_key: str, issue_id: str) -> tuple[str, str]:
    """Return ``(identifier, url)`` for a Linear issue id."""
    d = get_issue_display(api_key, issue_id)
    return (d.identifier, d.url)


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


class IssueLabelRef(NamedTuple):
    """A label on an issue and the label group it belongs to (``None`` when ungrouped)."""

    id: str
    parent_id: str | None


def issue_labels_with_parents(api_key: str, issue_id: str) -> list[IssueLabelRef]:
    q = """
query IssueLabelsWithParents($id: String!) {
  issue(id: $id) {
    labels { nodes { id parent { id } } }
  }
}
"""
    data = graphql(api_key, q, {"id": issue_id})
    issue = data.get("issue")
    if not issue:
        return []
    out: list[IssueLabelRef] = []
    for n in (issue.get("labels") or {}).get("nodes") or []:
        if not n.get("id"):
            continue
        parent = n.get("parent") or {}
        pid = parent.get("id")
        out.append(IssueLabelRef(str(n["id"]), str(pid) if pid else None))
    return out


def label_ids_replacing_group_sibling(
    current: list[IssueLabelRef], new_label_id: str, group_id: str | None
) -> list[str]:
    """Current label ids with any other child of ``group_id`` dropped and ``new_label_id`` added.

    Linear label groups are exclusive: an issue may carry only one child per group, and
    ``issueUpdate`` rejects a set that contains two. The scan-history comment keeps the full
    list of refs, so replacing the previous child is lossless.

    ``group_id`` is the parent of the label being applied. ``None`` means that label is
    ungrouped, so every current label stays and the new id is added.
    """
    if group_id is None:
        return dedupe_preserve_order([*(ref.id for ref in current), new_label_id])
    kept = [
        ref.id
        for ref in current
        if ref.id == new_label_id or ref.parent_id != group_id
    ]
    return dedupe_preserve_order([*kept, new_label_id])


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


def is_duplicate_label_error(err: BaseException) -> bool:
    em = str(err).lower()
    return any(
        x in em for x in ("existing", "already", "duplicate", " unique", "constraint")
    )


def find_label_group_id(
    api_key: str, group_name: str, team_id: str | None = None
) -> str | None:
    """Id of the label group ``group_name``: workspace-level, or on ``team_id`` when given."""
    if team_id:
        q = """
query TeamLabelGroupByName($name: String!, $teamId: ID!) {
  issueLabels(
    filter: { name: { eq: $name }, isGroup: { eq: true }, team: { id: { eq: $teamId } } }
    first: 1
  ) {
    nodes { id }
  }
}
"""
        variables: dict[str, Any] = {"name": group_name, "teamId": team_id}
    else:
        q = """
query WorkspaceLabelGroupByName($name: String!) {
  issueLabels(
    filter: { name: { eq: $name }, isGroup: { eq: true }, team: { null: true } }
    first: 1
  ) {
    nodes { id }
  }
}
"""
        variables = {"name": group_name}
    data = graphql(api_key, q, variables)
    nodes = (data.get("issueLabels") or {}).get("nodes") or []
    if not nodes:
        return None
    found = nodes[0].get("id")
    return str(found) if found else None


class ResolvedLabel(NamedTuple):
    """A label and the group it actually belongs to (``None`` when ungrouped)."""

    id: str
    parent_id: str | None


def find_label_id_by_name(api_key: str, label_name: str) -> ResolvedLabel | None:
    """Any issue label with this name, regardless of parent or team.

    Linear label names are unique in the workspace, so a name taken outside the group we
    intended still identifies the label to reuse. The parent is that label's real group.
    """
    q = """
query IssueLabelByName($name: String!) {
  issueLabels(filter: { name: { eq: $name } }, first: 1) {
    nodes { id parent { id } }
  }
}
"""
    data = graphql(api_key, q, {"name": label_name})
    nodes = (data.get("issueLabels") or {}).get("nodes") or []
    if not nodes or not nodes[0].get("id"):
        return None
    parent = nodes[0].get("parent") or {}
    parent_id = parent.get("id")
    return ResolvedLabel(str(nodes[0]["id"]), str(parent_id) if parent_id else None)


def find_group_child_label_id(api_key: str, group_id: str, label_name: str) -> str | None:
    q = """
query GroupChildLabelByName($name: String!, $parentId: ID!) {
  issueLabels(
    filter: { name: { eq: $name }, parent: { id: { eq: $parentId } } }
    first: 1
  ) {
    nodes { id }
  }
}
"""
    data = graphql(api_key, q, {"name": label_name, "parentId": group_id})
    nodes = (data.get("issueLabels") or {}).get("nodes") or []
    if not nodes:
        return None
    found = nodes[0].get("id")
    return str(found) if found else None


def _issue_label_create(api_key: str, label_input: dict[str, Any]) -> str:
    m = """
mutation IssueLabelCreate($input: IssueLabelCreateInput!) {
  issueLabelCreate(input: $input) {
    success
    issueLabel { id }
  }
}
"""
    data = graphql(api_key, m, {"input": label_input})
    result = data.get("issueLabelCreate") or {}
    if not result.get("success"):
        raise RuntimeError(f"issueLabelCreate failed: {data}")
    lid = (result.get("issueLabel") or {}).get("id")
    if not lid:
        raise RuntimeError(f"issueLabelCreate returned no id: {data}")
    return str(lid)


def create_label_group(api_key: str, group_name: str, team_id: str | None = None) -> str:
    label_input: dict[str, Any] = {"name": group_name, "isGroup": True}
    if team_id:
        label_input["teamId"] = team_id
    return _issue_label_create(api_key, label_input)


def create_group_child_label(
    api_key: str,
    group_id: str,
    label_name: str,
    color_hex: str,
    team_id: str | None = None,
) -> str:
    # A child of a team group must be created on that team; workspace groups take no teamId.
    label_input: dict[str, Any] = {
        "name": label_name,
        "parentId": group_id,
        "color": color_hex,
    }
    if team_id:
        label_input["teamId"] = team_id
    return _issue_label_create(api_key, label_input)


def get_or_create_label_group_id(
    api_key: str,
    group_name: str,
    team_id: str | None = None,
    *,
    create_if_missing: bool,
) -> str:
    existing = find_label_group_id(api_key, group_name, team_id)
    if existing:
        return existing
    if not create_if_missing:
        scope = f"team {team_id}" if team_id else "the workspace"
        raise RuntimeError(f"Linear label group {group_name!r} not found in {scope}")
    try:
        return create_label_group(api_key, group_name, team_id)
    except RuntimeError as e:
        if is_duplicate_label_error(e):
            existing_after = find_label_group_id(api_key, group_name, team_id)
            if existing_after:
                return existing_after
        raise


def _resolve_existing_child_label(
    api_key: str, group_id: str, label_name: str
) -> ResolvedLabel | None:
    """Child of ``group_id`` with this name, or the workspace label when the name is taken.

    A label found outside the group is reused as-is. It is not moved, so callers must use
    its real parent when replacing group siblings.
    """
    child_id = find_group_child_label_id(api_key, group_id, label_name)
    if child_id:
        return ResolvedLabel(child_id, group_id)
    found = find_label_id_by_name(api_key, label_name)
    if not found:
        return None
    if found.parent_id != group_id:
        print(
            f"Linear label {label_name!r} already exists ({found.id}) "
            f"outside group {group_id}; reusing it without moving it"
        )
    return found


def get_or_create_group_child_label_id(
    api_key: str, group_id: str, label_name: str, team_id: str | None = None
) -> ResolvedLabel:
    """Reuse or create ``label_name`` as a child of ``group_id``.

    Label names are unique in the workspace. A child of this group is preferred. If the name
    already exists outside the group, that label is reused and keeps its current parent.
    """
    existing = _resolve_existing_child_label(api_key, group_id, label_name)
    if existing:
        return existing
    try:
        created = create_group_child_label(
            api_key, group_id, label_name, random_label_color_hex(), team_id
        )
        return ResolvedLabel(created, group_id)
    except RuntimeError as e:
        if is_duplicate_label_error(e):
            existing_after = _resolve_existing_child_label(api_key, group_id, label_name)
            if existing_after:
                return existing_after
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


def _merge_gcs_put_headers(
    upload_headers: dict[str, str] | None, content_type: str
) -> dict[str, str]:
    """Ensure ``Content-Type`` is set; ``fileUpload`` may return empty ``headers`` (GCS PUT 400)."""
    out: dict[str, str] = {}
    for k, v in dict(upload_headers or {}).items():
        if k.lower() == "content-type" and not (str(v).strip() if v is not None else ""):
            continue
        out[k] = v
    if not any(name.lower() == "content-type" for name in out):
        out["Content-Type"] = content_type
    return out


def upload_file_to_signed_url(
    upload_url: str,
    upload_headers: dict[str, str] | None,
    payload: bytes,
    *,
    content_type: str,
) -> None:
    merged = _merge_gcs_put_headers(upload_headers, content_type)
    resp = requests.put(
        upload_url, headers=merged, data=payload, timeout=300
    )
    if not resp.ok:
        body = (resp.text or "")[:2000]
        raise RuntimeError(
            f"GCS upload failed: HTTP {resp.status_code} for {upload_url!r} — {body}"
        )


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
    upload_file_to_signed_url(
        upload_url, upload_headers, payload, content_type=content_type
    )
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
