#!/usr/bin/env python3
"""Sync container security scan reports into Linear issues.

Feature summary:
- Scanner input modes:
  - ``trivy``: parse Trivy JSON reports.
  - ``trivy_grype``: parse mixed Trivy + Grype JSON, normalize and merge by image+CVE.
- Grouping model:
  - Create one issue per ``(vulnerability id, image scope)`` where scope is repo basename plus
    optional variant suffix inferred from configured tag suffixes.
- Issue title/description generation:
  - Build deterministic Linear titles from CVE + package + scoped image.
  - Build rich markdown issue bodies with vulnerability details, affected images, scanner metadata,
    commit SHA, and workflow run URL.
- Raw scan report attachments on create:
  - For newly created issues, attach matching raw scanner JSON files (Trivy/Grype) when
    ``--raw-report-paths`` is passed once per raw file (repeat the flag).
  - Upload files via Linear signed upload URLs, then create issue attachments that reference the
    uploaded assets.
- Existing issue reconciliation:
  - Find issues by exact title.
  - Update refs comment and merge labels on existing issues without removing current labels.
- Labeling behavior:
  - Apply optional static labels from ``LINEAR_LABEL_IDS`` / ``TRIVY_LINEAR_LABEL_IDS``.
  - Apply component labels mapped from image repositories.
  - Apply a dynamic ref label using ``SCAN_REF_NAME`` (exact branch/tag text), team-scoped in
    Linear, reusing an existing label when present or creating one with a random color when absent.
- Refs comment tracking:
  - Maintain a single marker comment per issue with deduped branch/tag history for where the
    finding was observed.
- Severity-based prioritization:
  - For Trivy-derived findings, map worst severity to Linear priority and due date.
- Issue relation graph (new issues created in this run):
  - Build connected components where issues share CVE or package name, then create pairwise
    ``related`` links across each component (best-effort; duplicate-link errors are ignored).
- Team/state resolution:
  - Resolve team via ``LINEAR_TEAM_ID`` (or legacy fallback).
  - Resolve issue create state from explicit ``LINEAR_ISSUE_STATE_ID`` or team triage state.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import requests
from utils.linear_sync_utils import (
    attach_file_to_issue as _attach_file_to_issue_util,
    create_comment as _create_comment_util,
    create_issue as _create_issue_util,
    dedupe_preserve_order as _dedupe_preserve_order,
    find_issue_by_title as _find_issue_by_title_util,
    get_or_create_team_label_id as _get_or_create_team_label_id_util,
    get_marker_comment_id as _get_marker_comment_id_util,
    issue_graphql_label_ids as _issue_graphql_label_ids_util,
    issue_update_label_ids as _issue_update_label_ids_util,
    link_issue_related_best_effort as _link_issue_related_best_effort_util,
    linear_due_date_for_scan_severity as _linear_due_date_for_scan_severity,
    linear_priority_for_scan_severity as _linear_priority_for_scan_severity,
    repo_label_ids_for_occurrences as _repo_label_ids_for_occurrences_util,
    resolve_issue_create_state_id as _resolve_issue_create_state_id_from_linear_util,
    resolve_linear_repo_label_map as _resolve_linear_repo_label_map_util,
    update_comment as _update_comment_util,
    unique_repo_basenames_from_occurrences as _unique_repo_basenames_from_occurrences,
)
from utils.security_scan_utils import (
    advisory_title_for_description as _advisory_title_for_description_util,
    build_description as _build_description_util,
    comment_has_refs_anchor as _comment_has_refs_anchor_util,
    issue_pairs_cve_or_pkg as _issue_pairs_cve_or_pkg_util,
    linear_issue_title as _linear_issue_title_util,
    merge_refs_comment as _merge_refs_comment_util,
    parse_trivy_grype_merged,
    parse_trivy_reports,
    repo_scope_ticket_label as _repo_scope_ticket_label,
    trivy_pkg_name_for_title as _trivy_pkg_name_for_title_util,
    worst_trivy_severity as _worst_trivy_severity,
)

LINEAR_GRAPHQL = "https://api.linear.app/graphql"
MAX_TITLE_LEN = 250
REFS_SECTION_HEADER = "### Git branch/tag scan history"
REFS_SECTION_INTRO = (
    "CI records each **branch** or **tag** where this finding was reported (UTC timestamps). "
    "Refs are deduplicated by name."
)
LEGACY_REFS_HEADERS = (
    "### DataHub Trivy: scans by git branch/tag",
    "### DataHub security scans: git branch/tag",
)
LEGACY_REFS_MARKER = "<!-- datahub-trivy-refs -->"


@dataclass(frozen=True)
class ScanRef:
    kind: str  # "branch" | "tag"
    name: str

    @property
    def key(self) -> str:
        return f"{self.kind}:{self.name}"


# Trivy rows: (artifact_ref, result_target, class/type, vuln). artifact_ref = scanned image ref for scope.
GroupedRows = dict[str, list[tuple[str, str, str, dict[str, Any]]]]
ParserFn = Callable[[list[Path]], GroupedRows]


def _graphql(
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




# Baked-in: image repo basename (e.g. datahub-gms) → Acryl Linear label id. Edit in code to add/change.
_DEFAULT_LINEAR_REPO_LABEL_MAP: dict[str, str] = {
    "datahub-actions": "75489fcb-ab53-4087-a764-cd699db9c32a",
    "datahub-executor": "976d804a-217c-421e-a34c-ab8d2c9748d5",
    "datahub-frontend-react": "f643839d-83c3-41a8-bb8e-c28ffb36a643",
    "datahub-gms": "b9f7f5f9-bfce-49bc-befd-089933bfc0d6",
    "datahub-integrations-service": "6c93348f-bf52-4ccb-a01b-7c461871ea57",
    "datahub-mae-consumer": "632fb146-90ea-4683-88b4-624f86f45f61",
    "datahub-mce-consumer": "5b8941e9-0df6-44e6-b4ab-bb8dece4a1e1",
    "datahub-upgrade": "4c4f0d98-4921-4f02-b432-6823c3fdbff7",
}


def _resolve_linear_repo_label_map() -> dict[str, str]:
    """Map image repo basename to Linear label id (copy of ``_DEFAULT_LINEAR_REPO_LABEL_MAP``)."""
    return dict(_DEFAULT_LINEAR_REPO_LABEL_MAP)


def _repo_label_ids_for_occurrences(
    repo_map: dict[str, str],
    occurrences: list[tuple[str, str, str, dict[str, Any]]],
) -> list[str]:
    if not repo_map:
        return []
    out: list[str] = []
    for name in _unique_repo_basenames_from_occurrences(occurrences):
        lid = repo_map.get(name)
        if lid:
            out.append(lid)
    return out


def _issue_graphql_label_ids(api_key: str, issue_id: str) -> list[str]:
    q = """
query IssueLabelIds($id: String!) {
  issue(id: $id) {
    labels { nodes { id } }
  }
}
"""
    data = _graphql(api_key, q, {"id": issue_id})
    issue = data.get("issue")
    if not issue:
        return []
    return [
        str(n["id"])
        for n in (issue.get("labels") or {}).get("nodes") or []
        if n.get("id")
    ]


def _issue_update_label_ids(api_key: str, issue_id: str, label_ids: list[str]) -> None:
    m = """
mutation IssueUpdateLabelIds($id: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $id, input: $input) {
    success
  }
}
"""
    u = _dedupe_preserve_order(label_ids)
    data = _graphql(
        api_key, m, {"id": issue_id, "input": {"labelIds": u}}
    )
    if not (data.get("issueUpdate") or {}).get("success"):
        raise RuntimeError(f"issueUpdate labelIds failed: {data}")


# Names that read poorly in GFM tables in Linear (long text, JSON blobs, URLs).
_TRIVY_STACK_WHEN_KEY: frozenset[str] = frozenset(
    {
        "Title",
        "PrimaryURL",
        "Description",
        "PkgPath",
        "VendorSeverity",
        "CVSS",
        "Layer",
        "DataSource",
        "PkgIdentifier",
        "Fingerprint",
        "VendorIDs",
        "VendorIds",
        "Commit",
        "FilePath",
        "CweIDs",
        "SecondaryURLs",
    }
)
# Long free text in nested advisory records (Trivy varies by key spelling).
_ADVISORY_LONG_TEXT_KEYS = frozenset({"Description", "Details", "Body"})

# Trivy fields in the **Technical details** section (see ``_trivy_technical_supporting_sections``).
_TECHNICAL_FOLD_KEYS: frozenset[str] = frozenset(
    {"CVSS", "Layer", "DataSource", "PkgIdentifier"}
)

# Strip before serializing to Linear (not vendor fields).
_INTERNAL_VULN_KEYS: frozenset[str] = frozenset(
    {"_datahub_scanners", "_datahub_scanner_source"}
)


def _plain_scalar_for_cell(val: Any) -> str:
    """Single-line plaintext for table cells."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (dict, list)):
        compact = json.dumps(val, ensure_ascii=False, separators=(",", ":"))
        if len(compact) > 400:
            compact = compact[:397] + "..."
        return compact.replace("\n", " ")
    return str(val).strip().replace("\n", " ")


def _fmt_cell(val: Any) -> str:
    """Markdown pipe escaping (references section bullets)."""
    return _plain_scalar_for_cell(val).replace("|", "\\|")


def _markdown_table(header: tuple[str, str], rows: list[tuple[str, Any]]) -> str:
    """GFM pipe table — Linear renders these; column width is editor-controlled."""
    filtered = [(k, v) for k, v in rows if v is not None and v != ""]
    if not filtered:
        return ""
    lines = [
        f"| **{header[0]}** | **{header[1]}** |",
        "| :--- | :--- |",
    ]
    for key, val in filtered:
        lines.append(f"| {_fmt_cell(key)} | {_fmt_cell(val)} |")
    return "\n".join(lines) + "\n"


def _affected_result_target_class_table(detail_target: str, rclass: str) -> str:
    """One-row table: result target and class (Trivy ``Result`` target + type) side by side."""
    t = str(detail_target).strip() if detail_target is not None else ""
    c = str(rclass).strip() if rclass is not None else ""
    if not t and not c:
        return ""
    if not t:
        t = "—"
    if not c:
        c = "—"
    return _markdown_table(
        ("Result target", "Result class / type"),
        [(t, c)],
    )


def _trivy_technical_supporting_sections(technical: str, tail: str) -> str:
    """**Technical details** and **Supporting details** as separate Linear collapsible blocks.

    Linear makes sections toggleable with ``>>> `` (see editor docs) or ``/collapsible``; plain
    ``####`` headings are not collapsible. Two blocks are emitted as two top-level ``>>>`` sections
    (joined with a blank line) so each can expand/collapse on its own.
    """
    out: list[str] = []
    t = technical.strip() if technical else ""
    s = tail.strip() if tail else ""
    if t:
        out.append(">>> Technical details\n\n" + t + "\n")
    if s:
        out.append(">>> Supporting details\n\n" + s + "\n")
    if not out:
        return ""
    return "\n\n".join(out).rstrip() + "\n"


def _stacked_value_text(val: Any, *, max_len: int = 24_000) -> str:
    """Full text for stacked display; dict/list pretty-printed; strings keep newlines."""
    if isinstance(val, dict):
        raw = json.dumps(val, ensure_ascii=False, indent=2)
        return raw if len(raw) <= max_len else raw[: max_len - 3] + "..."
    if isinstance(val, list):
        raw = json.dumps(val, ensure_ascii=False, indent=2)
        return raw if len(raw) <= max_len else raw[: max_len - 3] + "..."
    if isinstance(val, bool):
        return "true" if val else "false"
    s = str(val).strip()
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


def _should_stack_field_row(field_name: str, val: Any) -> bool:
    if isinstance(val, (dict, list)):
        return True
    if field_name in _TRIVY_STACK_WHEN_KEY or field_name in _ADVISORY_LONG_TEXT_KEYS:
        return True
    if isinstance(val, str) and len(val.strip()) > 100:
        return True
    return False


def _layout_field_rows_ordered(rows: list[tuple[str, Any]]) -> str:
    """Compact table for short fields; full-width stacked blocks for long / JSON / known-wide keys."""
    chunks: list[str] = []
    table_buf: list[tuple[str, Any]] = []

    def flush_table() -> None:
        if table_buf:
            chunks.append(_markdown_table(("Field", "Value"), table_buf))
            table_buf.clear()

    for key, val in rows:
        if val is None or val == "":
            continue
        if _should_stack_field_row(key, val):
            flush_table()
            chunks.append(_stacked_field_blocks([(key, val)]).rstrip())
        else:
            table_buf.append((key, val))
    flush_table()
    return ("\n\n".join(chunks).strip() + "\n") if chunks else ""


def _stacked_block_primary_url_if_applicable(label: Any, val: Any) -> str | None:
    """GFM link for Trivy PrimaryURL (not code — links are not clickable inside fences)."""
    if str(label) != "PrimaryURL":
        return None
    u = str(val).strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return None
    lbl = _plain_scalar_for_cell(label)
    # Link text = URL (Linear renders as one clickable line).
    return f"**{lbl}**\n\n[{u}]({u})"


def _stacked_field_blocks(rows: list[tuple[str, Any]]) -> str:
    """Full-width labels + values (avoids skinny GFM columns in Linear)."""
    blocks: list[str] = []
    for label, val in rows:
        if val is None or val == "":
            continue
        primary = _stacked_block_primary_url_if_applicable(label, val)
        if primary:
            blocks.append(primary)
            continue
        txt = _stacked_value_text(val)
        lbl = _plain_scalar_for_cell(label)
        use_fence = isinstance(val, (dict, list)) or "\n" in txt or len(txt) > 100
        if use_fence:
            blocks.append(f"**{lbl}**\n\n```\n{txt}\n```")
        else:
            blocks.append(f"**{lbl}**\n\n`{txt}`")
    return "\n\n".join(blocks) + ("\n" if blocks else "")


def _serialize_trivy_vulnerability_record(vuln: dict[str, Any]) -> tuple[str, str, str]:
    """Returns ``(main_body, supporting_tail, technical_fold_markdown)``.

    *technical* is CVSS/Layer/DataSource/PkgIdentifier. *supporting_tail* is references, extra
    key/values, and (first) **Timing & classification** — PublishedDate, LastModifiedDate, CweIDs,
    VendorSeverity — before ``### References`` and ``### Additional fields``.
    """
    chunks: list[str] = []

    identity_rows: list[tuple[str, Any]] = []
    pkg_rows: list[tuple[str, Any]] = []
    meta_rows: list[tuple[str, Any]] = []
    technical_fold: list[tuple[str, Any]] = []
    tech_extras: list[tuple[str, Any]] = []
    refs_links: list[str] = []

    inner = vuln.get("Vulnerability")
    inner_tbl: list[tuple[str, Any]] = []
    if isinstance(inner, dict):
        for k, v in sorted(inner.items()):
            if k == "References" and isinstance(v, list):
                refs_links.extend(str(x) for x in v if x)
            elif k == "Title":
                # Same text as the issue body H1 from _advisory_title_for_description.
                continue
            elif k == "Description":
                # Shown as plain text under the CVE / summary at the top of the body.
                continue
            else:
                inner_tbl.append((k, v))

    ordered = (
        "VulnerabilityID",
        "PkgName",
        "PkgPath",
        "InstalledVersion",
        "FixedVersion",
        "Severity",
        "Title",
        "PrimaryURL",
        "References",
        "PublishedDate",
        "LastModifiedDate",
        "CweIDs",
        "VendorSeverity",
        "CVSS",
        "Layer",
        "FilePath",
        "DataSource",
        "PkgIdentifier",
    )
    seen: set[str] = set()
    for k in ordered:
        if k not in vuln or k == "Vulnerability":
            continue
        seen.add(k)
        v = vuln[k]
        if k == "References":
            if isinstance(v, list):
                refs_links.extend(str(x) for x in v if x)
            continue
        if k == "Title":
            # Duplicates the description H1 (advisory full title).
            continue
        if k in ("VulnerabilityID", "Severity", "PrimaryURL"):
            identity_rows.append((k, v))
        elif k in ("PkgName", "PkgPath", "InstalledVersion", "FixedVersion"):
            pkg_rows.append((k, v))
        elif k in ("PublishedDate", "LastModifiedDate", "CweIDs", "VendorSeverity"):
            meta_rows.append((k, v))
        elif k in _TECHNICAL_FOLD_KEYS:
            technical_fold.append((k, v))
        elif k == "FilePath":
            tech_extras.append((k, v))
        else:
            tech_extras.append((k, v))

    rest = {
        k: v
        for k, v in vuln.items()
        if k != "Vulnerability"
        and k not in seen
        and k != "Description"
        and k not in _INTERNAL_VULN_KEYS
    }

    if identity_rows:
        chunks.append("#### Identity & severity\n\n")
        chunks.append(_layout_field_rows_ordered(identity_rows))
    if pkg_rows:
        chunks.append("#### Package\n\n")
        chunks.append(_layout_field_rows_ordered(pkg_rows))
    if inner_tbl:
        chunks.append("#### Advisory / vulnerability record\n\n")
        chunks.append(_layout_field_rows_ordered(inner_tbl))
    if tech_extras:
        chunks.append("#### Other details\n\n")
        chunks.append(_layout_field_rows_ordered(tech_extras))

    technical_fold_str = (
        _layout_field_rows_ordered(technical_fold).strip() if technical_fold else ""
    )

    dedup_refs = list(dict.fromkeys(refs_links))
    tail_parts: list[str] = []
    if meta_rows:
        tail_parts.append(
            "#### Timing & classification\n\n"
            + _layout_field_rows_ordered(meta_rows).rstrip()
        )
    if dedup_refs:
        bullets = "\n".join(
            f"- [{u}]({u})" if u.startswith("http") else f"- `{_fmt_cell(u)}`"
            for u in dedup_refs
        )
        tail_parts.append("### References\n\n" + bullets)
    if rest:
        rest_body = _layout_field_rows_ordered(sorted(rest.items())).rstrip()
        if dedup_refs or meta_rows:
            tail_parts.append("### Additional fields\n\n" + rest_body)
        else:
            tail_parts.append(rest_body)

    if not tail_parts:
        tail_out = ""
    elif len(tail_parts) == 1 and not meta_rows and not dedup_refs and rest:
        # Prior shape: a single "additional fields" block with one ### heading in Supporting.
        tail_out = f"### Additional fields\n\n{tail_parts[0]}"
    else:
        tail_out = "\n\n".join(tail_parts)

    main = "\n".join(chunks).strip()
    return main, tail_out, technical_fold_str


def _raw_finding_table(vuln: dict[str, Any]) -> str:
    rows = [
        (k, v)
        for k, v in sorted(vuln.items(), key=lambda kv: kv[0])
        if k != "Title"
    ]
    return _layout_field_rows_ordered(rows)


SCANNERS: dict[str, ParserFn] = {
    "trivy": parse_trivy_reports,
    "trivy_grype": parse_trivy_grype_merged,
}


def _trivy_pkg_name_for_title(vuln: dict[str, Any]) -> str:
    """Installed package identifier for short issue titles (not CVE advisory prose)."""
    raw = ""
    pn = vuln.get("PkgName")
    if pn:
        raw = str(pn).strip()
    elif isinstance(vuln.get("PkgPath"), str) and vuln["PkgPath"].strip():
        raw = vuln["PkgPath"].strip().split("/")[-1]
    else:
        inner = vuln.get("Vulnerability")
        if isinstance(inner, dict) and inner.get("PkgName"):
            raw = str(inner["PkgName"]).strip()
    if not raw:
        return ""
    # Trivy adds ecosystem markers like "urllib3 (Python)"; strip trailing " (…)" for titles.
    return re.sub(r" \([^)]+\)$", "", raw).strip() or raw


def _linear_issue_title(
    vid: str,
    first_vuln: dict[str, Any],
    repo_scope_label: str,
    *,
    scanner: str,
) -> str:
    """`CVE-…: {package} (datahub-executor-slim)` for Trivy; scope is the unique image/variant id."""
    if scanner in ("trivy", "trivy_grype"):
        mid = _trivy_pkg_name_for_title(first_vuln).strip()
    else:
        ttitle = first_vuln.get("Title") or ""
        inner = first_vuln.get("Vulnerability")
        if isinstance(inner, dict) and inner.get("Title"):
            ttitle = str(inner["Title"])
        mid = (ttitle or "").strip()
    if not mid:
        mid = vid.strip()
    suffix = f" ({repo_scope_label})"
    max_inner = max(0, MAX_TITLE_LEN - len(suffix))
    inner_line = f"{vid}: {mid}"
    if len(inner_line) <= max_inner:
        return (inner_line + suffix)[:MAX_TITLE_LEN]
    prefix = f"{vid}: "
    budget = max_inner - len(prefix)
    if budget < 12:
        return (vid + suffix)[:MAX_TITLE_LEN]
    truncated = f"{prefix}{mid[: budget - 3]}..." + suffix
    return truncated[:MAX_TITLE_LEN]


def _advisory_title_for_description(
    scanner: str, first_vuln: dict[str, Any], fallback: str
) -> str:
    """Full vulnerability/advisory title from the report (H1 in issue body). Not the short Linear issue title."""
    ttitle = first_vuln.get("Title") or ""
    inner = first_vuln.get("Vulnerability")
    if isinstance(inner, dict) and inner.get("Title"):
        ttitle = str(inner["Title"])
    t = (ttitle or "").strip()
    return t if t else fallback


def _scanner_cell_from_occurrences(
    occurrences: list[tuple[str, str, str, dict[str, Any]]],
    fallback_label: str,
) -> str:
    """Table cell for **Scanner** — merged tool names from ``_datahub_scanners`` when present."""
    seen: set[str] = set()
    for *_, v in occurrences:
        for s in v.get("_datahub_scanners") or []:
            if s:
                seen.add(str(s).strip().lower())
    if seen:
        return "`" + ", ".join(sorted(seen)) + "`"
    return f"`{fallback_label}`"


def _trivy_advisory_description_plain(vuln: dict[str, Any]) -> str:
    """Advisory body text for the top of the issue (not code-fenced; same as nested ``Vulnerability.Description``)."""
    inner = vuln.get("Vulnerability")
    if isinstance(inner, dict) and inner.get("Description") is not None:
        t = str(inner["Description"]).strip()
        if t:
            return t
    d = vuln.get("Description")
    if d is not None and str(d).strip():
        return str(d).strip()
    return ""


def _build_description(
    scanner: str,
    vid: str,
    occurrences: list[tuple[str, str, str, dict[str, Any]]],
    run_url: str,
    scan_ref: ScanRef,
    commit_sha: str,
    description_heading: str,
    repo_scope_label: str,
) -> str:
    parts: list[str] = []
    parts.append(f"# {description_heading}\n")
    summary_rows: list[tuple[str, Any]] = [
        ("Vulnerability ID", f"`{vid}`"),
        ("Image scope (ticket id)", f"`{repo_scope_label}`"),
        ("Scanner", _scanner_cell_from_occurrences(occurrences, scanner)),
    ]
    parts.append(_markdown_table(("Property", "Value"), summary_rows))
    if scanner in ("trivy", "trivy_grype") and occurrences:
        desc_top = _trivy_advisory_description_plain(occurrences[0][3])
        if desc_top:
            parts.append("\n" + desc_top + "\n\n")

    seen: set[str] = set()
    occ_idx = 0
    occ_blocks: list[str] = []
    for artifact_ref, detail_target, rclass, vuln in occurrences:
        dedupe = f"{artifact_ref}\x1f{detail_target}"
        if dedupe in seen:
            continue
        seen.add(dedupe)
        occ_idx += 1
        stacked = _stacked_field_blocks([("Scanned artifact", artifact_ref)])
        result_table = _affected_result_target_class_table(detail_target, rclass)
        occ_top = stacked.rstrip() + (f"\n\n{result_table}" if result_table else "")
        occ_body: list[str] = [
            f"### Affected image #{occ_idx}\n",
            occ_top,
        ]
        if scanner in ("trivy", "trivy_grype"):
            main, tail, technical = _serialize_trivy_vulnerability_record(vuln)
            occ_body.append(main)
            ts = _trivy_technical_supporting_sections(technical, tail)
            if ts:
                occ_body.append(ts)
        else:
            occ_body.append("#### Raw finding\n\n")
            occ_body.append(_raw_finding_table(vuln))
        occ_blocks.append("\n".join(occ_body).strip())

    parts.append("\n\n---\n\n".join(occ_blocks))

    run_cell = f"[View workflow run]({run_url})" if run_url else "—"
    meta_inner = _markdown_table(
        ("Property", "Value"),
        [
            ("Git ref", f"`{scan_ref.key}`"),
            ("Commit", f"`{commit_sha}`"),
            ("Workflow run", run_cell),
        ],
    )
    # Scan / workflow line — keep after occurrence blocks; Technical/Supporting use Linear ``>>>`` there.
    scan_section = f"### Scan metadata\n\n{meta_inner.rstrip()}"
    parts.append("\n\n" + scan_section)
    return "\n".join(parts).strip()


def _parse_existing_ref_keys(comment_body: str) -> dict[str, str]:
    """Collect ref bullet lines anywhere in the comment (legacy or wrapped in <details>)."""
    keys: dict[str, str] = {}
    for line in comment_body.splitlines():
        line = line.strip()
        m = re.match(r"^[-*]\s*`(branch|tag):([^`]+)`", line)
        if m:
            k = f"{m.group(1)}:{m.group(2)}"
            keys[k] = line
    return keys


def _format_ref_line(
    scan_ref: ScanRef, short_sha: str, run_url: str, utc_now: str
) -> str:
    return (
        f"- `{scan_ref.key}` — SHA `{short_sha}` — "
        f"[workflow run]({run_url}) — {utc_now}"
    )


def _format_refs_comment_body(keys: dict[str, str]) -> str:
    lines = sorted(keys.values(), key=lambda s: s.lower())
    # Plain markdown only — Linear displays raw `<details>` / HTML comments as text.
    return (
        f"{REFS_SECTION_HEADER}\n\n"
        f"{REFS_SECTION_INTRO}\n\n"
        + "\n".join(lines)
        + "\n"
    )


def _merge_refs_comment(
    previous_body: str | None,
    scan_ref: ScanRef,
    short_sha: str,
    run_url: str,
) -> str:
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_line = _format_ref_line(scan_ref, short_sha, run_url, utc_now)
    keys: dict[str, str] = {}
    if previous_body:
        keys = _parse_existing_ref_keys(previous_body)
    if scan_ref.key in keys:
        return _format_refs_comment_body(keys)
    keys[scan_ref.key] = new_line
    return _format_refs_comment_body(keys)


def _find_issue_by_title(api_key: str, team_id: str, title: str) -> str | None:
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
    data = _graphql(
        api_key,
        q,
        {"teamId": team_id, "title": title},
    )
    nodes = (data.get("issues") or {}).get("nodes") or []
    if not nodes:
        return None
    return str(nodes[0]["id"])


def _resolve_issue_create_state_id(api_key: str, team_id: str) -> str | None:
    """Workflow state UUID for issueCreate (`stateId`).

    Priority:
    1. ``LINEAR_ISSUE_STATE_ID`` — explicit UUID (always wins).
    2. Else, if the team has **triage enabled**, use ``Team.triageIssueState`` — the state Linear
       uses for issues opened by integrations/non-members when triage is on (often named *Triage*).
    3. Otherwise omit ``stateId`` and Linear applies the team's normal default for API creates.

    Workspace states are UUIDs; ``LINEAR_ISSUE_STATE_ID`` overrides when auto triage is wrong.
    """
    explicit = os.environ.get("LINEAR_ISSUE_STATE_ID", "").strip()
    if explicit:
        return explicit
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
    data = _graphql(api_key, q, {"id": team_id})
    team = data.get("team") or {}
    if not team.get("triageEnabled"):
        return None
    triage_st = team.get("triageIssueState") or {}
    tid = triage_st.get("id")
    return str(tid) if tid else None


def _create_issue(
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
    data = _graphql(api_key, m, {"input": input_payload})
    result = data.get("issueCreate") or {}
    if not result.get("success"):
        raise RuntimeError(f"issueCreate failed: {data}")
    issue = result.get("issue") or {}
    return str(issue["id"])


def _link_issue_related(
    api_key: str, issue_id: str, related_issue_id: str
) -> None:
    """Create a ``related`` link between two issues (Linear is symmetric; one call per pair)."""
    m = """
mutation IssueRelationCreate($input: IssueRelationCreateInput!) {
  issueRelationCreate(input: $input) {
    success
    issueRelation { id type }
  }
}
"""
    data = _graphql(
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


def _link_issue_related_best_effort(
    api_key: str, issue_id: str, related_issue_id: str
) -> str:
    """Try ``_link_issue_related``; return a non-empty message only on a non-duplicate error."""
    try:
        _link_issue_related(api_key, issue_id, related_issue_id)
    except RuntimeError as e:
        em = str(e).lower()
        if any(
            x in em
            for x in (
                "existing",
                "already",
                "duplicate",
                " unique",
                "constraint",
            )
        ):
            return ""
        return str(e)
    return ""


def _undirected_pair_key(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a < b else (b, a)


def _issue_pairs_cve_or_pkg(
    created: list[tuple[str, str, str]],
) -> set[tuple[str, str]]:
    """Pairs of new issue ids that must be *related* (clique on each OR-connected component).

    ``created`` entries: ``(issue_id, vid, pkg_key)``. Edge between i and j if ``vid`` matches, or
    (for non-empty package keys) ``pkg_key`` matches — then union-find, then all pairs per component.
    """
    n = len(created)
    if n < 2:
        return set()
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: int, y: int) -> None:
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry

    for i in range(n):
        _, vid_i, pkg_i = created[i]
        for j in range(i + 1, n):
            _, vid_j, pkg_j = created[j]
            if vid_i == vid_j or (bool(pkg_i) and bool(pkg_j) and pkg_i == pkg_j):
                union(i, j)
    comp: dict[int, list[str]] = defaultdict(list)
    for i in range(n):
        comp[find(i)].append(created[i][0])
    out: set[tuple[str, str]] = set()
    for members in comp.values():
        mlen = len(members)
        if mlen < 2:
            continue
        for i in range(mlen):
            for j in range(i + 1, mlen):
                a, b = members[i], members[j]
                out.add(_undirected_pair_key(a, b))
    return out


def _create_issue_relations_cve_or_pkg(
    api_key: str, pairs: set[tuple[str, str]],
) -> None:
    """``pairs`` = unique (min,max) id tuples; one ``issueRelationCreate`` per edge."""
    if not pairs:
        return
    n_ok, n_err = 0, 0
    for a, b in sorted(pairs):
        err = _link_issue_related_best_effort_util(api_key, a, b)
        if err:
            n_err += 1
            print(
                f"WARNING: could not add related link {a} <-> {b}: {err}",
                file=sys.stderr,
            )
        else:
            n_ok += 1
    # ok counts try successes + benign duplicates; pairs may re-run on a second sync
    print(
        f"Issue relations (same-CVE or same-PkgName components): "
        f"{n_ok} pair operation(s) OK, {n_err} error(s) ({len(pairs)} unique pair(s))."
    )


def _comment_has_refs_anchor(body: str) -> bool:
    return _comment_has_refs_anchor_util(body)


def _get_marker_comment_id(
    api_key: str, issue_id: str
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
    data = _graphql(api_key, q, {"issueId": issue_id})
    issue = data.get("issue")
    if not issue:
        return None, None
    for node in (issue.get("comments") or {}).get("nodes") or []:
        body = node.get("body") or ""
        if _comment_has_refs_anchor(body):
            return str(node["id"]), body
    return None, None


def _create_comment(api_key: str, issue_id: str, body: str) -> None:
    m = """
mutation CreateComment($input: CommentCreateInput!) {
  commentCreate(input: $input) {
    success
  }
}
"""
    data = _graphql(
        api_key,
        m,
        {"input": {"issueId": issue_id, "body": body}},
    )
    if not (data.get("commentCreate") or {}).get("success"):
        raise RuntimeError(f"commentCreate failed: {data}")


def _update_comment(api_key: str, comment_id: str, body: str) -> None:
    m = """
mutation CommentUpdate($id: String!, $input: CommentUpdateInput!) {
  commentUpdate(id: $id, input: $input) {
    success
  }
}
"""
    data = _graphql(
        api_key,
        m,
        {"id": comment_id, "input": {"body": body}},
    )
    if not (data.get("commentUpdate") or {}).get("success"):
        raise RuntimeError(f"commentUpdate failed: {data}")


def _sync_refs_comment(
    api_key: str,
    issue_id: str,
    scan_ref: ScanRef,
    short_sha: str,
    run_url: str,
) -> None:
    comment_id, prev_body = _get_marker_comment_id_util(
        api_key, issue_id, _comment_has_refs_anchor_util
    )
    new_body = _merge_refs_comment_util(
        prev_body, scan_ref.kind, scan_ref.name, short_sha, run_url
    )
    if comment_id:
        _update_comment_util(api_key, comment_id, new_body)
    else:
        _create_comment_util(api_key, issue_id, new_body)


def _resolve_linear_team_id() -> str:
    return (
        os.environ.get("LINEAR_TEAM_ID", "").strip()
        or os.environ.get("TRIVY_LINEAR_TEAM_ID", "").strip()
    )


def _resolve_linear_label_ids() -> list[str] | None:
    raw = os.environ.get("LINEAR_LABEL_IDS", "").strip() or os.environ.get(
        "TRIVY_LINEAR_LABEL_IDS", ""
    ).strip()
    return [x.strip() for x in raw.split(",") if x.strip()] or None


def _image_ref_key(target: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", target)


def _raw_report_paths_for_occurrences(
    raw_report_paths: list[Path],
    occurrences: list[tuple[str, str, str, dict[str, Any]]],
    scanner: str,
) -> list[Path]:
    if not raw_report_paths:
        return []
    wanted_prefixes: list[str]
    if scanner == "trivy":
        wanted_prefixes = ["trivy-"]
    elif scanner == "trivy_grype":
        wanted_prefixes = ["trivy-", "grype-"]
    else:
        wanted_prefixes = ["grype-"]
    keys = {_image_ref_key(artifact_ref) for artifact_ref, _, _, _ in occurrences}
    selected: list[Path] = []
    seen: set[str] = set()
    for p in raw_report_paths:
        name = p.name
        if not any(name.startswith(prefix) for prefix in wanted_prefixes):
            continue
        if not any(name == f"{prefix}{key}.json" for key in keys for prefix in wanted_prefixes):
            continue
        sp = str(p)
        if sp not in seen:
            seen.add(sp)
            selected.append(p)
    return selected


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--scanner",
        default=os.environ.get("SCANNER", "trivy"),
        help="Scanner id: trivy | trivy_grype (mixed Trivy + Grype JSON, deduped by image+CVE). "
        "Default: trivy.",
    )
    p.add_argument(
        "report_paths",
        nargs="+",
        type=Path,
        help="Report file(s) produced by the scanner (e.g. Trivy JSON)",
    )
    p.add_argument(
        "--raw-report-paths",
        action="append",
        type=Path,
        help="Optional raw scanner report; repeat the flag for each file (uploaded as issue attachments on create).",
    )
    args = p.parse_args()
    raw_report_path_list: list[Path] = list(args.raw_report_paths or [])
    scanner = args.scanner.strip().lower()

    api_key = os.environ.get("LINEAR_SECURITY_SCAN_API_KEY", "").strip()
    if not api_key:
        print("ERROR: Set LINEAR_SECURITY_SCAN_API_KEY", file=sys.stderr)
        return 1

    team_id = _resolve_linear_team_id()
    if not team_id:
        print(
            "ERROR: Set LINEAR_TEAM_ID (or legacy TRIVY_LINEAR_TEAM_ID) to the Linear team UUID",
            file=sys.stderr,
        )
        return 1

    base_label_ids = _resolve_linear_label_ids() or []
    repo_label_map = _resolve_linear_repo_label_map_util()

    if scanner not in SCANNERS:
        print(
            f"ERROR: Unknown scanner {scanner!r}. Implemented: {sorted(SCANNERS)}",
            file=sys.stderr,
        )
        return 1

    kind = os.environ.get("SCAN_REF_KIND", "").strip().lower()
    name = os.environ.get("SCAN_REF_NAME", "").strip()
    if kind not in ("branch", "tag") or not name:
        print(
            "ERROR: Set SCAN_REF_KIND to branch|tag and SCAN_REF_NAME "
            "(normalized ref from workflow)",
            file=sys.stderr,
        )
        return 1
    scan_ref = ScanRef(kind=kind, name=name)
    ref_label_id = _get_or_create_team_label_id_util(api_key, team_id, scan_ref.name)

    initial_state_id = _resolve_issue_create_state_id_from_linear_util(
        api_key, team_id, os.environ.get("LINEAR_ISSUE_STATE_ID", "").strip()
    )

    commit_sha = os.environ.get("GITHUB_SHA", "unknown")
    short_sha = commit_sha[:7] if len(commit_sha) >= 7 else commit_sha
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    run_url = f"{server}/{repo}/actions/runs/{run_id}" if repo and run_id else ""

    groups = SCANNERS[scanner](list(args.report_paths))
    if not groups:
        print(f"No findings in reports for scanner {scanner!r}. Nothing to sync.")
        return 0

    created = 0
    updated = 0
    # (issue_id, vid, pkg_key) for each **created** issue this run — used for clique relations at end.
    created_in_run: list[tuple[str, str, str]] = []

    for group_key, occ in sorted(groups.items(), key=lambda x: x[0]):
        artifact_ref, _, _, first = occ[0]
        if "\x1f" in group_key:
            vid, repo_scope = group_key.split("\x1f", 1)
        else:
            vid = group_key
            repo_scope = _repo_scope_ticket_label(artifact_ref)
        linear_title = _linear_issue_title_util(vid, first, repo_scope, scanner=scanner)
        description_heading = _advisory_title_for_description_util(
            scanner, first, fallback=linear_title
        )
        description = _build_description_util(
            scanner,
            vid,
            occ,
            run_url,
            scan_ref.kind,
            scan_ref.name,
            commit_sha,
            description_heading,
            repo_scope,
        )

        linear_priority: int | None = None
        linear_due_date: str | None = None
        if scanner in ("trivy", "trivy_grype"):
            worst = _worst_trivy_severity(occ)
            linear_priority = _linear_priority_for_scan_severity(worst)
            linear_due_date = _linear_due_date_for_scan_severity(worst)

        repo_lids = _repo_label_ids_for_occurrences_util(repo_label_map, occ)
        create_labels = _dedupe_preserve_order([*base_label_ids, ref_label_id, *repo_lids])
        create_labels_arg: list[str] | None = (
            create_labels if create_labels else None
        )

        existing = _find_issue_by_title_util(api_key, team_id, linear_title)
        if existing:
            issue_id = existing
            _sync_refs_comment(api_key, issue_id, scan_ref, short_sha, run_url)
            desired_sync_label_ids = _dedupe_preserve_order([ref_label_id, *repo_lids])
            if desired_sync_label_ids:
                current = _issue_graphql_label_ids_util(api_key, issue_id)
                merged = _dedupe_preserve_order([*current, *desired_sync_label_ids])
                if set(merged) != set(current):
                    _issue_update_label_ids_util(api_key, issue_id, merged)
            updated += 1
            print(f"Updated refs comment: {linear_title} ({issue_id})")
        else:
            issue_id = _create_issue_util(
                api_key,
                team_id,
                linear_title,
                description,
                create_labels_arg,
                linear_priority,
                initial_state_id,
                linear_due_date,
            )
            _sync_refs_comment(api_key, issue_id, scan_ref, short_sha, run_url)
            created += 1
            print(f"Created issue: {linear_title} ({issue_id})")
            issue_raw_reports = _raw_report_paths_for_occurrences(
                raw_report_path_list, occ, scanner
            )
            for raw_report in issue_raw_reports:
                if not raw_report.is_file():
                    continue
                try:
                    _attach_file_to_issue_util(
                        api_key=api_key,
                        issue_id=issue_id,
                        file_path=raw_report,
                        title=f"Raw scan report: {raw_report.name}",
                    )
                except Exception as exc:
                    print(
                        f"WARNING: Failed to attach raw report {raw_report} to issue {issue_id}: {exc}",
                        file=sys.stderr,
                    )
            pkg_key = (
                _trivy_pkg_name_for_title_util(first).strip()
                if scanner in ("trivy", "trivy_grype")
                else ""
            )
            created_in_run.append((issue_id, vid, pkg_key))

    if created_in_run:
        _create_issue_relations_cve_or_pkg(
            api_key, _issue_pairs_cve_or_pkg_util(created_in_run)
        )

    print(f"Done. Created {created}, updated refs on {updated} existing.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
