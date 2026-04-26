from __future__ import annotations

import json
import os
import re
import sys
from functools import lru_cache
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Trivy rows: (artifact_ref, result_target, class/type, vuln). artifact_ref = scanned image ref for scope.
GroupedRows = dict[str, list[tuple[str, str, str, dict[str, Any]]]]

_SEVERITY_RANK = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}


def split_image_ref(target: str) -> tuple[str, str]:
    """Best-effort parse of a Trivy Target into (repository basename, tag)."""
    t = target.strip()
    if not t:
        return "", ""
    if "@sha256:" in t:
        t = t.split("@", 1)[0]
    if ":" in t:
        image_part, tag = t.rsplit(":", 1)
        repo = image_part.split("/")[-1]
        return repo, tag
    base = t.rstrip("/").split("/")[-1]
    return base, ""


@lru_cache(maxsize=1)
def _variant_suffix_labels_from_env() -> tuple[str, ...]:
    """Read variant suffix labels from DATAHUB_VARIANT_TAG_SUFFIXES env (comma-separated)."""
    raw = os.getenv("DATAHUB_VARIANT_TAG_SUFFIXES", "")
    out: list[str] = []
    seen: set[str] = set()
    for part in raw.split(","):
        tok = part.strip().lower()
        if tok.startswith("-"):
            tok = tok[1:]
        if tok and tok not in seen:
            seen.add(tok)
            out.append(tok)
    out.sort(key=len, reverse=True)
    return tuple(out)


def variant_label_from_tag(tag: str) -> str:
    """Return configured variant label from tag suffix, or '' for primary/unknown."""
    if not tag:
        return ""
    suffix_labels = _variant_suffix_labels_from_env()
    if not suffix_labels:
        return ""
    lower = tag.lower()
    for suf in suffix_labels:
        suf_token = "-" + suf
        if lower.endswith(suf_token):
            return suf
    return ""


def repo_scope_ticket_label(target: str) -> str:
    """Unique ticket scope: `datahub-executor` or `datahub-executor-slim` etc."""
    repo, tag = split_image_ref(target)
    if not repo:
        return "unknown"
    variant = variant_label_from_tag(tag)
    return f"{repo}-{variant}" if variant else repo


def try_load_json_report(p: Path) -> dict[str, Any] | None:
    """Load a Trivy/Grype JSON report; skip empty or invalid files with a stderr warning."""
    try:
        raw = p.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"WARNING: {p.name}: could not read ({exc}); skipping", file=sys.stderr)
        return None
    if not raw.strip():
        print(f"WARNING: {p.name}: empty report file; skipping", file=sys.stderr)
        return None
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"WARNING: {p.name}: invalid JSON ({exc}); skipping", file=sys.stderr)
        return None
    if not isinstance(doc, dict):
        print(
            f"WARNING: {p.name}: expected JSON object, got {type(doc).__name__}; skipping",
            file=sys.stderr,
        )
        return None
    return doc


def load_json_files(paths: list[Path]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in paths:
        if not p.is_file():
            raise FileNotFoundError(p)
        doc = try_load_json_report(p)
        if doc is not None:
            out.append(doc)
    return out


def trivy_vulnerability_id(vuln: dict[str, Any]) -> str:
    vid = vuln.get("VulnerabilityID")
    if vid:
        return str(vid)
    inner = vuln.get("Vulnerability")
    if isinstance(inner, dict) and inner.get("VulnerabilityID"):
        return str(inner["VulnerabilityID"])
    return f"UNKNOWN:{vuln.get('PkgName', '')}:{vuln.get('Title', '')}"[:120]


def trivy_severity_string(vuln: dict[str, Any]) -> str | None:
    """Trivy finding severity (top-level or nested under Vulnerability)."""
    s = vuln.get("Severity")
    if s:
        return str(s).strip()
    inner = vuln.get("Vulnerability")
    if isinstance(inner, dict) and inner.get("Severity"):
        return str(inner["Severity"]).strip()
    return None


def worst_trivy_severity(
    occurrences: list[tuple[str, str, str, dict[str, Any]]],
) -> str | None:
    """Highest-severity label across occurrences."""
    best_rank = -1
    label: str | None = None
    for _, _, _, vuln in occurrences:
        raw = trivy_severity_string(vuln)
        if not raw:
            continue
        u = raw.upper()
        rnk = _SEVERITY_RANK.get(u, 0)
        if rnk > best_rank:
            best_rank = rnk
            label = u
    return label


@lru_cache(maxsize=1)
def _scan_severity_allowlist_from_env() -> frozenset[str] | None:
    """Read scanner severity allowlist from DATAHUB_SCAN_SEVERITIES (comma-separated)."""
    raw = os.getenv("DATAHUB_SCAN_SEVERITIES", "")
    if not raw.strip():
        return None
    out: set[str] = set()
    for part in raw.split(","):
        tok = part.strip().upper()
        if tok:
            out.add(tok)
    return frozenset(out)


def iter_trivy_findings(
    reports: list[dict[str, Any]],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    rows: list[tuple[str, str, str, dict[str, Any]]] = []
    for doc in reports:
        artifact = doc.get("ArtifactName") or doc.get("ArtifactType") or "unknown"
        for result in doc.get("Results") or []:
            detail_target = result.get("Target") or artifact
            rclass = result.get("Class") or ""
            rtype = result.get("Type") or ""
            for vuln in result.get("Vulnerabilities") or []:
                rows.append((artifact, detail_target, f"{rclass}/{rtype}".strip("/"), dict(vuln)))
    return rows


def parse_trivy_reports(paths: list[Path]) -> GroupedRows:
    """Parse Trivy JSON; group by vulnerability ID + repo/variant scope."""
    reports = load_json_files(paths)
    rows = iter_trivy_findings(reports)
    groups: GroupedRows = {}
    for artifact_ref, detail_target, rclass, vuln in rows:
        vid = trivy_vulnerability_id(vuln)
        scope = repo_scope_ticket_label(artifact_ref)
        gkey = f"{vid}\x1f{scope}"
        groups.setdefault(gkey, []).append((artifact_ref, detail_target, rclass, vuln))
    return groups


def report_json_kind(doc: dict[str, Any]) -> str:
    """trivy vs grype from top-level JSON shape."""
    if isinstance(doc.get("Results"), list):
        return "trivy"
    if isinstance(doc.get("matches"), list):
        return "grype"
    return "unknown"


def grype_image_ref(doc: dict[str, Any]) -> str:
    src = doc.get("source") or {}
    t = src.get("target")
    if isinstance(t, dict):
        u = t.get("userInput") or t.get("fullTag") or t.get("imageID")
        if u:
            return str(u)
    if isinstance(t, str) and t.strip():
        return t.strip()
    return "unknown"


def grype_severity_to_trivy_upper(v: dict[str, Any]) -> str:
    """Map Grype vulnerability.severity to Trivy-style labels."""
    s = str(v.get("severity") or "").strip().upper()
    if s in ("MEDIUM", "MED", "M"):
        return "MEDIUM"
    if s in ("LOW", "L"):
        return "LOW"
    if s in ("NEGLIGIBLE", "NEGL", "INFO", "INFORMATIONAL"):
        return "UNKNOWN"
    return s


def grype_match_to_trivy_vuln(match: dict[str, Any]) -> dict[str, Any]:
    """Shape a Grype match like a Trivy Vulnerabilities[] entry."""
    v = match.get("vulnerability") or {}
    art = match.get("artifact") or {}
    vid = str(v.get("id") or "").strip()
    sev = grype_severity_to_trivy_upper(v)
    desc = v.get("description")
    urls = [str(x) for x in (v.get("urls") or []) if x]
    primary = urls[0] if urls else None
    inner: dict[str, Any] = {}
    if vid:
        inner["VulnerabilityID"] = vid
        inner["Title"] = vid
    if desc is not None and str(desc).strip():
        inner["Description"] = str(desc).strip()
    if primary:
        inner["PrimaryURL"] = primary
    out: dict[str, Any] = {
        "VulnerabilityID": vid,
        "Title": vid,
        "PkgName": str(art.get("name") or ""),
        "InstalledVersion": str(art.get("version") or ""),
        "Severity": sev,
        "PrimaryURL": primary,
        "References": urls,
        "Vulnerability": inner,
        "DataSource": v.get("dataSource"),
        "_datahub_scanner_source": "grype",
    }
    locs = art.get("locations") or []
    if locs and isinstance(locs[0], dict) and locs[0].get("path"):
        out["FilePath"] = str(locs[0]["path"])
    if v.get("cvss") is not None:
        out["CVSS"] = v.get("cvss")
    fix = v.get("fix")
    if isinstance(fix, dict) and fix.get("versions"):
        out["FixedVersion"] = ", ".join(str(x) for x in fix.get("versions") or [])
    return out


def iter_grype_findings(doc: dict[str, Any]) -> list[tuple[str, str, str, dict[str, Any]]]:
    rows: list[tuple[str, str, str, dict[str, Any]]] = []
    allowed = _scan_severity_allowlist_from_env()
    art_ref = grype_image_ref(doc)
    for m in doc.get("matches") or []:
        vg = m.get("vulnerability") or {}
        sev = grype_severity_to_trivy_upper(vg)
        if allowed is not None and sev not in allowed:
            continue
        vuln = grype_match_to_trivy_vuln(m)
        gart = m.get("artifact") or {}
        rclass = f"grype/{str(gart.get('type') or 'package')}"
        rows.append((art_ref, art_ref, rclass, vuln))
    return rows


def merge_trivy_grype_rows_for_same_image_cve(
    rows: list[tuple[str, str, str, dict[str, Any]]],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    """One row per (scanned image ref, CVE) with _datahub_scanners; prefer Trivy when both exist."""
    from collections import defaultdict

    buckets: dict[tuple[str, str], list[tuple[str, str, str, dict[str, Any]]]] = defaultdict(list)
    for row in rows:
        artifact, _target, _rclass, vuln = row
        vid = trivy_vulnerability_id(vuln)
        buckets[(artifact, vid)].append(row)
    out: list[tuple[str, str, str, dict[str, Any]]] = []
    for _k, group in buckets.items():
        trivy: tuple[str, str, str, dict[str, Any]] | None = None
        grype: tuple[str, str, str, dict[str, Any]] | None = None
        for artifact, target, rclass, vuln in group:
            src = vuln.get("_datahub_scanner_source", "")
            if src == "trivy":
                trivy = (artifact, target, rclass, vuln)
            elif src == "grype":
                grype = (artifact, target, rclass, vuln)
        if trivy and grype:
            a, t, c, v = trivy
            v2 = dict(v)
            v2.pop("_datahub_scanner_source", None)
            v2["_datahub_scanners"] = ["trivy", "grype"]
            out.append((a, t, c, v2))
        elif trivy:
            a, t, c, v = trivy
            v2 = dict(v)
            v2.pop("_datahub_scanner_source", None)
            v2["_datahub_scanners"] = ["trivy"]
            out.append((a, t, c, v2))
        elif grype:
            a, t, c, v = grype
            v2 = dict(v)
            v2.pop("_datahub_scanner_source", None)
            v2["_datahub_scanners"] = ["grype"]
            out.append((a, t, c, v2))
        else:
            artifact, target, rclass, vuln = group[0]
            v2 = dict(vuln)
            v2.pop("_datahub_scanner_source", None)
            v2["_datahub_scanners"] = ["unknown"]
            out.append((artifact, target, rclass, v2))
    return out


def parse_trivy_grype_merged(paths: list[Path]) -> GroupedRows:
    """Load Trivy and/or Grype JSON, merge by image + CVE, then group for Linear."""
    all_rows: list[tuple[str, str, str, dict[str, Any]]] = []
    for p in paths:
        if not p.is_file():
            continue
        doc = try_load_json_report(p)
        if doc is None:
            continue
        kind = report_json_kind(doc)
        if kind == "trivy":
            for a, t, c, v in iter_trivy_findings([doc]):
                vv = dict(v)
                vv["_datahub_scanner_source"] = "trivy"
                all_rows.append((a, t, c, vv))
        elif kind == "grype":
            all_rows.extend(iter_grype_findings(doc))
        else:
            print(
                f"WARNING: {p.name}: not Trivy (Results) or Grype (matches) JSON; skipping",
                file=sys.stderr,
            )
    if not all_rows:
        return {}
    merged = merge_trivy_grype_rows_for_same_image_cve(all_rows)
    groups: GroupedRows = {}
    for artifact_ref, detail_target, rclass, vuln in merged:
        vid = trivy_vulnerability_id(vuln)
        scope = repo_scope_ticket_label(artifact_ref)
        gkey = f"{vid}\x1f{scope}"
        groups.setdefault(gkey, []).append((artifact_ref, detail_target, rclass, vuln))
    return groups


# Names that read poorly in GFM tables in Linear (long text, JSON blobs, URLs).
TRIVY_STACK_WHEN_KEY: frozenset[str] = frozenset(
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
ADVISORY_LONG_TEXT_KEYS = frozenset({"Description", "Details", "Body"})
TECHNICAL_FOLD_KEYS: frozenset[str] = frozenset(
    {"CVSS", "Layer", "DataSource", "PkgIdentifier"}
)
INTERNAL_VULN_KEYS: frozenset[str] = frozenset(
    {"_datahub_scanners", "_datahub_scanner_source"}
)

REFS_HTML_MARKER = "<!-- datahub-security-scan-refs -->"
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


def plain_scalar_for_cell(val: Any) -> str:
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


def fmt_cell(val: Any) -> str:
    return plain_scalar_for_cell(val).replace("|", "\\|")


def markdown_table(header: tuple[str, str], rows: list[tuple[str, Any]]) -> str:
    filtered = [(k, v) for k, v in rows if v is not None and v != ""]
    if not filtered:
        return ""
    lines = [
        f"| **{header[0]}** | **{header[1]}** |",
        "| :--- | :--- |",
    ]
    for key, val in filtered:
        lines.append(f"| {fmt_cell(key)} | {fmt_cell(val)} |")
    return "\n".join(lines) + "\n"


def affected_result_target_class_table(detail_target: str, rclass: str) -> str:
    rows = [("Image", detail_target)]
    if rclass:
        rows.append(("Class/Type", rclass))
    return markdown_table(("Affected", "Value"), rows)


def trivy_technical_supporting_sections(technical: str, tail: str) -> str:
    parts: list[str] = []
    if technical.strip():
        parts.append("#### Technical details\n\n")
        parts.append(technical.rstrip() + "\n\n")
    if tail.strip():
        parts.append("#### Supporting details\n\n")
        parts.append(tail.rstrip() + "\n\n")
    return "".join(parts)


def stacked_value_text(val: Any, *, max_len: int = 24_000) -> str:
    if val is None:
        return ""
    if isinstance(val, str):
        txt = val.strip()
    elif isinstance(val, (dict, list)):
        txt = json.dumps(val, ensure_ascii=False, indent=2).strip()
    else:
        txt = str(val).strip()
    if len(txt) > max_len:
        txt = txt[: max_len - 3] + "..."
    return txt


def should_stack_field_row(field_name: str, val: Any) -> bool:
    if val is None:
        return False
    if field_name in TRIVY_STACK_WHEN_KEY:
        return True
    if isinstance(val, str) and ("\n" in val or len(val) > 120):
        return True
    return isinstance(val, (dict, list))


def layout_field_rows_ordered(rows: list[tuple[str, Any]]) -> str:
    simple_rows: list[tuple[str, Any]] = []
    stacked_rows: list[tuple[str, Any]] = []
    for k, v in rows:
        (stacked_rows if should_stack_field_row(k, v) else simple_rows).append((k, v))
    out: list[str] = []
    if simple_rows:
        out.append(markdown_table(("Field", "Value"), simple_rows))
    if stacked_rows:
        out.append(stacked_field_blocks(stacked_rows))
    return "".join(out)


def stacked_block_primary_url_if_applicable(label: Any, val: Any) -> str | None:
    if str(label).strip() != "PrimaryURL":
        return None
    txt = stacked_value_text(val)
    if not txt:
        return None
    if txt.lower().startswith(("http://", "https://")):
        return f"- **PrimaryURL**: [{txt}]({txt})\n"
    return f"- **PrimaryURL**: `{txt}`\n"


def stacked_field_blocks(rows: list[tuple[str, Any]]) -> str:
    parts: list[str] = []
    for label, val in rows:
        maybe_url_line = stacked_block_primary_url_if_applicable(label, val)
        if maybe_url_line is not None:
            parts.append(maybe_url_line)
            continue
        txt = stacked_value_text(val)
        if not txt:
            continue
        if "\n" in txt:
            parts.append(f"- **{label}**\n\n```\n{txt}\n```\n")
        else:
            parts.append(f"- **{label}**: `{txt}`\n")
    return "".join(parts)


def serialize_trivy_vulnerability_record(vuln: dict[str, Any]) -> tuple[str, str, str]:
    v = dict(vuln)
    inner = v.get("Vulnerability")
    if isinstance(inner, dict):
        merged = dict(inner)
        merged.update({k: v[k] for k in v.keys() if k not in ("Vulnerability",)})
        v = merged
    for k in INTERNAL_VULN_KEYS:
        v.pop(k, None)
    rows: list[tuple[str, Any]] = []
    for key in sorted(v.keys()):
        if key in ("Description", "Details", "Body"):
            continue
        rows.append((key, v[key]))
    technical_rows = [(k, vv) for k, vv in rows if k in TECHNICAL_FOLD_KEYS]
    support_rows = [(k, vv) for k, vv in rows if k not in TECHNICAL_FOLD_KEYS]
    technical = layout_field_rows_ordered(technical_rows)
    tail = layout_field_rows_ordered(support_rows)
    title = advisory_title_for_description("trivy", v, fallback="Advisory")
    desc = trivy_advisory_description_plain(v)
    return title, desc, trivy_technical_supporting_sections(technical, tail)


def raw_finding_table(vuln: dict[str, Any]) -> str:
    rows = [(k, v) for k, v in sorted(vuln.items(), key=lambda kv: kv[0]) if k != "Title"]
    return layout_field_rows_ordered(rows)


def trivy_pkg_name_for_title(vuln: dict[str, Any]) -> str:
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
    return raw


def linear_issue_title(
    vid: str,
    vuln: dict[str, Any],
    repo_scope: str,
    *,
    scanner: str,
    max_title_len: int = 250,
) -> str:
    if scanner in ("trivy", "trivy_grype"):
        pkg = trivy_pkg_name_for_title(vuln)
        if pkg:
            t = f"[{repo_scope}] {vid} in {pkg}"
        else:
            t = f"[{repo_scope}] {vid}"
    else:
        t = f"[{repo_scope}] {vid}"
    return t if len(t) <= max_title_len else t[: max_title_len - 1] + "…"


def advisory_title_for_description(
    scanner: str, vuln: dict[str, Any], *, fallback: str
) -> str:
    if scanner in ("trivy", "trivy_grype"):
        for key in ("Title", "VulnerabilityID"):
            val = vuln.get(key)
            if val and str(val).strip():
                return str(val).strip()
        inner = vuln.get("Vulnerability")
        if isinstance(inner, dict):
            for key in ("Title", "VulnerabilityID"):
                val = inner.get(key)
                if val and str(val).strip():
                    return str(val).strip()
    return fallback


def scanner_cell_from_occurrences(
    scanner: str, occ: list[tuple[str, str, str, dict[str, Any]]]
) -> str:
    if scanner == "trivy":
        return "trivy"
    merged: list[str] = []
    for _, _, _, vuln in occ:
        for s in vuln.get("_datahub_scanners", []) or []:
            if s and s not in merged:
                merged.append(str(s))
    return ",".join(merged) if merged else scanner


def trivy_advisory_description_plain(vuln: dict[str, Any]) -> str:
    for key in ("Description", "Details", "Body"):
        val = vuln.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    inner = vuln.get("Vulnerability")
    if isinstance(inner, dict):
        for key in ("Description", "Details", "Body"):
            val = inner.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return ""


def build_description(
    scanner: str,
    vid: str,
    occurrences: list[tuple[str, str, str, dict[str, Any]]],
    run_url: str,
    scan_ref_kind: str,
    scan_ref_name: str,
    commit_sha: str,
    heading: str,
    repo_scope: str,
) -> str:
    lines = [f"### {heading}\n\n", "#### Identity & severity\n\n"]
    lines.append(markdown_table(("Field", "Value"), [("Vulnerability ID", vid), ("Image scope", repo_scope), ("Scanner", scanner_cell_from_occurrences(scanner, occurrences))]))
    lines.append("\n")
    lines.append("#### Scan context\n\n")
    scan_rows = [
        ("Git ref kind", scan_ref_kind),
        ("Git ref name", scan_ref_name),
        ("Commit SHA", commit_sha),
    ]
    if run_url:
        scan_rows.append(("Workflow run", run_url))
    lines.append(markdown_table(("Field", "Value"), scan_rows))
    lines.append("\n")
    lines.append("#### Affected images\n\n")
    dedup: dict[tuple[str, str], str] = {}
    for artifact_ref, detail_target, rclass, _v in occurrences:
        dedup[(artifact_ref, detail_target)] = rclass
    for (artifact_ref, detail_target), rclass in sorted(dedup.items()):
        lines.append(affected_result_target_class_table(detail_target, rclass))
        lines.append("\n")
    lines.append("#### Raw finding details\n\n")
    if scanner in ("trivy", "trivy_grype"):
        _, _, _, first = occurrences[0]
        title, desc, details = serialize_trivy_vulnerability_record(first)
        lines.append(f"##### {title}\n\n")
        if desc:
            lines.append(desc.rstrip() + "\n\n")
        if details:
            lines.append(details.rstrip() + "\n\n")
    else:
        _, _, _, first = occurrences[0]
        lines.append(raw_finding_table(first))
    return "".join(lines).rstrip() + "\n"


def parse_existing_ref_keys(comment_body: str) -> dict[str, str]:
    keys: dict[str, str] = {}
    for line in comment_body.splitlines():
        m = re.match(r"^\s*-\s+\*\*(Branch|Tag)\*\*:\s+`([^`]+)`\b", line)
        if not m:
            continue
        kind = m.group(1).lower()
        name = m.group(2).strip()
        if kind in ("branch", "tag") and name:
            keys[f"{kind}:{name}"] = line
    return keys


def format_ref_line(
    scan_ref_kind: str,
    scan_ref_name: str,
    short_sha: str,
    run_url: str,
    utc_ts: str,
) -> str:
    kind_h = "Branch" if scan_ref_kind == "branch" else "Tag"
    run_part = f" ([run]({run_url}))" if run_url else ""
    return f"- **{kind_h}**: `{scan_ref_name}` @ `{short_sha}`{run_part} _(UTC {utc_ts})_"


def format_refs_comment_body(keys: dict[str, str]) -> str:
    lines = [REFS_HTML_MARKER, REFS_SECTION_HEADER, "", REFS_SECTION_INTRO, ""]
    def _sort_key(item: str) -> tuple[str, str]:
        kind, name = item.split(":", 1)
        return (kind, name.lower())
    for key in sorted(keys.keys(), key=_sort_key):
        lines.append(keys[key])
    return "\n".join(lines).rstrip() + "\n"


def merge_refs_comment(
    previous_body: str | None,
    scan_ref_kind: str,
    scan_ref_name: str,
    short_sha: str,
    run_url: str,
) -> str:
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_line = format_ref_line(scan_ref_kind, scan_ref_name, short_sha, run_url, utc_now)
    keys: dict[str, str] = {}
    if previous_body:
        keys = parse_existing_ref_keys(previous_body)
    key = f"{scan_ref_kind}:{scan_ref_name}"
    if key in keys:
        return format_refs_comment_body(keys)
    keys[key] = new_line
    return format_refs_comment_body(keys)


def comment_has_refs_anchor(body: str) -> bool:
    if REFS_HTML_MARKER in body:
        return True
    if LEGACY_REFS_MARKER in body or REFS_SECTION_HEADER in body:
        return True
    return any(h in body for h in LEGACY_REFS_HEADERS)


def undirected_pair_key(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a < b else (b, a)


def issue_pairs_cve_or_pkg(created: list[tuple[str, str, str]]) -> set[tuple[str, str]]:
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
    from collections import defaultdict

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
                out.add(undirected_pair_key(a, b))
    return out
