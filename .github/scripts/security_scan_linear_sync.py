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
  - Update refs comment and merge labels on existing issues without removing current labels,
    except for the previous child of the same ref label group (see below).
- Labeling behavior:
  - Apply optional static labels from ``LINEAR_LABEL_IDS`` / ``TRIVY_LINEAR_LABEL_IDS``.
  - Apply component labels mapped from image repositories.
  - Apply a dynamic ref label named after ``SCAN_REF_NAME``. CI sets this to the repository
    default branch when the scanned image tag is ``quickstart``, ``head``, or ``latest``;
    otherwise to the image tag. The label lives in a label group chosen by the ref:
    - Tag refs whose names are semantic versions, including release candidates (``vX.Y.Z``,
      ``vX.Y.Z.W``, optional ``rcN`` / ``-rcN``, optional ``-cloud``), reuse the child of a
      workspace release group. A ``-cloud`` suffix uses ``Saas Release`` and the tag as the
      label name. A tag without it uses ``OSS Release`` and the label name ``OSS <tag>``
      (for example ``OSS v1.7.0.1``). The group is the one the release workflow creates when
      a final tag is cut, and it must already exist. An RC child is created under that group
      when missing. A branch is never a release, even when its name looks like a version.
    - Branch refs, and non-semantic tag refs (default branch, ``sha-*`` tags, custom builds),
      go under the team label group ``Security Scan``. Within that group the last scan wins.
      The group and the child are created when missing.
    Label groups are exclusive, so on existing issues the previous child of the applied
    label's group is replaced. A reused label that lives outside that group keeps its current
    parent and is added without dropping the group's existing child. The refs comment keeps
    the full history.
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
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from utils.linear_sync_utils import (
    attach_file_to_issue as _attach_file_to_issue_util,
    create_comment as _create_comment_util,
    create_issue as _create_issue_util,
    dedupe_preserve_order as _dedupe_preserve_order,
    find_issue_by_title as _find_issue_by_title_util,
    get_issue_identifier_url as _get_issue_identifier_url_util,
    get_marker_comment_id as _get_marker_comment_id_util,
    get_or_create_group_child_label_id as _get_or_create_group_child_label_id_util,
    get_or_create_label_group_id as _get_or_create_label_group_id_util,
    ResolvedLabel,
    issue_labels_with_parents as _issue_labels_with_parents_util,
    issue_update_label_ids as _issue_update_label_ids_util,
    label_ids_replacing_group_sibling as _label_ids_replacing_group_sibling_util,
    linear_due_date_for_scan_severity as _linear_due_date_for_scan_severity,
    linear_priority_for_scan_severity as _linear_priority_for_scan_severity,
    link_issue_related_best_effort as _link_issue_related_best_effort_util,
    repo_label_ids_for_occurrences as _repo_label_ids_for_occurrences_util,
    resolve_issue_create_state_id as _resolve_issue_create_state_id_from_linear_util,
    resolve_linear_repo_label_map as _resolve_linear_repo_label_map_util,
    update_comment as _update_comment_util,
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
    write_security_scan_summary_json,
)

SAAS_RELEASE_LABEL_GROUP = "Saas Release"
OSS_RELEASE_LABEL_GROUP = "OSS Release"
SECURITY_SCAN_LABEL_GROUP = "Security Scan"
# Child labels under OSS Release are ``OSS v1.7.0.1``, not the bare git tag.
OSS_RELEASE_LABEL_PREFIX = "OSS "
CLOUD_RELEASE_SUFFIX = "-cloud"

# Release tags: vX.Y.Z or vX.Y.Z.W, optional rc (v1.2.3rc1, v1.2.3-rc1), optional -cloud.
# Anything else (sha-*, branch names, custom builds) is non-semantic.
SEMANTIC_VERSION_TAG_RE = re.compile(
    rf"^v\d+\.\d+\.\d+(?:\.\d+)?(?:-?rc\d+)?(?:{re.escape(CLOUD_RELEASE_SUFFIX)})?$"
)


@dataclass(frozen=True)
class ScanRef:
    kind: str  # "branch" | "tag"
    name: str

    @property
    def key(self) -> str:
        return f"{self.kind}:{self.name}"


@dataclass(frozen=True)
class RefLabel:
    """The ref label to apply and the group it actually belongs to.

    ``group_id`` is ``None`` when the label is ungrouped. Sibling replacement uses this
    parent, which can differ from the group the ref was routed to when an existing label
    is reused in place.
    """

    label_id: str
    group_id: str | None


# Trivy rows: (artifact_ref, result_target, class/type, vuln). artifact_ref = scanned image ref for scope.
GroupedRows = dict[str, list[tuple[str, str, str, dict[str, Any]]]]
ParserFn = Callable[[list[Path]], GroupedRows]

SCANNERS: dict[str, ParserFn] = {
    "trivy": parse_trivy_reports,
    "trivy_grype": parse_trivy_grype_merged,
}


def is_semantic_version_tag(ref_name: str) -> bool:
    """True for a final release or an RC (``v1.2.3rc1``, ``v1.2.3-rc1``), optional ``-cloud``."""
    return SEMANTIC_VERSION_TAG_RE.match(ref_name.strip()) is not None


def release_label_group_for_tag(ref_name: str) -> str | None:
    """Workspace release group for a semantic tag, or ``None`` when it is not one.

    Final releases and RCs share a group. A ``-cloud`` suffix is ``Saas Release``; the same
    version shape without it is ``OSS Release``.
    """
    name = ref_name.strip()
    if not is_semantic_version_tag(name):
        return None
    if name.endswith(CLOUD_RELEASE_SUFFIX):
        return SAAS_RELEASE_LABEL_GROUP
    return OSS_RELEASE_LABEL_GROUP


def ref_label_name(ref_name: str, ref_kind: str = "tag") -> str:
    """Linear child label name for a scan ref.

    OSS release tags follow the workspace convention ``OSS v1.7.0.1``. SaaS
    release tags, branch refs, and non-semantic refs use the ref name itself.
    """
    name = ref_name.strip()
    if ref_kind == "tag" and release_label_group_for_tag(name) == OSS_RELEASE_LABEL_GROUP:
        return f"{OSS_RELEASE_LABEL_PREFIX}{name}"
    return name


def _resolve_ref_label(
    api_key: str, team_id: str, ref_name: str, ref_kind: str = "tag"
) -> RefLabel:
    """Pick the label group for ``ref_name`` and reuse or create the child label under it.

    Release groups apply only to tag refs. The returned group is the parent of the label
    that was found or created, which is the routed group unless an existing label was reused.
    """
    label_name = ref_label_name(ref_name, ref_kind)
    release_group = release_label_group_for_tag(ref_name) if ref_kind == "tag" else None
    resolved: ResolvedLabel
    if release_group:
        group_id = _get_or_create_label_group_id_util(
            api_key, release_group, None, create_if_missing=False
        )
        resolved = _get_or_create_group_child_label_id_util(
            api_key, group_id, label_name
        )
        print(f"Linear ref label {label_name!r}: workspace group {release_group!r}")
    else:
        group_id = _get_or_create_label_group_id_util(
            api_key, SECURITY_SCAN_LABEL_GROUP, team_id, create_if_missing=True
        )
        resolved = _get_or_create_group_child_label_id_util(
            api_key, group_id, label_name, team_id
        )
        print(
            f"Linear ref label {label_name!r}: team group {SECURITY_SCAN_LABEL_GROUP!r}"
        )
    return RefLabel(label_id=resolved.id, group_id=resolved.parent_id)


def _create_issue_relations_cve_or_pkg(
    api_key: str,
    pairs: set[tuple[str, str]],
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
    raw = (
        os.environ.get("LINEAR_LABEL_IDS", "").strip()
        or os.environ.get("TRIVY_LINEAR_LABEL_IDS", "").strip()
    )
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
        if not any(
            name == f"{prefix}{key}.json" for key in keys for prefix in wanted_prefixes
        ):
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
    p.add_argument(
        "--output-summary-json",
        type=Path,
        default=None,
        help="Write machine-readable summary JSON for downstream integrations.",
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
            "(e.g. repository default branch from GitHub API in CI)",
            file=sys.stderr,
        )
        return 1
    scan_ref = ScanRef(kind=kind, name=name)
    ref_label = _resolve_ref_label(api_key, team_id, scan_ref.name, scan_ref.kind)

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

    docker_tag = os.environ.get("RESOLVED_DOCKER_TAG", "").strip()
    severity_levels = os.environ.get("DATAHUB_SCAN_SEVERITIES", "").strip()

    created = 0
    updated = 0
    created_issue_records: list[dict[str, Any]] = []
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
        create_labels = _dedupe_preserve_order(
            [*base_label_ids, ref_label.label_id, *repo_lids]
        )

        existing = _find_issue_by_title_util(api_key, team_id, linear_title)
        if existing:
            issue_id = existing
            _sync_refs_comment(api_key, issue_id, scan_ref, short_sha, run_url)
            current = _issue_labels_with_parents_util(api_key, issue_id)
            merged = _dedupe_preserve_order(
                [
                    *_label_ids_replacing_group_sibling_util(
                        current, ref_label.label_id, ref_label.group_id
                    ),
                    *repo_lids,
                ]
            )
            if set(merged) != {ref.id for ref in current}:
                _issue_update_label_ids_util(api_key, issue_id, merged)
            updated += 1
            print(f"Updated refs comment: {linear_title} ({issue_id})")
        else:
            issue_id = _create_issue_util(
                api_key,
                team_id,
                linear_title,
                description,
                create_labels or None,
                linear_priority,
                initial_state_id,
                linear_due_date,
            )
            identifier, url = "", ""
            if args.output_summary_json is not None:
                identifier, url = _get_issue_identifier_url_util(api_key, issue_id)
                ident_display = identifier or issue_id
            else:
                ident_display = issue_id
            _sync_refs_comment(api_key, issue_id, scan_ref, short_sha, run_url)
            created += 1
            print(f"Created issue: {linear_title} ({ident_display})")
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
            if args.output_summary_json is not None:
                created_issue_records.append(
                    {
                        "vulnerability_id": vid,
                        "package_name": pkg_key,
                        "repo_scope": repo_scope,
                        "identifier": identifier,
                        "url": url,
                        "title": linear_title,
                        "issue_id": issue_id,
                    }
                )

    if args.output_summary_json is not None:
        write_security_scan_summary_json(
            args.output_summary_json,
            created_issue_records=created_issue_records,
            created_count=created,
            updated_count=updated,
            run_url=run_url,
            scan_ref_kind=scan_ref.kind,
            scan_ref_name=scan_ref.name,
            commit_sha=commit_sha,
            docker_tag=docker_tag,
            severity_levels=severity_levels,
        )

    if created_in_run:
        _create_issue_relations_cve_or_pkg(
            api_key, _issue_pairs_cve_or_pkg_util(created_in_run)
        )

    print(f"Done. Created {created}, updated refs on {updated} existing.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
