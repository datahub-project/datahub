from datetime import datetime
from typing import Dict, List, Optional

from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.metadata.schema_classes import OkfStampClass


def _invalid(key: str, path: str, report: GitHubDocumentsSourceReport) -> None:
    report.okf_invalid_values += 1
    report.warning(
        title="Invalid OKF metadata value",
        message=("The value remains in raw YAML but could not be typed."),
        context=f"path={path}, key={key}",
    )


def parse_timestamp(
    value: object, *, key: str, path: str, report: GitHubDocumentsSourceReport
) -> Optional[int]:
    if isinstance(value, str):
        try:
            # Python 3.10 does not accept Z in fromisoformat.
            timestamp = datetime.fromisoformat(
                value[:-1] + "+00:00" if value.endswith("Z") else value
            )
            if timestamp.utcoffset() is not None:
                return int(timestamp.timestamp() * 1000)
        except (ValueError, OverflowError):
            pass
    _invalid(key, path, report)
    return None


def parse_status(
    values: Dict[str, object], *, path: str, report: GitHubDocumentsSourceReport
) -> Optional[str]:
    if "status" not in values:
        return None
    value = values["status"]
    if isinstance(value, str):
        return value
    _invalid("status", path, report)
    return None


def parse_stamp(
    value: object, *, key: str, path: str, report: GitHubDocumentsSourceReport
) -> Optional[OkfStampClass]:
    if not isinstance(value, dict):
        _invalid(key, path, report)
        return None
    actor: object = value.get("by")
    if not isinstance(actor, str) or not actor.strip():
        _invalid(key, path, report)
        return None
    return OkfStampClass(
        by=actor,
        at=parse_timestamp(value["at"], key=f"{key}.at", path=path, report=report)
        if "at" in value
        else None,
    )


def parse_verified(
    values: Dict[str, object], *, path: str, report: GitHubDocumentsSourceReport
) -> Optional[List[OkfStampClass]]:
    if "verified" not in values:
        return None
    value = values["verified"]
    if not isinstance(value, (dict, list)):
        _invalid("verified", path, report)
        return None
    entries = value if isinstance(value, list) else [value]
    stamps: List[OkfStampClass] = []
    for entry in entries:
        stamp = parse_stamp(entry, key="verified", path=path, report=report)
        if stamp is not None:
            stamps.append(stamp)
    return stamps or None
