import json
import logging
from typing import Optional, Set

from databricks.sdk.errors import DatabricksError, PermissionDenied, Unauthenticated

from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport

logger = logging.getLogger(__name__)

LOG_CHUNK_SIZE = 12000


def _log_response(operation: str, identifier: str, response: object) -> None:
    # Preserve unknown fields and missing-vs-empty values instead of applying a
    # model that could hide the very fields this diagnostic is investigating.
    payload = json.dumps(response, ensure_ascii=True)
    chunks = (len(payload) + LOG_CHUNK_SIZE - 1) // LOG_CHUNK_SIZE
    for offset in range(0, len(payload), LOG_CHUNK_SIZE):
        logger.info(
            "Genie diagnostic %s id=%s chunk=%d/%d payload=%s",
            operation,
            identifier,
            offset // LOG_CHUNK_SIZE + 1,
            chunks,
            payload[offset : offset + LOG_CHUNK_SIZE],
        )


def _log_error(
    operation: str, identifier: str, error: Exception, report: UnityCatalogReport
) -> None:
    status = (
        "permission_denied"
        if isinstance(error, (PermissionDenied, Unauthenticated))
        else "request_failed"
    )
    code = (
        error.error_code if isinstance(error, DatabricksError) else type(error).__name__
    )
    # Do not dump exception messages or request headers, which may contain
    # credentials or unrelated response content from proxies.
    logger.warning(
        "Genie diagnostic %s id=%s status=%s error_code=%s",
        operation,
        identifier,
        status,
        code,
    )
    report.warning(
        message="Genie diagnostic request failed",
        context=f"{operation} {identifier}: {status} ({code})",
        log=False,
    )


def log_genie_spaces(proxy: UnityCatalogApiProxy, report: UnityCatalogReport) -> None:
    page_token: Optional[str] = None
    seen_tokens: Set[str] = set()
    seen_spaces: Set[str] = set()
    pages = exported = failures = 0
    listing_complete = False
    try:
        while True:
            page = proxy.list_genie_spaces_raw(page_token=page_token)
            pages += 1
            _log_response("list", str(pages), page)
            if not isinstance(page, dict) or not isinstance(
                page.get("spaces", []), list
            ):
                raise ValueError("Invalid Genie list response")
            for space in page.get("spaces", []):
                if (
                    not isinstance(space, dict)
                    or not isinstance(space.get("space_id"), str)
                    or not space["space_id"]
                ):
                    failures += 1
                    _log_error("detail", "missing-space-id", ValueError(), report)
                    continue
                space_id = space["space_id"]
                if space_id in seen_spaces:
                    continue
                seen_spaces.add(space_id)
                try:
                    detail = proxy.get_genie_space_raw(space_id)
                    _log_response("detail", space_id, detail)
                    exported += 1
                except Exception as error:
                    failures += 1
                    _log_error("detail", space_id, error, report)
            token = page.get("next_page_token")
            if not token:
                listing_complete = True
                break
            if not isinstance(token, str) or token in seen_tokens:
                raise ValueError("Invalid or repeated Genie page token")
            seen_tokens.add(token)
            page_token = token
    except Exception as error:
        failures += 1
        _log_error("list", str(pages + 1), error, report)
    logger.info(
        "Genie diagnostic summary: pages=%d visible_spaces=%d details=%d failures=%d listing_complete=%s. "
        "Counts reflect only spaces visible to the authenticated principal.",
        pages,
        len(seen_spaces),
        exported,
        failures,
        listing_complete,
    )
