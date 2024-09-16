import json
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple, Union

from typing_extensions import Protocol

from datahub.ingestion.api.report import Report
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn
from datahub.utilities.stats_collections import int_top_k_dict

UrnStr = str

logger = logging.getLogger(__name__)


class QueryLog(Protocol):
    """Represents Query Log Entry
    expected by QueryMetaExractor interface
    """

    query_text: str
    user: Optional[Union[CorpUserUrn, CorpGroupUrn]]
    extra_info: Optional[dict]


def _get_last_line(query: str) -> str:
    return query.rstrip().rsplit("\n", maxsplit=1)[-1]


@dataclass
class ToolMetaExtractorReport(Report):
    num_queries_meta_extracted: Dict[str, int] = field(default_factory=int_top_k_dict)


class ToolMetaExtractor:
    """Enriches input query log entry with tool-specific details captured as part of query log

    Such as
    - Queries executed on warehouse by Mode BI tool contain information of actual user interacting
    with BI tool which is more useful as compared to service account used to execute query as reported
    by warehouse query logs.
    """

    def __init__(self) -> None:
        self.report = ToolMetaExtractorReport()
        self.known_tool_extractors: List[Tuple[str, Callable[[QueryLog], bool]]] = [
            (
                "mode",
                self._extract_mode_query,
            )
        ]

    def _extract_mode_query(self, entry: QueryLog) -> bool:
        """
        Returns:
            bool: whether QueryLog entry is that of mode and mode user info
            is extracted into entry.
        """
        last_line = _get_last_line(entry.query_text)

        if not (
            last_line.startswith("--")
            and '"url":"https://modeanalytics.com' in last_line
        ):
            return False

        mode_json_raw = last_line[2:]
        mode_json = json.loads(mode_json_raw)

        original_user = entry.user

        entry.user = email_to_user_urn(mode_json["email"])
        entry.extra_info = entry.extra_info or {}
        entry.extra_info["user_via"] = original_user

        # TODO: Generate an "origin" urn.

        return True

    def extract_bi_metadata(self, entry: QueryLog) -> bool:

        for tool, meta_extractor in self.known_tool_extractors:
            try:
                if meta_extractor(entry):
                    self.report.num_queries_meta_extracted[tool] += 1
                    return True
            except Exception:
                logger.debug("Tool metadata extraction failed with error : {e}")
        return False


# NOTE: This is implementing the most common user urn generation scenario
# however may need to be revisited at later point
def email_to_user_urn(email: str) -> CorpUserUrn:
    return CorpUserUrn(email.split("@", 1)[0])
