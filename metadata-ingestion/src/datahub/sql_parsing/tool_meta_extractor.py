import contextlib
import json
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple, Union

from typing_extensions import Protocol

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import PlatformDetail
from datahub.ingestion.api.report import Report
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.resource import (
    from_serialized_value,
    generate_user_id_mapping_resource_urn,
)
from datahub.metadata.schema_classes import PlatformResourceInfoClass
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


class ToolMetaExtractorConfig(ConfigModel):
    looker_platform_details: Optional[PlatformDetail] = None


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

    def __init__(self, looker_user_mapping: Optional[Dict[str, str]] = None) -> None:
        self.report = ToolMetaExtractorReport()
        self.known_tool_extractors: List[Tuple[str, Callable[[QueryLog], bool]]] = [
            (
                "mode",
                self._extract_mode_query,
            ),
            (
                "looker",
                self._extract_looker_query,
            ),
        ]
        # maps user id (as string) to email address
        self.looker_user_mapping = looker_user_mapping

    @classmethod
    def create(
        cls,
        config: ToolMetaExtractorConfig,
        graph: Optional[DataHubGraph] = None,
    ) -> "ToolMetaExtractor":
        looker_user_mapping: Dict[str, str] = {}
        if graph and config.looker_platform_details:
            resource_urn = generate_user_id_mapping_resource_urn(
                "looker",
                config.looker_platform_details.platform_instance,
                config.looker_platform_details.env,
            )
            aspect = graph.get_aspect_v2(
                entity_urn=resource_urn,
                aspect_type=PlatformResourceInfoClass,
                aspect="platformResourceInfo",
            )
            if aspect and aspect.value:
                with contextlib.suppress(ValueError, AssertionError):
                    looker_user_mapping = from_serialized_value(aspect.value)
                    assert isinstance(looker_user_mapping, dict)

        return cls(looker_user_mapping=looker_user_mapping)

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

    def _extract_looker_query(self, entry: QueryLog) -> bool:
        """
        Returns:
            bool: whether QueryLog entry is that of looker and looker user info
            is extracted into entry.
        """
        if not self.looker_user_mapping:
            return False

        last_line = _get_last_line(entry.query_text)

        if not (last_line.startswith("--") and "Looker Query Context" in last_line):
            return False

        start_quote_idx = last_line.index("'")
        end_quote_idx = last_line.rindex("'")
        if start_quote_idx == -1 or end_quote_idx == -1:
            return False

        looker_json_raw = last_line[start_quote_idx + 1 : end_quote_idx]
        looker_json = json.loads(looker_json_raw)

        user_id = str(looker_json["user_id"])
        email = self.looker_user_mapping.get(user_id)
        if not email:
            return False

        original_user = entry.user

        entry.user = email_to_user_urn(email)
        entry.extra_info = entry.extra_info or {}
        entry.extra_info["user_via"] = original_user

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
