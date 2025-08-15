import contextlib
import json
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple, Union

from typing_extensions import Protocol

from datahub.api.entities.platformresource.platform_resource import (
    ElasticPlatformResourceQuery,
    PlatformResource,
    PlatformResourceSearchFields,
)
from datahub.ingestion.api.report import Report
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn, DataPlatformUrn, Urn
from datahub.utilities.search_utils import LogicalOperator
from datahub.utilities.stats_collections import int_top_k_dict

UrnStr = str

logger = logging.getLogger(__name__)

MODE_PLATFORM_URN = DataPlatformUrn.from_string("urn:li:dataPlatform:mode")
LOOKER_PLATFORM_URN = DataPlatformUrn.from_string("urn:li:dataPlatform:looker")
HEX_PLATFORM_URN = DataPlatformUrn.from_string("urn:li:dataPlatform:hex")


class QueryLog(Protocol):
    """Represents Query Log Entry
    expected by QueryMetaExractor interface
    """

    query_text: str
    user: Optional[Union[CorpUserUrn, CorpGroupUrn]]
    extra_info: Optional[dict]
    origin: Optional[Urn]


def _get_last_line(query: str) -> str:
    return query.rstrip().rsplit("\n", maxsplit=1)[-1]


@dataclass
class ToolMetaExtractorReport(Report):
    num_queries_meta_extracted: Dict[str, int] = field(default_factory=int_top_k_dict)
    failures: List[str] = field(default_factory=list)
    looker_user_mapping_missing: Optional[bool] = None


class ToolMetaExtractor:
    """Enriches input query log entry with tool-specific details captured as part of query log

    Such as
    - Queries executed on warehouse by Mode BI tool contain information of actual user interacting
    with BI tool which is more useful as compared to service account used to execute query as reported
    by warehouse query logs.
    """

    def __init__(
        self,
        report: ToolMetaExtractorReport,
        looker_user_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        self.report = report
        self.known_tool_extractors: List[Tuple[str, Callable[[QueryLog], bool]]] = [
            (
                "mode",
                self._extract_mode_query,
            ),
            (
                "looker",
                self._extract_looker_query,
            ),
            (
                "hex",
                self._extract_hex_query,
            ),
        ]
        # maps user id (as string) to email address
        self.looker_user_mapping = looker_user_mapping

    @classmethod
    def create(
        cls,
        graph: Optional[DataHubGraph] = None,
    ) -> "ToolMetaExtractor":
        report = ToolMetaExtractorReport()
        looker_user_mapping = None
        if graph:
            try:
                looker_user_mapping = cls.extract_looker_user_mapping_from_graph(
                    graph, report
                )
            except Exception as e:
                report.failures.append(
                    f"Unexpected error during Looker user metadata extraction: {str(e)}"
                )

        return cls(report, looker_user_mapping)

    @classmethod
    def extract_looker_user_mapping_from_graph(
        cls, graph: DataHubGraph, report: ToolMetaExtractorReport
    ) -> Optional[Dict[str, str]]:
        looker_user_mapping = None
        query = (
            ElasticPlatformResourceQuery.create_from()
            .group(LogicalOperator.AND)
            .add_field_match(PlatformResourceSearchFields.PLATFORM, "looker")
            .add_field_match(
                PlatformResourceSearchFields.RESOURCE_TYPE,
                "USER_ID_MAPPING",
            )
            .end()
        )
        platform_resources = list(
            PlatformResource.search_by_filters(query=query, graph_client=graph)
        )

        if len(platform_resources) == 0:
            report.looker_user_mapping_missing = True
        elif len(platform_resources) > 1:
            report.failures.append(
                "Looker user metadata extraction failed. Found more than one looker user id mappings."
            )
        else:
            platform_resource = platform_resources[0]

            if (
                platform_resource
                and platform_resource.resource_info
                and platform_resource.resource_info.value
            ):
                with contextlib.suppress(ValueError, AssertionError):
                    value = platform_resource.resource_info.value.as_raw_json()
                    if value:
                        looker_user_mapping = value

        return looker_user_mapping

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

        entry.origin = MODE_PLATFORM_URN

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

        entry.origin = LOOKER_PLATFORM_URN

        return True

    def _extract_hex_query(self, entry: QueryLog) -> bool:
        """
        Returns:
            bool: whether QueryLog entry is that of hex.
        """
        last_line = _get_last_line(entry.query_text)

        if not last_line.startswith("-- Hex query metadata:"):
            return False

        entry.origin = HEX_PLATFORM_URN

        return True

    def extract_bi_metadata(self, entry: QueryLog) -> bool:
        for tool, meta_extractor in self.known_tool_extractors:
            try:
                if meta_extractor(entry):
                    self.report.num_queries_meta_extracted[tool] += 1
                    return True
            except Exception as e:
                logger.debug(f"Tool metadata extraction failed with error : {e}")
        return False


# NOTE: This is implementing the most common user urn generation scenario
# however may need to be revisited at later point
def email_to_user_urn(email: str) -> CorpUserUrn:
    return CorpUserUrn(email.split("@", 1)[0])
