import logging
import re
from typing import Dict, List

logger = logging.getLogger(__name__)


class DatasetLineageState:
    """Maintains dataset lineage state across different managers"""

    def __init__(self):
        self.dataset_urns: Dict[str, Dict[str, any]] = {}
        self.debug_info: Dict[str, List[str]] = {
            "registered_datasets": [],
            "matched_pairs": [],
        }

    def register_dataset(
        self,
        table_name: str,
        urn: str,
        columns: List[str],
        platform: str,
        additional_info: Dict = None,
    ):
        """Register a dataset and its columns"""
        normalized_name = self.normalize_name(table_name)
        self.dataset_urns[normalized_name] = {
            "urn": urn,
            "columns": columns,
            "platform": platform,
            "additional_info": additional_info or {},
        }

    def get_upstream_datasets(
        self, table_name: str = None, platform: str = None
    ) -> List[Dict[str, any]]:
        """Get potential upstream datasets

        Args:
            table_name: Optional table name to match
            platform: Optional platform to filter by
        """
        results = []

        for name, info in self.dataset_urns.items():
            # If platform is specified, filter by platform
            if platform and info["platform"] != platform:
                continue

            # If table name is specified, match the name
            if table_name and name != self.normalize_name(table_name):
                continue

            results.append(
                {
                    **info,
                    "table_name": name,  # Include the normalized table name
                }
            )

        return results

    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize names for comparison"""
        # Remove any schema/database prefixes
        if "." in name:
            name = name.split(".")[-1]
        return name.lower().strip()

    @staticmethod
    def normalize_delta_column(column_name: str) -> str:
        """Normalize Delta column names by removing metadata in square brackets"""
        parts = column_name.split(".")
        if not parts:
            return column_name

        clean_name = parts[-1]
        clean_name = re.sub(r"\[.*?\]", "", clean_name)
        return clean_name.strip()
