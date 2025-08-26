import json
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class BuildInfo:
    def __init__(self) -> None:
        self.build_info: Dict = {}
        self._load_build_info()

    def _load_build_info(self) -> None:
        try:
            with open("/build_info.json", "r") as info:
                self.build_info = json.load(info)
        except Exception as e:
            logger.error(f"Error loading build info: {e}")

    def get(self, key: str) -> str:
        return self.build_info.get(key, None)

    def get_version(self) -> str:
        return self.get("release_version") or self.get("docker_version") or "unset"
