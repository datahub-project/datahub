import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def get_missing_key(inp_dict: Dict, keys: List[str]) -> Optional[str]:
    cur_val = inp_dict
    for key in keys:
        if cur_val is None:
            return None
        if key not in cur_val:
            return key
        cur_val = cur_val[key]
    return None


def get_missing_key_any(inp_dict: Dict, keys: List[str]) -> Optional[str]:
    for key in keys:
        if key not in inp_dict:
            return key
    return None
