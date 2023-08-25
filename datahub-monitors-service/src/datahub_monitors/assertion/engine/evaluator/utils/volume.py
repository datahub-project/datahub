from typing import Dict, Optional

from datahub_monitors.types import Assertion


def get_filter_parameters(assertion: Assertion) -> Optional[Dict]:
    """
    Extracts filter information from Volume Assertion and returns it as a dictionary
    """
    volume_assertion = assertion.volume_assertion
    if volume_assertion is not None and volume_assertion.filter is not None:
        return volume_assertion.filter.__dict__
    return None
