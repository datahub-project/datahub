import re
from typing import Optional
from urllib.parse import parse_qs, urlparse

from datahub_integrations.app import DATAHUB_FRONTEND_URL


def extract_urn_from_url(url: str) -> Optional[str]:
    """
    Extract a DataHub URN from a DataHub frontend URL.

    Args:
        url: The URL to extract URN from

    Returns:
        The extracted URN if found, None otherwise
    """

    try:
        # Parse the URL
        parsed = urlparse(url)

        # Check if it's a DataHub URL - be flexible with domain matching
        frontend_parsed = urlparse(DATAHUB_FRONTEND_URL)

        # First check exact match with configured frontend URL
        if parsed.netloc == frontend_parsed.netloc:
            pass  # Exact match, continue processing
        # Also allow *.acryl.io domains (relaxed matching for customer instances)
        elif re.match(r"^[^.]+\.acryl\.io$", parsed.netloc):
            pass  # Matches customer.acryl.io pattern, continue processing
        else:
            return None

        # Extract URN from path
        # URLs typically look like: /dataset/urn:li:dataset:...
        # or /chart/urn:li:chart:...
        path = parsed.path

        # Look for URN pattern in the path
        urn_pattern = r"urn:li:[^/?]+"
        match = re.search(urn_pattern, path)

        if match:
            return match.group(0)

        # Also check query parameters
        query_params = parse_qs(parsed.query)
        for param_values in query_params.values():
            for value in param_values:
                match = re.search(urn_pattern, value)
                if match:
                    return match.group(0)

        return None

    except Exception:
        return None
