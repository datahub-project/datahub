import urllib.parse
from typing import Optional


def extract_urn_from_url(url: str) -> Optional[str]:
    """Extract the urn from a DataHub URL.

    Args:
        url: The URL to extract the urn from.

    Returns:
        The urn from the URL, or None if there's no urn in the URL.

    Examples:
        >>> extract_urn_from_url("https://tenant.acryl.io/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.dataset_1,PROD)")
        'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.dataset_1,PROD)'
        >>> extract_urn_from_url("https://tenant.acryl.io/") is None
        True
        >>> extract_urn_from_url("https://acryl.acryl.io/settings/integrations/slack") is None
        True
        >>> extract_urn_from_url("https://tenant.acryl.io/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.dataset_1,PROD)/Lineage?is_lineage_mode=false")
        'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.dataset_1,PROD)'
        >>> extract_urn_from_url("https://demo.datahub.com/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Clong_tail_companions.adoption.actuating%2CPROD%29/")
        'urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.actuating,PROD)'
        >>> extract_urn_from_url("https://demo.datahub.com/container/urn%3Ali%3Acontainer%3Ab41c14bc5cb3ccfbb0433c8cbdef2992/Contents")
        'urn:li:container:b41c14bc5cb3ccfbb0433c8cbdef2992'
    """

    # TODO: Do we need to handle cases where we don't have the http/https prefix?

    link_parts = url.split("/")
    if len(link_parts) < 5:
        return None

    # Extract the potential URN part
    potential_urn = link_parts[4]

    # Handle URL-encoded URNs
    if potential_urn.startswith("urn%3A"):
        try:
            decoded_urn = urllib.parse.unquote(potential_urn)
        except UnicodeDecodeError:
            # Safety condition to throwing on invalid URL-encoded URNs.
            return None
    else:
        decoded_urn = potential_urn

    # Check if it starts with "urn:"
    if not decoded_urn.startswith("urn:"):
        return None

    return decoded_urn
